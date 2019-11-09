#!/usr/bin/env ruby
Dir["../datascience/data-mining/*.rb"].each {|file| require file }
Rails.logger.level = :warn

def process_parse_scan(schema:, tablename:, eventidcol:, processes: 1, message_thresh: 50, query:, store_doc_dir: '', try_doc_parse: false, request_parsed_reply: false, form_type:[])
	
	###Set Basic Parameters
	uniqnum = (rand * 1000000).to_int.to_s	
	max_retries = 10
	conn = PG.connect(:dbname => 'development', :host => 'localhost', :user => 'postgres')    

	#Get form ids for targeted mining
	if (try_doc_parse)
		i = 0
		params = []
		form_type.each { params.push(form_type[i]); i+=1; }

		select_form_id = 'SELECT datasci_mining.grouped_form_id(' + "'{" + params.join(',') + "}'::varchar[])" 
		conn.prepare('select_form_id_' + uniqnum, select_form_id)
		form_id_query = conn.exec_prepared('select_form_id_' + uniqnum).values.flatten.first
		$stderr.puts "Form id query string: #{form_id_query}".magenta

	else 
		form_id_query = ''

	end

	select_query = 'SELECT %{eventidcol} FROM %{schema}.%{tablename}' % {:eventidcol => eventidcol, :schema => schema, :tablename => tablename }
	conn.prepare('select_statement_' + uniqnum, select_query)
	eventids = conn.exec_prepared('select_statement_' + uniqnum).values.flatten

	if eventids.count > 65535
		eventids = eventids.first(65535)
		puts("More than maximum allowable PG fetches. Selecting first 65,535.")
	end 

	###Pull Data
	$stderr.puts "[#{Time.now}] Began pulling data".green

	begin
		conn.exec("DEALLOCATE select_statement_" + uniqnum); nil
		rescue 
	end

	##Loop over staged events to construct select statement
	i = 1
	params = []
	eventids.each { params.push(i); i+=1; }

	conn.prepare('select_statement_' + uniqnum, 'SELECT * FROM datasci.property_transaction_events WHERE event_id IN ($'+params.join(',$')+')')
	events = conn.exec_prepared('select_statement_' + uniqnum, eventids)
	keys = events.fields; nil
	events = events.values; nil

	total_count = events.count; nil

	begin
	    Parallel.each_with_index(events.each, in_processes: processes) do |event, index|
			$stderr.puts "[#{Time.now}][#{index+1}/#{total_count}] Looking at #{event[keys.index("type")]} ##{event[keys.index("id")]} for #{event[keys.index("address")]}".green            

			#Collect basic information
			$stderr.puts  "[#{Time.now}] Collecting basic Tx information"
			external_account_id = event[keys.index("external_account_id")].to_i; nil
			property_transaction_id = event[keys.index("property_transaction_id")].to_i; nil
			event_type = event[keys.index("type")]; nil
			message_id = event[keys.index("originating_message_id")]; nil
			street_address = event[keys.index("street_addr")]

			ac_check = active_record_conn_check
			
			if !(ac_check)
				raise Parallel::Break
				exit   
			else
				begin
					gmail_token = ExternalAccount.ok.find_by_id(external_account_id)
					transaction = PropertyTransaction.find(property_transaction_id)
					representation = representation_for_transaction(transaction)
				rescue ActiveRecord::RecordNotFound
					representation = :unknown
				end
			end

		    #If no gmail token, set missing and write record
		    if gmail_token.nil?

		        missing_reason = 'No Gmail Token'

		        #Insert Record
                data = [event[keys.index("id")].to_i, property_transaction_id, event_type, representation, nil, missing_reason, nil, nil, nil, nil, nil, nil, nil, nil, nil] 
                insert_update(conn: conn, operation: 'insert', schema: schema, table: 'offer_messages', data: data, update_where: "") 

		    	$stderr.puts missing_reason.red
	      
		    #If token present proceed with query and collect emails
		    else   

		        retries = 1
		        begin       
					$stderr.puts 'Requesting message count.'
					api = gmail_token.mail_api 
					uniqnum = (rand * 1000000).to_int.to_s	

					select_form_id = 'SELECT datasci_mining.grouped_form_id()' 
					conn.prepare('select_form_id_' + uniqnum, select_form_id)
					form_id_query = conn.exec_prepared('select_form_id_' + uniqnum).values.flatten.first
					puts(form_id_query)

					fullquery = query + "AND (" + form_id_query + ") AND \"#{street_address}\""

					puts(fullquery)
					
					result = api.messages(fullquery);nil					
					messages_count = result.count;nil					
				
				rescue Faraday::TimeoutError, Google::Apis::RateLimitError, Google::Apis::ServerError, OpenSSL::SSL::SSLError => request_error
			        if retries <= max_retries
						$stderr.puts "[#{Time.now}] Encountered error: #{request_error}. Will retry #{max_retries} times. Processing attempt #{retries}".white          
			        	retries += 1
			        	sleep 2 ** retries
			        	retry
			        else
			        	raise "Timeout: #{request_error}"
			        	Parallel::Break
			        	return
			        end
				end

				#If no results, set missing and write record
				$stderr.puts("[#{Time.now}] ".green + "#{messages_count} Messages found ".magenta.bold + "for query on #{event[keys.index("type")]} ##{event[keys.index("id")]} for #{event[keys.index("address")]}".green)            
				
				if messages_count == 0

		        	missing_reason = 'Query returned no reults'

		        	#Insert Record
	                data = [event[keys.index("id")].to_i, property_transaction_id, event_type, representation, nil, missing_reason, nil, nil, nil, nil, nil, nil, nil, nil, nil]
	                insert_update(conn: conn, operation: 'insert', schema: schema, table: 'offer_messages', data: data, update_where: "") 

	 		    	$stderr.puts missing_reason.red

				#If message count greater than threshold, punt on data collection
				elsif messages_count > message_thresh

					$stderr.puts "[#{Time.now}] #{messages_count} Messages found for query on #{event[keys.index("type")]} #{event[keys.index("id")]} for #{event[keys.index("address")]} greater than threshold #{message_thresh}".green            

		        	missing_reason = 'Query returned more messages than allowed by threshold'
		        	
		        	#Insert Record
					data = [event[keys.index("id")].to_i, property_transaction_id, query, message_thresh, messages_count]
	                insert_update(conn: conn, operation: 'insert', schema: schema, table: 'offer_messages_over_threshold', data: data, update_where: "") 
			        
			        $stderr.puts missing_reason.red
		        	
				#Else, fetch info for each message in query
				else

					docsMD5 = []

					result.each_with_index do |message,m|
						
						$stderr.puts "[#{Time.now}] Processing [#{m+1}/#{messages_count}] Messages found for query on #{event[keys.index("type")]} #{event[keys.index("id")]} for #{event[keys.index("address")]}".green            

						#Result Metadata needed for document inserts
						metadata = {"event_id" => event[keys.index("id")].to_i,
									"state" => event[keys.index("state")],
									"message_id" => message.header[GmailAPI::X_GMAIL_MESSAGE_ID].value,
									"property_transaction_id" => property_transaction_id
									}

		        		#Insert Record
						data = [metadata["event_id"], property_transaction_id, event_type, representation, metadata["message_id"], nil, message.from&.first, (!message.to.nil? && message.to.kind_of?(Array) ? (message.to.join(',')) : (message.to)), (!message.sender.nil? ? message.sender : (nil)), message.subject, message.date, message.cc&.join(','), message.mime_version, message.text, (message.html.delete("\u0000") if !message.html.nil?)]
		                insert_update(conn: conn, operation: 'insert', schema: schema, table: 'offer_messages', data: data, update_where: "") 
				        
				        if message.attachments.present?
			                	
							pdf_attachments = []

				       		message.attachments.select do |attachment|

							    if (attachment.mime_type == 'application/pdf' || attachment.filename.downcase.ends_with?('.pdf')) &&
									(docsMD5.grep(Digest::MD5.hexdigest(attachment.decoded)).count == 0) 
							      	pdf_attachments << attachment
							      	docsMD5 << Digest::MD5.hexdigest(attachment.decoded)
							    end

							end

						    if pdf_attachments.count == 0
						    	#No PDFs found
						    	$stderr.puts "[#{Time.now}] No pdf format attachment data found".blue
						    
						    else

						    	begin
		                		add_transaction_documents(schema: schema, metadata: metadata, documents: pdf_attachments, conn: conn, store_doc_dir: store_doc_dir, try_doc_parse: try_doc_parse, request_parsed_reply: request_parsed_reply, form_type: form_type)

		        				#Reconnect
								rescue PG::UnableToSend, PG::ConnectionBad => pg_exception
									begin
			            				conn.reset
			          				rescue
			            				sleep 10
			            				retry
			            			end			        		
		        				end
							end
				    	end		
					end
				$stderr.puts "[#{Time.now}] Finished processing query results for #{event[keys.index("type")]} ##{event[keys.index("id")]} for #{event[keys.index("address")]}".green
				end				
			end
		
		Signal.trap("USR1") {
		
			$stderr.puts "[#{Time.now}] Parallel received signal, attempting to shut down."		
			raise Parallel::Break
			exit
		}		
		
		end
		
		$stderr.puts "[#{Time.now}] Finish parallel processing".green

	rescue Interrupt # thrown by Parallel when SIGINT sent

	end

	#Finished Pulling Data
	$stderr.puts "[#{Time.now}] Finished pulling data".green  

end 

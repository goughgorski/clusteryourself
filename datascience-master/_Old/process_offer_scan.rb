#!/usr/bin/env ruby

Rails.logger.level = :warn

def process_offer_scan(schema, tablename, eventidcol, processes = 1, message_thresh = 50, query, store_pdf)
	prep_prefix = 'pos_'
	max_retries = 10
	conn = PG.connect(:dbname => 'development', :host => 'localhost', :user => 'postgres')    
	select_query = 'SELECT %{eventidcol} FROM %{schema}.%{tablename}' % {:eventidcol => eventidcol, :schema => schema, :tablename => tablename }
	conn.prepare(prep_prefix.to_s + 'select_statement', select_query)
	eventids = conn.exec_prepared(prep_prefix.to_s + 'select_statement').values.flatten

	$stderr.puts "[#{Time.now}] Began pulling data".green

	begin
		conn.exec("DEALLOCATE " + prep_prefix.to_s + "select_statement"); nil
		rescue 
	end

	i = 1
	params = []
	eventids.each { params.push(i); i+=1; }

	conn.prepare(prep_prefix.to_s+'select_statement', 'SELECT * FROM datasci.property_transactions_ds WHERE id IN ($'+params.join(',$')+')')
	events = conn.exec_prepared(prep_prefix.to_s + 'select_statement', eventids)
	keys = events.fields; nil
	events = events.values; nil

	total_count = events.count

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

			#Check connection and get addtional information
			$stderr.puts 'Checking Active Record connection.'
			begin
				db = ActiveRecord::Base.connection.current_database; nil
				if db.nil?
					begin
						$stderr.puts 'No connection found. Attempting to reestabish.'
						ActiveRecord::Base.connection.reconnect!
					rescue
						sleep 10
						retry
					end
				end

				db = ActiveRecord::Base.connection.current_database; nil
				if db.nil?
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
			end

		    #If no gmail token, set missing and write record
		    if gmail_token.nil?

		        missing_reason = 'No Gmail Token'

		        #Insert Record
				begin
				    conn.prepare(prep_prefix.to_s + 'insert_statement_' + index.to_s, 'INSERT INTO datasci.offer_messages (
			            event_id 
			            , property_transaction_id 
			            , event_type 
			            , representation 
			            , missing_reason   
			            , message_id 
			            , "from" 
			            , "to" 
			            , sender 
			            , subject 
			            , date 
			            , cc 
			            , mime_version 
			            , body_text 
			            , body_html 
			            , created_at 
			            ) values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)')
			          
					conn.exec_prepared(prep_prefix.to_s + 'insert_statement_' + index.to_s, [
						event[keys.index("id")].to_i,
						property_transaction_id, 
						event_type, 
						representation, 
						missing_reason, 
						nil, 
						nil, 
						nil, 
						nil, 
						nil, 
						nil, 
						nil, 
						nil, 
						nil, 
						nil, 
						Time.now.getutc])

			    	$stderr.puts missing_reason.red
			        
		        #Reconnect
				rescue PG::UnableToSend, PG::ConnectionBad => pg_exception
					begin
			           	conn.reset
			      	rescue
			            sleep 10
			            retry
			        end
			        retry			            				
				
				rescue PG::UniqueViolation => pg_violation
              		$stderr.puts "Encountered exception #{pg_violation.message}".blue					

				rescue PG::InvalidSqlStatementName => p_inv_statement_error
				#Retry insert
					retry

				rescue PG::DuplicatePstatement => p_dup_statement_error
				#Deallocate insert
					conn.exec("DEALLOCATE " + prep_prefix.to_s + "insert_statement_" + index.to_s); nil
					retry

				end
	      
		    #If token present proceed with query and collect emails
		    else   

		        retries = 1
		        begin       
					$stderr.puts 'Requesting message count.'
					api = gmail_token.mail_api
					fullquery = query + " AND \"#{street_address}\""
					messages_count = api.messages(fullquery).count;nil					
				
				rescue Faraday::TimeoutError, Google::Apis::RateLimitError, Google::Apis::ServerError, OpenSSL::SSL::SSLError => request_error
			        if retries <= max_retries
						$stderr.puts("[#{Time.now}] Encountered error: #{request_error}. Will retry #{max_retries} times. Processing attempt #{retries}".white)            
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
					begin

					    conn.prepare(prep_prefix.to_s + 'insert_statement_' + index.to_s, 'INSERT INTO datasci.offer_messages (
			                event_id 
			                , property_transaction_id 
			                , event_type 
			                , representation 
			                , missing_reason   
			                , message_id 
			                , "from" 
			                , "to" 
			                , sender 
			                , subject 
			                , date 
			                , cc 
			                , mime_version 
			                , body_text 
			                , body_html 
			                , created_at 
			                ) values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)')
			              
						conn.exec_prepared(prep_prefix.to_s + 'insert_statement_' + index.to_s, [
							event[keys.index("id")].to_i,
							property_transaction_id, 
							event_type, 
							representation, 
							missing_reason, 
							nil, 
							nil, 
							nil, 
							nil, 
							nil, 
							nil, 
							nil, 
							nil, 
							nil, 
							nil, 
							Time.now.getutc])
			        
			        	$stderr.puts missing_reason.red
		        	
		        	#Reconnect
					rescue PG::UnableToSend, PG::ConnectionBad => pg_exception
						begin
			            	conn.reset
			          	rescue
			            	sleep 10
			            	retry
			            end
			            retry			            
			         
			        rescue PG::UniqueViolation => pg_violation
              			$stderr.puts "Encountered exception #{pg_violation.message}".blue
          			
					rescue PG::InvalidSqlStatementName => p_inv_statement_error
					#Retry insert
						retry

          			rescue PG::DuplicatePstatement => p_dup_statement_error
						#Deallocate insert
						conn.exec("DEALLOCATE " + prep_prefix.to_s + "insert_statement_" + index.to_s); nil
						retry

          			end					

				#If message count greater than threshold, punt on data collection
				elsif messages_count > message_thresh
					$stderr.puts "[#{Time.now}] #{messages_count} Messages found for query on #{event[keys.index("type")]} #{event[keys.index("id")]} for #{event[keys.index("address")]} greater than threshold #{message_thresh}".green            

		        	missing_reason = 'Query returned more messages than allowed by threshold'

		        	#Insert Record
					begin

					    conn.prepare(prep_prefix.to_s + 'insert_statement_' + index.to_s, 'INSERT INTO datasci_projects.offers_over_message_threshold (
			                event_id 
			                , property_transaction_id 
			                , query
			                , message_thresh
			                , messages_count
			                , created_at 
			                ) values ($1, $2, $3, $4, $5, $6)')
			              
						conn.exec_prepared(prep_prefix.to_s + 'insert_statement_' + index.to_s, [
							event[keys.index("id")].to_i,
							property_transaction_id, 
							query, 
							message_thresh, 
							messages_count, 
							Time.now.getutc])
			        
			        	$stderr.puts missing_reason.red
		        	
		        	#Reconnect
					rescue PG::UnableToSend, PG::ConnectionBad => pg_exception
						begin
			            	conn.reset
			          	rescue
			            	sleep 10
			            	retry
			            end
			            retry			            
			         
			        rescue PG::UniqueViolation => pg_violation
              			$stderr.puts "Encountered exception #{pg_violation.message}".blue
          			
          			rescue PG::InvalidSqlStatementName => p_inv_statement_error
					#Retry insert
						retry

          			rescue PG::DuplicatePstatement => p_dup_statement_error
						#Deallocate insert
						conn.exec("DEALLOCATE " + prep_prefix.to_s + "insert_statement_" + index.to_s); nil
						retry

          			end					

				#Else, fetch info for each message in query
				else
					
					begin       
						result = api.messages(fullquery);nil					
						messages = []
						result.each do |message|
				    		messages << message
							end
					rescue Google::Apis::RateLimitError
			        	sleep 10
			        	retry
					end

					messages.each_with_index do |message,m|
						#Add message here RE index progress
						$stderr.puts "[#{Time.now}] Processing [#{m+1}/#{messages_count}] Messages found for query on #{event[keys.index("type")]} #{event[keys.index("id")]} for #{event[keys.index("address")]}".green            

						this_message_id = message.header[GmailAPI::X_GMAIL_MESSAGE_ID].value

						puts(this_message_id)
						
						begin 
						
							conn.prepare(prep_prefix.to_s + 'insert_message_statement_' + index.to_s, 'INSERT INTO datasci.offer_messages (
								event_id 
								, property_transaction_id 
								, event_type 
								, representation 
								, missing_reason   
								, message_id 
								, "from" 
								, "to" 
								, sender 
								, subject 
								, date 
								, cc 
								, mime_version 
								, body_text 
								, body_html 
								, created_at 
								) values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)')

							conn.exec_prepared(prep_prefix.to_s + 'insert_message_statement_' + index.to_s, [
								event[keys.index("id")].to_i,
								property_transaction_id, 
								event_type, 
								representation, 
								missing_reason, 
								this_message_id, 
								message.from&.first,
								(!message.to.nil? && message.to.kind_of?(Array) ? (message.to.join(',')) : (message.to)),
								(!message.sender.nil? ? message.sender : (nil)),
								message.subject,
								message.date,
								message.cc&.join(','),
								message.mime_version,
								message.text,
								(message.html.delete("\u0000") if !message.html.nil?),
								Time.now.getutc])

							#Deallocate insert
							conn.exec("DEALLOCATE " + prep_prefix.to_s + "insert_message_statement_" + index.to_s); nil
		        		
		        		#Reconnect
						rescue PG::UnableToSend, PG::ConnectionBad => pg_exception
							begin
			            		conn.reset
			          		rescue
			            		sleep 10
			            		retry
			            	end
			            	retry

				        rescue PG::UniqueViolation => pg_violation
    	          			$stderr.puts "Encountered exception #{pg_violation.message}".blue

						rescue PG::InvalidSqlStatementName => p_inv_statement_error
						#Retry insert
							retry

						rescue PG::DuplicatePstatement => p_dup_statement_error
							#Deallocate insert
							conn.exec("DEALLOCATE " + prep_prefix.to_s + "insert_message_statement_" + index.to_s); nil
							retry

						end

				        #Write attachment records
				        begin
		                 add_transaction_documents(property_transaction_id, this_message_id, message.attachments, conn, index, prep_prefix, store_pdf)
		        		
		        		#Reconnect
						rescue PG::UnableToSend, PG::ConnectionBad => pg_exception
							begin
			            		conn.reset
			          		rescue
			            		sleep 10
			            		retry
			            	end
			            	retry			            	
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

def representation_for_transaction(transaction) 
  if transaction.nil?
    :unknown
  elsif transaction.agent_represents_buyer? && transaction.agent_represents_seller?
    :both
  elsif transaction.agent_represents_buyer?
    :buyer
  elsif transaction.agent_represents_seller?
    :seller
  else
    :unknown
  end
end

def add_transaction_documents(property_transaction_id, message_id, attachments, conn, index, prep_prefix = '', store_pdf)
  if attachments.present?
    
    pdf_attachments = attachments.select do |attachment|
      attachment.mime_type == 'application/pdf' || attachment.filename.downcase.ends_with?('.pdf')
    end
    
    if pdf_attachments.count == 0
    	
    	$stderr.puts "[#{Time.now}] No pdf format attachment data found".blue
    
    else
    	$stderr.puts "[#{Time.now}] Storing pdf attachment data".green

	    pdf_attachments.each_with_index do |pdf_attachment|
	      pdf = pdf_attachment.decoded
	      $stderr.puts "  PDF attachment: #{pdf_attachment.filename}"
	      
	      if text = extract_pdf_text(pdf) and text.size > 300
	        $stderr.puts "    Text size without OCR: #{text.size}"
	      elsif text = extract_ocr_text(pdf)
	        $stderr.puts "    Text size from OCR: #{text.size}"
	      end

	      if (text.size > 0)

	        begin

				if (store)
				
					f = File.new("/tmp/#{pdf_attachment.filename}", "w")
					f.write(pdf)
					f.close
		        
				end

		        conn.prepare(prep_prefix.to_s + 'insert_document_statement_' + index.to_s, 'INSERT INTO datasci.offer_documents (
		          property_transaction_id  
		          , message_id 
		          , attachment_name 
		          , document_content
		          , created_at
		          ) values ($1, $2, $3, $4, $5)')
	        
		        conn.exec_prepared(prep_prefix.to_s + 'insert_document_statement_' + index.to_s, [
		        	property_transaction_id,
		            message_id,
		            pdf_attachment.filename,
		            text, 
		            Time.now.getutc])

	        rescue PG::UniqueViolation, PG::CharacterNotInRepertoire => pg_violation
	            $stderr.puts "Encountered exception #{pg_violation.message}".blue

			rescue PG::InvalidSqlStatementName => p_inv_statement_error
				#Retry insert
				retry
				
			rescue PG::DuplicatePstatement => p_dup_statement_error
				#Deallocate insert
				conn.exec("DEALLOCATE " + prep_prefix.to_s + "insert_document_statement_" + index.to_s); nil
				retry

	        end
	        
	      end
	    end
	end
  end
end

def extract_ocr_text pdf
  engine = Tesseract::Engine.new{|e| e.language= :eng}
  tempdir = Dir.mktmpdir
  begin
    IO.popen("convert -density 300 -[0] -depth 8 #{tempdir}/page-%04d.png", "w", encoding: 'BINARY', err: :close) { |p| p.write pdf }
    Dir.glob("#{tempdir}/page-*.png").map { |page| engine.text_for page }.join("\n")
  ensure
    FileUtils.rm_r tempdir
  end
end

def extract_pdf_text(pdf)
  Henkei.read :text, pdf
rescue Exception => error
  Rails.logger.fatal("Failed to extract text from PDF\n#{error.class} (#{error.message})")
  nil
end
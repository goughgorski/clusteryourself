conn = PG.connect(:dbname => 'dupr2vdkvoban', :host => 'ec2-34-237-233-59.compute-1.amazonaws.com',
 :user => 'uenrajq17b9i6e', :password => 'p9a513552fea4dfd276a6dcbddbeb5e3de4766bf96cdd42790464b0a427f8e59f')

select_query = "SELECT id FROM public.external_accounts
		WHERE created_at > '2018-10-18'
		AND created_at < '2018-11-01'
		AND type = 'GoogleAccount'"

fullquery = "after:2018/09/07 before:2018/11/08"; nil

prep_prefix = 'pos_'
conn.prepare(prep_prefix.to_s+'select_statement', select_query)
events = conn.exec_prepared(prep_prefix.to_s + 'select_statement')
keys = events.fields; nil
events = events.values; nil

#events = events[0, 100]; nil
#events = events[100, 100]; nil
#events = events[200, 100]; nil
#events = events[300, 100]; nil
#events = events[400, 100]; nil
#events = events[500, 100]; nil
#events = events[600, 100]; nil
#events = events[700, 100]; nil
#events = events[800, 100]; nil
#events = events[900, 100]; nil
#events = events[1000, 100]; nil
#events = events[1100, 100]; nil
#events = events[1200, 100]; nil
#events = events[1300, 100]; nil
#events = events[1400, 100]; nil
#events = events[1500, 100]; nil
#events = events[1600, 100]; nil
#events = events[1700, 100]; nil
#events = events[1800, 100]; nil
events = events[1900, 100]; nil
#events = events[2000, 100]; nil
#events = events[2100, 100]; nil
#events = events[2200, 100]; nil


total_count = events.count

begin
	Parallel.each_with_index(events.each, in_processes: 6) do |event, index|

		#event = events[325] #added for single instance of loop
		#index = 0; nil

		#Collect basic information
		$stderr.puts  "[#{Time.now}] Collecting basic Tx information"
		#event_id = event[keys.index("event_id")].to_i; nil
		#event_type = event[keys.index("type")].to_s; nil
		#event_created_at = event[keys.index("created_at")].to_date; nil
		#property_transaction_id = event[keys.index("property_transaction_id")].to_i; nil
		external_account_id = event[keys.index("id")].to_i; nil
		#originating_message_id = event[keys.index("originating_message_id")].to_s; nil

		db = ActiveRecord::Base.connection.current_database; nil

		begin
			gmail_token = ExternalAccount.ok.find_by_id(external_account_id)
		end

			if gmail_token.nil?

			    missing_reason = 'No Gmail Token'

			    #Insert Record
				begin
					conn.prepare(prep_prefix.to_s + 'insert_message_statement_' + index.to_s, 'INSERT INTO datasci_projects.broad_inbox_messages (
						external_account_id
						, missing_reason
						, "from"
						, "to"
						, subject
						, "date"
						, cc
						, body_text
						, message_id
						, labels
						, n_attachments
						, attachment_names
						, created_at
						) values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)')
			          
					conn.exec_prepared(prep_prefix.to_s + 'insert_message_statement_' + index.to_s, [
						external_account_id,
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
					conn.exec("DEALLOCATE " + prep_prefix.to_s + "insert_message_statement_" + index.to_s); nil
					retry

				end

			else   

			begin 
			
				api = gmail_token.mail_api; nil
				result = api.messages(fullquery);nil
				messages_count = result.count; nil

				if messages_count == 0

					missing_reason = 'Query returned no results'

					#Insert Record
					begin
						conn.prepare(prep_prefix.to_s + 'insert_message_statement_' + index.to_s, 'INSERT INTO datasci_projects.broad_inbox_messages (
							external_account_id
							, missing_reason
							, "from"
							, "to"
							, subject
							, "date"
							, cc
							, body_text
							, message_id
							, labels
							, n_attachments
							, attachment_names
							, created_at
							) values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)')
				          
						conn.exec_prepared(prep_prefix.to_s + 'insert_message_statement_' + index.to_s, [
							external_account_id,
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
						conn.exec("DEALLOCATE " + prep_prefix.to_s + "insert_message_statement_" + index.to_s); nil
						retry

	      			end

				else

				    begin       
						$stderr.puts 'Requesting message count.'
						
						result = api.messages(fullquery);nil					
						messages_count = result.count;nil	

						messages = result.take(messages_count);nil

						missing_reason = ''

						messages.each_with_index do |message, m|

							if message.attachments.present?

								attachment_names = []

								message.attachments.select do |attachment|

									attachment_names << attachment.content_disposition

								end

								attachment_names = attachment_names.join(',')

							else

								attachment_names = nil

							end

							conn.prepare(prep_prefix.to_s + 'insert_message_statement_' + index.to_s + '_' + m.to_s, 'INSERT INTO datasci_projects.broad_inbox_messages (
							external_account_id
							, missing_reason
							, "from"
							, "to"
							, subject
							, "date"
							, cc
							, body_text
							, message_id
							, labels
							, n_attachments
							, attachment_names
							, created_at
							) values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)')
				          
						conn.exec_prepared(prep_prefix.to_s + 'insert_message_statement_' + index.to_s + '_' + m.to_s, [
							external_account_id, 
							missing_reason, 
							message.from&.first, 
							(!message.to.nil? && message.to.kind_of?(Array) ? (message.to.join(',')) : (nil)), 
							(!message.subject.nil? ? (message.subject.force_encoding('utf-8')) : (message.subject)), 
							message.date,
							(message.cc.kind_of?(Array) ? (message.cc&.join(',')) : (nil)),
							(!message.text.nil? ? (message.text.force_encoding('utf-8')) : (message.text)), 
							message.header[GmailAPI::X_GMAIL_MESSAGE_ID].value,
							message.header[GmailAPI::X_GMAIL_LABEL_IDS].value,
							message.attachments.count,
							attachment_names,
							Time.now.getutc])

						#Deallocate insert
							conn.exec("DEALLOCATE " + prep_prefix.to_s + "insert_message_statement_" + index.to_s + '_' + m.to_s); nil		        

		        		end

	        		rescue Google::Apis::ClientError => client_violation
	          			$stderr.puts "Encountered exception #{client_violation.message}".blue

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

					end

				end

			rescue Google::Apis::ClientError => client_violation
			  	$stderr.puts "Encountered exception #{client_violation.message}".blue		

			end

		end

	end

rescue Google::Apis::ClientError => client_violation
  	$stderr.puts "Encountered exception #{client_violation.message}".blue

rescue Mail::UnknownEncodingType => encode_violation
	$stderr.puts "Encountered exception #{encode_violation.message}".blue

rescue Google::Apis::TransmissionError => trans_violation
	$stderr.puts "Encountered exception #{trans_violation.message}".blue

rescue GoogleAccountOAuthClient::InvalidGrantError => grant_violation
	$stderr.puts "Encountered exception #{grant_violation.message}".blue

rescue ActiveRecord::StatementInvalid => statement_violation
	$stderr.puts "Encountered exception #{statement_violation.message}".blue	

end
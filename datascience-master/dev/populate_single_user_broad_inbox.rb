
email_address = 'goughgorski@gmail.com'

def populate_internal_broad_inbox_table(email_address)

	conn_read_only = PG.connect(:dbname => 'dfpa89c6j8adi3', :host => 'ec2-18-233-212-115.compute-1.amazonaws.com',
	 :user => 'amitree-read-only', :password => 'p19e020a83545accec53ed2ad75eddcfafb3c20621a73961ff1e6ef8b2fcc8263')

	conn_ds = PG.connect(:dbname => 'dupr2vdkvoban', :host => 'ec2-34-237-233-59.compute-1.amazonaws.com',
	 :user => 'uenrajq17b9i6e', :password => 'p9a513552fea4dfd276a6dcbddbeb5e3de4766bf96cdd42790464b0a427f8e59f')

	select_query = "SELECT id FROM public.oauth_accounts WHERE email = '" + email_address.to_s + "'"

	prep_prefix = 'pos_'
	conn_read_only.prepare(prep_prefix.to_s+'select_statement', select_query)
	events = conn_read_only.exec_prepared(prep_prefix.to_s + 'select_statement')
	keys = events.fields; nil
	events = events.values; nil

	event = events[0] #added for single instance of loop
	index = 0; nil

	#Collect basic information
	$stderr.puts  "[#{Time.now}] Collecting basic Tx information"
	oauth_account_id = event[keys.index("id")].to_i

	db = ActiveRecord::Base.connection.current_database; nil

	begin
		gmail_token = OAuthAccount.ok.find_by_id(oauth_account_id)
	end

		if gmail_token.nil?

		    missing_reason = 'No Gmail Token'

		    #Insert Record
			begin
				conn_ds.prepare(prep_prefix.to_s + 'insert_message_statement_' + index.to_s, 'INSERT INTO datasci_projects.internal_broad_inbox_messages (
					oauth_account_id
					, email_address
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
					) values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)')
		          
				conn_ds.exec_prepared(prep_prefix.to_s + 'insert_message_statement_' + index.to_s, [
					oauth_account_id,
					email_address.to_s,
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
		           	conn_ds.reset
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
				conn_ds.exec("DEALLOCATE " + prep_prefix.to_s + "insert_message_statement_" + index.to_s); nil
				retry


			end

		else   

		begin 
		
			api = gmail_token.mail_api; nil
			result = api.messages('');nil
			messages_count = result.count; nil

			if messages_count == 0

				missing_reason = 'Query returned no results'

				#Insert Record
				begin
					conn_ds.prepare(prep_prefix.to_s + 'insert_message_statement_' + index.to_s, 'INSERT INTO datasci_projects.internal_broad_inbox_messages (
						oauth_account_id
						, email_address
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
						) values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)')
			          
					conn_ds.exec_prepared(prep_prefix.to_s + 'insert_message_statement_' + index.to_s, [
						oauth_account_id,
						email_address.to_s,
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
		            	conn_ds.reset
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
					conn_ds.exec("DEALLOCATE " + prep_prefix.to_s + "insert_message_statement_" + index.to_s); nil
					retry

	  			end

			else

			    begin       
					$stderr.puts 'Requesting message count.'
					
					result = api.messages('');nil					
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

						conn_ds.prepare(prep_prefix.to_s + 'insert_message_statement_' + index.to_s + '_' + m.to_s, 'INSERT INTO datasci_projects.internal_broad_inbox_messages (
						oauth_account_id
						, email_address
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
						) values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)')
			          
					conn_ds.exec_prepared(prep_prefix.to_s + 'insert_message_statement_' + index.to_s + '_' + m.to_s, [
						oauth_account_id,
						email_address.to_s,
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
						conn_ds.exec("DEALLOCATE " + prep_prefix.to_s + "insert_message_statement_" + index.to_s + '_' + m.to_s); nil		        

	        		end

	    		rescue Google::Apis::ClientError => client_violation
	      			$stderr.puts "Encountered exception #{client_violation.message}".blue

	    		#Reconnect
				rescue PG::UnableToSend, PG::ConnectionBad => pg_exception
					begin
	            		conn_ds.reset
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
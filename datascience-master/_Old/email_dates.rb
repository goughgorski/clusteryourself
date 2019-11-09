

conn = PG.connect(:dbname => 'development', :host => 'localhost', :user => 'postgres')

select_query = "SELECT dspta.external_account_id
				, dspta.created_at
				, property_transaction_id
				, street_addr
		FROM datasci_projects.tranche_assignments dspta
		LEFT JOIN datasci.property_transactions dspt on dspt.external_account_id = dspta.external_account_id
		LEFT JOIN datasci.properties dsp on dsp.id = dspt.property_id
		WHERE dspta.treatment = true
		AND confirmation_status IN ('confirmed', 'manual')"

prep_prefix = 'pos_'
conn.prepare(prep_prefix.to_s+'select_statement', select_query)
events = conn.exec_prepared(prep_prefix.to_s + 'select_statement')
keys = events.fields; nil
events = events.values; nil

begin
	    Parallel.each_with_index(events.each) do |event, index|
			
			#event = events[1] #added for single instance of loop
			#index = 0

			#Collect basic information
			$stderr.puts  "[#{Time.now}] Collecting basic Tx information"
			external_account_id = event[keys.index("external_account_id")].to_i; nil
			tranche_assignment_date = event[keys.index("created_at")]; nil
			tranche_assignment_date = Time.parse tranche_assignment_date; nil
			property_transaction_id = event[keys.index("property_transaction_id")].to_i; nil
			street_address = event[keys.index("street_addr")]

			db = ActiveRecord::Base.connection.current_database; nil

			begin
				gmail_token = ExternalAccount.ok.find_by_id(external_account_id)
				transaction = PropertyTransaction.find(property_transaction_id); nil
			end

			    #If no gmail token, set missing and write record
			    if gmail_token.nil?

			        missing_reason = 'No Gmail Token'

			        #Insert Record
					begin
						conn.prepare(prep_prefix.to_s + 'insert_message_statement_' + index.to_s, 'INSERT INTO datasci_projects.tranche_assignments_email_dates (
							external_account_id 
							, property_transaction_id
							, tranche_assignment_date
							, last_email_date
							, email_scan_date
							, missing_reason
							) values ($1, $2, $3, $4, $5, $6)')
				          
						conn.exec_prepared(prep_prefix.to_s + 'insert_message_statement_' + index.to_s, [
							external_account_id,
							property_transaction_id,
							tranche_assignment_date,
							nil, 
							Time.now.getutc,
							missing_reason])

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
		      
			    #If token present proceed with query and collect emails
			    else   

				api = gmail_token.mail_api

				result = api.messages(street_address);nil

				messages_count = result.count; nil

				if messages_count == 0

					missing_reason = 'Query returned no results'

					#Insert Record
					begin

					    conn.prepare(prep_prefix.to_s + 'insert_message_statement_' + index.to_s, 'INSERT INTO datasci_projects.tranche_assignments_email_dates (
							external_account_id 
							, property_transaction_id
							, tranche_assignment_date
							, last_email_date
							, email_scan_date
							, missing_reason
							) values ($1, $2, $3, $4, $5, $6)')
				          
						conn.exec_prepared(prep_prefix.to_s + 'insert_message_statement_' + index.to_s, [
							external_account_id,
							property_transaction_id,
							tranche_assignment_date,
							nil, 
							Time.now.getutc,
							missing_reason])
			        
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
						messages = []
						result.each do |message|
						messages << message
					end
					rescue Google::Apis::RateLimitError
						sleep 10
						retry
					end

					date = messages[0].date

					missing_reason = ''

						begin 
								
							conn.prepare(prep_prefix.to_s + 'insert_message_statement_' + index.to_s, 'INSERT INTO datasci_projects.tranche_assignments_email_dates (
								external_account_id 
								, property_transaction_id
								, tranche_assignment_date
								, last_email_date
								, email_scan_date
								, missing_reason
								) values ($1, $2, $3, $4, $5, $6)')

							conn.exec_prepared(prep_prefix.to_s + 'insert_message_statement_' + index.to_s, [
								external_account_id,
								property_transaction_id,
								tranche_assignment_date,
								date,
								Time.now.getutc,
								missing_reason])

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

				end		
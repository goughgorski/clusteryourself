#!/usr/bin/env ruby

Rails.logger.level = :warn

def process_transaction_events(schema, tablename, eventidcol, store = true, processes = 1, *query)

  if !query.nil?
    query = query.first.to_s  
  end

  conn = PG.connect(:dbname => 'development', :host => 'localhost', :user => 'postgres')    
  select_query = 'SELECT %{eventidcol} FROM %{schema}.%{tablename}' % {:eventidcol => eventidcol, :schema => schema, :tablename => tablename }
  conn.prepare('select_statement', select_query)
  eventids = conn.exec_prepared('select_statement').values.flatten

  $stderr.puts "[#{Time.now}] Began pulling data".green

  i = 1
  params = []
  eventids.each { params.push(i); i+=1; }

  begin
    conn.exec("DEALLOCATE select_statement"); nil
  rescue 
  end

  conn.prepare('select_statement', 'SELECT * FROM datasci.property_transactions_ds WHERE id IN ($'+params.join(',$')+')')
  events = conn.exec_prepared('select_statement', eventids)
  keys = events.fields; nil
  events = events.values; nil

  total_count = events.count
  $stderr.puts "[#{Time.now}] Begin parallel processing".green

  begin
    Parallel.each_with_index(events.each, in_processes: processes) do |event, index|
      $stderr.puts "[#{Time.now}][#{index+1}/#{total_count}] Looking at #{event[keys.index("type")]} ##{event[keys.index("id")]} for #{event[keys.index("address")]}".green            
      external_account_id = event[keys.index("external_account_id")].to_i; nil
      property_transaction_id = event[keys.index("property_transaction_id")].to_i; nil
      event_type = event[keys.index("type")]; nil
      messages_id = []
      messages_id << event[keys.index("originating_message_id")]; nil

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
        else
          begin
            gmail_token = GmailToken.ok.find_by_id(external_account_id)
            transaction = PropertyTransaction.find(property_transaction_id)
            representation = representation_for_transaction(transaction)
          rescue ActiveRecord::RecordNotFound
            representation = :unknown
          end
        end
      end

      begin
        conn.exec("DEALLOCATE insert_statement_" + index.to_s); nil
        rescue 
      end

      if gmail_token.nil?

        missing_reason = 'No Gmail Token'

        if !query.nil?

          conn.prepare('insert_statement_'+index.to_s, 'INSERT INTO datasci.query_comparison (
                event_id 
                , property_transaction_id 
                , event_type 
                , missing_reason   
                , message_id
                , query
                , found_indicator 
                , created_at 
                ) values ($1, $2, $3, $4, $5, $6, $7, $8)')
              
          conn.exec_prepared('insert_statement_'+index.to_s, [
            event[keys.index("id")].to_i,
            property_transaction_id, 
            event_type, 
            missing_reason, 
            nil, 
            query, 
            nil, 
            Time.now.getutc])

        end

        if store

          conn.prepare('insert_message_statement_'+index.to_s, 'INSERT INTO datasci.transaction_messages (
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
              
          conn.exec_prepared('insert_message_statement_'+index.to_s, [
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

        end

        $stderr.puts missing_reason.red
        
        begin
          ActiveRecord::Base.connection.current_database; nil
        rescue
          begin
            ActiveRecord::Base.connection.reconnect!
          rescue
            sleep 10
            retry
          end
        end
      
      #If token present - get message(s)
      else   
        api = GmailAPI.new(gmail_token)        
        
        #If no message ids check for dates to pull message ids
        if messages_id.reject(&:blank?).count == 0

          if representation == :seller
            date = transaction.timeline.events.listing_date.take.try(:date)
          elsif representation == :buyer
            date = transaction.timeline.events.offer_accepted.take.try(:date)
          end

          if (!date)
          
            missing_reason = 'No Message Id or Date Found'

            if !query.nil?         
            
              conn.prepare('insert_statement_'+index.to_s, 'INSERT INTO datasci.query_comparison (
                  event_id 
                  , property_transaction_id 
                  , event_type 
                  , missing_reason   
                  , message_id
                  , query
                  , found_indicator 
                  , created_at 
                  ) values ($1, $2, $3, $4, $5, $6, $7, $8)')
                
              conn.exec_prepared('insert_statement_'+index.to_s, [
                event[keys.index("id")].to_i,
                property_transaction_id, 
                event_type, 
                missing_reason, 
                nil, 
                query, 
                nil, 
                Time.now.getutc])

            end

            if store

              conn.prepare('insert_message_statement_'+index.to_s, 'INSERT INTO datasci.transaction_messages (
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
                
              conn.exec_prepared('insert_message_statement_'+index.to_s, [
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
        
            end

            $stderr.puts missing_reason.red

            begin
              ActiveRecord::Base.connection.current_database; nil
            rescue
              begin
                ActiveRecord::Base.connection.reconnect!
              rescue
                sleep 10
                retry
              end
            end

          else 
            
            start_date = (date - 1.day).beginning_of_day
            end_date = date.end_of_day
            $stderr.puts "  Looking for emails with the label '#{transaction.label_name}' from #{start_date} to #{end_date}"
        
            api.messages("after:#{start_date.to_i} before:#{end_date.to_i}", label_ids: transaction.label_id, format: 'metadata', batched: true).each do |message_metadata|
              m_id = message_metadata.header[GmailAPI::X_GMAIL_MESSAGE_ID].value
              $stderr.puts "   Found '#{message_metadata.subject}' with id #{m_id}"
              messages_id << m_id
            end
          
            #If still no messages found end, with missing reason, otherwise, get data
            if messages_id.reject(&:blank?).count == 0

              missing_reason = 'No Messages Found'

              if !query.nil?
                
                conn.prepare('insert_statement_'+index.to_s, 'INSERT INTO datasci.query_comparison (
                      event_id 
                      , property_transaction_id 
                      , event_type 
                      , missing_reason   
                      , message_id
                      , query
                      , found_indicator 
                      , created_at 
                      ) values ($1, $2, $3, $4, $5, $6, $7, $8)')
                    
                conn.exec_prepared('insert_statement_'+index.to_s, [
                  event[keys.index("id")].to_i,
                  property_transaction_id, 
                  event_type, 
                  missing_reason, 
                  nil, 
                  query, 
                  nil, 
                  Time.now.getutc])

              end

              if store

                conn.prepare('insert_message_statement_'+index.to_s, 'INSERT INTO datasci.transaction_messages (
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
                    
                conn.exec_prepared('insert_message_statement_'+index.to_s, [
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

              end
              
              $stderr.puts missing_reason.red 

              begin
                ActiveRecord::Base.connection.current_database; nil
              rescue
                begin
                  ActiveRecord::Base.connection.reconnect!
                rescue
                  sleep 10
                  retry
                end
              end
            end
          end
        end

        if messages_id.reject(&:blank?).count != 0
        
          messages = [] 
          
          messages_id.reject(&:blank?).each do |message_id|
            
            message = fetch_message(message_id, api)
          
            if !message
              missing_reason = 'Requested Message Not Found'

              if !query.nil?

                conn.prepare('insert_statement_'+index.to_s, 'INSERT INTO datasci.query_comparison (
                    event_id 
                    , property_transaction_id 
                    , event_type 
                    , missing_reason   
                    , message_id
                    , query
                    , found_indicator 
                    , created_at 
                    ) values ($1, $2, $3, $4, $5, $6, $7, $8)')
                  
                conn.exec_prepared('insert_statement_'+index.to_s, [
                  event[keys.index("id")].to_i,
                  property_transaction_id, 
                  event_type, 
                  missing_reason, 
                  message_id, 
                  query, 
                  nil, 
                  Time.now.getutc])

              end

              
              if store

                conn.prepare('insert_message_statement_'+index.to_s, 'INSERT INTO datasci.transaction_messages (
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
                    
                conn.exec_prepared('insert_message_statement_'+index.to_s, [
                  event[keys.index("id")].to_i, 
                  property_transaction_id, 
                  event_type, 
                  representation, 
                  missing_reason, 
                  message_id, 
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
                
              end

              $stderr.puts missing_reason.red

            else

              messages << message  

            end  
          
          end
        
          #If no messages available, exit, otherwise, process messages

          if messages.compact.count == 0

            $stderr.puts "No messages found for Tx #{event[keys.index("property_transaction_id")]}."

            begin
              ActiveRecord::Base.connection.current_database; nil
            rescue
              begin
                ActiveRecord::Base.connection.reconnect!
              rescue
                sleep 10
                retry
              end
            end

          else
          
            messages = messages.compact

            messages.each do |message|

              this_message_id = message.header[GmailAPI::X_GMAIL_MESSAGE_ID]

              if !query.nil?

                if message.message_id.nil?

                  missing_reason = 'Message ID Missing From Header'

                  conn.prepare('insert_statement_'+index.to_s, 'INSERT INTO datasci.query_comparison (
                      event_id 
                      , property_transaction_id 
                      , event_type 
                      , missing_reason   
                      , message_id
                      , query
                      , found_indicator 
                      , created_at 
                      ) values ($1, $2, $3, $4, $5, $6, $7, $8)')
                    
                  conn.exec_prepared('insert_statement_'+index.to_s, [
                    event[keys.index("id")].to_i,
                    property_transaction_id, 
                    event_type, 
                    missing_reason, 
                    this_message_id, 
                    query, 
                    nil, 
                    Time.now.getutc])

                  $stderr.puts missing_reason.red

                else
                                
                  $stderr.puts "[#{Time.now}] Running query".green
                  
                  id_query = ' AND rfc822msgid:'+ message.message_id
                  full_query = query + id_query
                  #Sometimes message_id returns more than one message!!!! Convert to binary here
                  query_count = api.messages(full_query).count > 1 ? 1 : api.messages(full_query).count 

                  $stderr.puts "[#{Time.now}] Writing query results".green

                  begin
                    conn.exec("DEALLOCATE insert_statement_" + index.to_s); nil
                    rescue 
                  end

                  conn.prepare('insert_statement_'+index.to_s, 'INSERT INTO datasci.query_comparison (
                        event_id 
                        , property_transaction_id 
                        , event_type 
                        , missing_reason   
                        , message_id
                        , query
                        , found_indicator 
                        , created_at 
                      ) values ($1, $2, $3, $4, $5, $6, $7, $8)')
                      
                  conn.exec_prepared('insert_statement_'+index.to_s, [
                    event[keys.index("id")].to_i, 
                    property_transaction_id,
                    event_type,
                    nil, 
                    this_message_id, 
                    query, 
                    query_count, 
                    Time.now.getutc])

                end

              end

              if store

                $stderr.puts "[#{Time.now}] Storing message data".green

                conn.prepare('insert_message_statement_'+index.to_s, 'INSERT INTO datasci.transaction_messages (
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
                
                conn.exec_prepared('insert_message_statement_'+index.to_s, [
                  event[keys.index("id")].to_i,
                  property_transaction_id,
                  event_type,
                  representation,
                  nil,        
                  this_message_id,
                  message.from&.first,
                  (message.to.join(',') if !message.to.nil?),
                  message.sender,
                  message.subject,
                  message.date,
                  message.cc&.join(','),
                  message.mime_version,
                  message.text,
                  (message.html.delete("\u0000") if !message.html.nil?),
                  Time.now.getutc])          

                  begin
                    conn.exec("DEALLOCATE insert_message_statement_" + index.to_s); nil
                    rescue 
                  end

                begin
                  add_transaction_documents(property_transaction_id, this_message_id, message.attachments, conn, index)
                rescue PG::UnableToSend, PG::ConnectionBad => pg_exception
                  ActiveRecord::Base.connection.reconnect!
                end

              end

            end

            begin
              ActiveRecord::Base.connection.current_database; nil
            rescue
              begin
                ActiveRecord::Base.connection.reconnect!
              rescue
                sleep 10
                retry
              end
            end
          
          end      
        end
      end
    
      begin
        conn.exec("DEALLOCATE insert_statement_" + index.to_s); nil
        rescue 
      end
      
      begin
        conn.exec("DEALLOCATE insert_message_statement_" + index.to_s); nil
        rescue 
      end    
    
    end
    $stderr.puts "[#{Time.now}] Finish parallel processing".green
  rescue Interrupt # thrown by Parallel when SIGTERM sent
    $stderr.puts "[#{Time.now}] Parallel received TERM signal, stopping."
  end
  $stderr.puts "[#{Time.now}] Finished pulling data".green  
end

def fetch_message(message_id, api)
  $stderr.puts "  Fetching message #{message_id}"
  begin
    retries ||= 1
    $stderr.puts "  Hitting Google API - attempt #{retries} of 10"
    message = api.find_message(message_id)
    return if !message
    unless message.raw_source.chars.all?(&:valid_encoding?)
      missing_reason = 'Invalid Raw Source Encoding'
      $stderr.puts missing_reason.red
      return
    end 
  rescue => exception
      $stderr.puts "  Encountered exception #{exception.message}".red
  rescue Net::ReadTimeout      
    if (retries += 1) <= 10
      $stderr.puts "  Timed out hitting Google API - retrying."
      retry
    else
      missing_reason = 'Fetch Tries Hit Maximum'
      $stderr.puts missing_reason.red
      return 
    end  
  end
  return message
end

def add_transaction_documents(property_transaction_id, message_id, attachments, conn, index)
  if attachments.present?
    $stderr.puts "[#{Time.now}] Storing attachment data".green
    pdf_attachments = attachments.select do |attachment|
      attachment.mime_type == 'application/pdf' || attachment.filename.downcase.ends_with?('.pdf')
    end

    pdf_attachments.each_with_index do |pdf_attachment|
      pdf = pdf_attachment.decoded
      $stderr.puts "  PDF attachment: #{pdf_attachment.filename}"
      
      if text = extract_pdf_text(pdf) and text.size > 300
        $stderr.puts "    Text size without OCR: #{text.size}"
      elsif text = extract_ocr_text(pdf)
        $stderr.puts "    Text size from OCR: #{text.size}"
      end

      if (text.size > 0)

        conn.prepare('insert_document_statement_'+index.to_s, 'INSERT INTO datasci.transaction_documents (
          property_transaction_id  
          , message_id 
          , attachment_name 
          , document_content
          , created_at
          ) values ($1, $2, $3, $4, $5)')
        
        begin
          conn.exec_prepared('insert_document_statement_'+index.to_s, [
            property_transaction_id,
            message_id,
            pdf_attachment.filename,
            text, 
            Time.now.getutc])
          rescue PG::UniqueViolation => pg_violation
            $stderr.puts "Encountered exception #{pg_violation.message}".red
        end
        
        begin
          conn.exec("DEALLOCATE insert_document_statement_"+index.to_s); nil
          rescue 
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
  Yomu.read :text, pdf
rescue Exception => error
  Rails.logger.fatal("Failed to extract text from PDF\n#{error.class} (#{error.message})")
  nil
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
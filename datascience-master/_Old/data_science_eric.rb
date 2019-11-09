Rails.logger.level = :warn

require 'fileutils'

DEFAULT_EVENT_ID = 11740185

def populate_annotated_transactions
  $stderr.puts "[#{Time.now}] Grabbing max id".green
  max_id = AnnotatedTransaction.order(:event_id).last&.event_id || DEFAULT_EVENT_ID
  $stderr.puts "[#{Time.now}] max_id = #{max_id}".green
  $stderr.puts "[#{Time.now}] Began pulling data".green    
  begin
    ActiveRecord::Base.connection_pool.with_connection do
      events = Event.where(type: %w[ PropertyTransactionConfirmedEvent PropertyTransactionRejectedEvent ]).where('id > ?', max_id).reorder(:id)
      process_events events, AnnotatedTransaction
    end
  rescue Interrupt # thrown by Parallel when SIGTERM sent
    $stderr.puts "[#{Time.now}] Parallel received TERM signal, stopping."
  end
  $stderr.puts "[#{Time.now}] Finished pulling data".green  
end

def populate_manual_transactions
  max_id = ManualTransaction.order(:event_id).last&.event_id || DEFAULT_EVENT_ID
  begin
    ActiveRecord::Base.connection_pool.with_connection do
      events = PropertyTransactionManuallyCreatedEvent.where('id > ?', max_id).reorder(:id)
      process_events events, ManualTransaction
    end
  rescue Interrupt # thrown by Parallel when SIGTERM sent
    $stderr.puts "[#{Time.now}] Parallel received TERM signal, stopping."
  end  
end

def process_events(events, model)
  total_count = events.count
  $stderr.puts "[#{Time.now}] Begin parallel processing".green
  Parallel.each_with_index(events.find_each(batch_size: 300), in_processes: 10, interrupt_signal: 'TERM') do |event, index|
    $stderr.puts "[#{Time.now}][#{index+1}/#{total_count}] Looking at #{event.type} ##{event.id} for #{event.event_data[:address]}".green      
    begin      
      process_event(event, index, model)
    rescue PG::UnableToSend => pg_exception
      $stderr.puts "[#{Time.now}] Encountered Postgres 'UnableToSend' exception. Attempting graceful shutdown.".red
      raise Parallel::Break    
    end
  end
  $stderr.puts "[#{Time.now}] Finish parallel processing".green
end

def process_event(event, index, model)  
  external_account_id = event.event_data[:external_account_id]
  gmail_token = GmailToken.ok.find_by_id(external_account_id)
  if gmail_token.nil?
    $stderr.puts "  GmailToken is gone, skipping".red
    return
  end

  api = GmailAPI.new(gmail_token)

  gmail_email = gmail_token.email

  property_transaction_id = event.event_data[:property_transaction_id]
  begin
    transaction = PropertyTransaction.find(property_transaction_id)
    representation = representation_for_transaction(transaction)
  rescue ActiveRecord::RecordNotFound
    representation = :unknown
  end

  row = {
    event_id: event.id,
    event_created_at: event.created_at,
    acting_user_id: event.acting_user_id,
    external_account_id: external_account_id,
    gmail_email: gmail_email,
    property_transaction_id: property_transaction_id,
    address: event.event_data[:address],
    street_addr: event.event_data[:street_addr],
    representation: representation
  }

  if model == AnnotatedTransaction
    message_id = event.event_data[:originating_message_id]
    if (!message_id)
      $stderr.puts "  No message_id, skipping".red
      return
    end

    begin
      message = fetch_message(message_id, gmail_token)
      return if !message
    rescue => exception
      $stderr.puts "  Encountered exception #{exception.message}".red
      return
    end

    row.merge!({
      event_type: event.type,
      rejection_reason: event.event_data[:rejection_reason],
      originating_message_id: message_id,
      raw_message: message.raw_source
    })
  elsif model == ManualTransaction
    if !transaction
      $stderr.puts "  No transaction found, skipping".red
      return
    end

    if representation == :seller
      date = transaction.timeline.events.listing_date.take.try(:date)
    elsif representation == :buyer
      date = transaction.timeline.events.offer_accepted.take.try(:date)
    end

    if !date
      $stderr.puts "  No initiation date found, skipping".red
      return
    end

    start_date = (date - 1.day).beginning_of_day
    end_date = date.end_of_day
    $stderr.puts "  Looking for emails with the label '#{transaction.label_name}' from #{start_date} to #{end_date}"

    # find messages for the right date (listing date or offer accepted date)
    messages = []
    api.messages("after:#{start_date.to_i} before:#{end_date.to_i}", label_ids: transaction.label_id, format: 'metadata', batched: true).each do |message_metadata|
      m_id = message_metadata.header[GmailAPI::X_GMAIL_MESSAGE_ID].value
      $stderr.puts "   Found '#{message_metadata.subject}' with id #{m_id}"
      messages << fetch_message(m_id, gmail_token)
    end
    messages = messages.compact

    if messages.count == 0
      $stderr.puts "  No messages found, skipping".red
      return
    end
  end

  begin
    record = model.create(row)

    if message
      messages = [message]
    end

    messages.each do |message|
      if model == ManualTransaction
        this_message_id = message.header[GmailAPI::X_GMAIL_MESSAGE_ID]
        record.manual_transaction_messages.create(message_id: this_message_id, raw_message: message.raw_source)
      end

      add_transaction_documents(property_transaction_id, message_id, message.attachments)
    end
  rescue ActiveRecord::RecordNotUnique => e
    $stderr.puts "  Record not unique: #{event.id}".red
  rescue ActiveRecord::StatementInvalid => e
    $stderr.puts "  Record invalid: #{event.id} (#{e.message})".red
  rescue => exception
    $stderr.puts "  Encountered exception #{exception.message}".red
  end
rescue GmailToken::InvalidGrantError
end

def fetch_message(message_id, gmail_token)
  $stderr.puts "  Fetching message #{message_id}"
  begin
    retries ||= 1
    $stderr.puts "  Hitting Google API - attempt #{retries} of 10"
    api = GmailAPI.new(gmail_token)
    message = api.find_message(message_id)
    unless message.raw_source.chars.all?(&:valid_encoding?)
      $stderr.puts "  Encountered invalid encoding in the raw_source for event #{event.id}".red
      return
    end
  rescue Net::ReadTimeout      
    if (retries += 1) <= 10
      $stderr.puts "  Timed out hitting Google API - retrying."
      retry
    else
      $stderr.puts "  Tried fetching message #{message_id} 10 times - giving up."
      return
    end  
  end

  return message
end

def populate_transaction_emails
  transactions = PropertyTransaction.confirmed.where('created_at BETWEEN ? AND ?', Date.parse('April 1, 2017').beginning_of_day, Date.parse('April 30, 2017').end_of_day).limit(1000)
  total_count = transactions.count
  transactions.each_with_index do |transaction, index|
    $stderr.puts "[#{index+1}/#{total_count}] Grabbing emails for transaction ##{transaction.id} for #{transaction.address.serialized}".green

    representation = representation_for_transaction(transaction)    

    begin
      api = GmailAPI.new(transaction.gmail_token)
      messages = api.messages(nil, label_ids: transaction.label_id, format: :raw, batched: true)
      messages.each do |message|
        $stderr.puts "  Storing message #{message.message_id}"

        row = {
          external_account_id: transaction.external_account_id,
          property_transaction_id: transaction.id,
          address: transaction.address.serialized,
          representation: representation,
          raw_message: message.raw_source
        }
        # puts row.except(:raw_message)

        TransactionEmail.create(row)
      end
    rescue => exception
      $stderr.puts "  Encountered exception #{exception.message}".red
      next
    end

  end
end

def add_transaction_documents(property_transaction_id, message_id, attachments)
  if attachments.present?
    pdf_attachments = attachments.select do |attachment|
      attachment.mime_type == 'application/pdf' || attachment.filename.downcase.ends_with?('.pdf')
    end

    pdf_attachments.each do |pdf_attachment|
      pdf = pdf_attachment.decoded
      $stderr.puts "  PDF attachment: #{pdf_attachment.filename}"
      
      if text = extract_pdf_text(pdf) and text.size > 300
        $stderr.puts "    Text size without OCR: #{text.size}"
      elsif text = extract_ocr_text(pdf)
        $stderr.puts "    Text size from OCR: #{text.size}"
      end

      if (text.size > 0)
        TransactionDocument.create(
          property_transaction_id: property_transaction_id, 
          attachment_name: pdf_attachment.filename, 
          document_content: text,
          message_id: message_id
        )
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

def backfill_representation
  $stderr.puts "Fetching annotated transactions that do not have a representation set"
  AnnotatedTransaction.select(:id, :representation, :property_transaction_id).includes(:property_transaction).where(representation: nil).find_each do |annotated_transaction|
    property_transaction = annotated_transaction.property_transaction
    $stderr.puts "Attempting to update representation for annotated transaction ##{annotated_transaction.id}".green
    representation = representation_for_transaction(property_transaction)
    $stderr.puts "    #{representation}"
    annotated_transaction.update(representation: representation)
  end
end

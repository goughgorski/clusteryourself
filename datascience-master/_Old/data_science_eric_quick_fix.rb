Rails.logger.level = :info

def populate_annotated_transactions
  $stderr.puts "[#{Time.now}] Grabbing min id".green
  min_id = AnnotatedTransaction.select('min(event_id)').take.min || 1
  $stderr.puts "[#{Time.now}] min_id = #{min_id}".green
  $stderr.puts "[#{Time.now}] Began pulling data".green    
  begin
    ActiveRecord::Base.connection_pool.with_connection do
    events = Event.where(type: ['PropertyTransactionConfirmedEvent', 'PropertyTransactionRejectedEvent']).where('id >= ?', min_id).pluck(:id, :event_data)
    process_events events, AnnotatedTransaction  
  end
  rescue Interrupt
    $stderr.puts "[#{Time.now}] Parallel received TERM signal, stopping."
  end
  $stderr.puts "[#{Time.now}] Finished pulling data".green  
end

def process_events(events, model)
  total_count = events.count  
  $stderr.puts "[#{Time.now}] Begin parallel processing".green
  Parallel.each_with_index(events, in_processes: 10, interrupt_signal: 'INT') do |event, index|
    $stderr.puts "[#{Time.now}][#{index+1}/#{total_count}]".green      
    begin      
      process_event(event)    
    rescue => e
      $stderr.puts "[#{Time.now}] Encountered exception #{e.message}.".red      
      raise Parallel::Break    
    end
  end
  $stderr.puts "[#{Time.now}] Finish parallel processing".green
end

def process_event(event)  
  # event => [12345, {rejection_reason: "foobar"}]
  event_id = event.first
  rejection_reason = event.last[:rejection_reason]
  found_event = AnnotatedTransaction.where(event_id: event_id).first
  found_event&.update(rejection_reason: rejection_reason)    
end


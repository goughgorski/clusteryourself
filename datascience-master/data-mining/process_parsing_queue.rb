#!/usr/bin/env ruby
Dir["../datascience/data-mining/*.rb"].each {|file| require file }
Rails.logger.level = :warn

def process_parsing_queue(schema:, tablename:, docidcol:, endpointcol:, processes:1)

	###Set Basic Parameters
	uniqnum = (rand * 1000000).to_int.to_s	
	conn = PG.connect(:dbname => 'development', :host => 'localhost', :user => 'postgres')    

	select_query = 'SELECT %{endpointcol}, %{docidcol} FROM %{schema}.%{tablename}' % {:endpointcol => endpointcol, :docidcol => docidcol, :schema => schema, :tablename => tablename }
	conn.prepare('select_statement_' + uniqnum, select_query)
	endpoints = conn.exec_prepared('select_statement_' + uniqnum).values
	total_count = endpoints.count; nil

	begin
		conn.exec("DEALLOCATE select_statement_" + uniqnum); nil
		rescue 
	end

	###Process Queue
	$stderr.puts "[#{Time.now}] Processing Queue".green

	begin
	    Parallel.each_with_index(endpoints.each, in_processes: processes) do |endpoint, index|
			$stderr.puts "[#{Time.now}] Processing endpoint [#{index+1}/#{total_count}]".green
			parsed_doc_data = fetch_parsed_data(endpoint: endpoint[0], max_retries: 3)
			
			uniqnum = (rand * 1000000).to_int.to_s	
			update_where = "doc_id = #{endpoint[1]}"
			data = [endpoint[1], parsed_doc_data]
			insert_update(conn: conn, operation: 'upsert', schema: schema, table: 'offer_documents_parsing_payloads', data: data, update_where: update_where)
			$stderr.puts "[#{Time.now}] Finished processing endpoint [#{index+1}/#{total_count}]".green		
		
		Signal.trap("USR1") {
		
			$stderr.puts "[#{Time.now}] Parallel received signal, attempting to shut down."		
			raise Parallel::Break
			exit
		}		
		
		end
		
		$stderr.puts "[#{Time.now}] Finished parallel processing".green

	rescue Interrupt # thrown by Parallel when SIGINT sent

	end

	#Finished Pulling Data
	$stderr.puts "[#{Time.now}] Finished processing parsing queue".green  

end 


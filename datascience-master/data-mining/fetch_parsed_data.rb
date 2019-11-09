#!/usr/bin/env ruby

def fetch_parsed_data(endpoint:, max_retries: 10)

	auth = {username: ENV['DOCPARSER_KEY'], password: '' }
	retries = 1

	begin
		t0 = Time.now
		
		#Request parsed data
		parsed_doc_data = HTTParty.get(endpoint, basic_auth: auth)

		#If parsed data is not done, raise error and retry
		if (parsed_doc_data.select{|k,v| k == "error"}.count > 0)
			error = parsed_doc_data.select{|k,v| k == "error"}.values[0]
			raise RuntimeError.new("#{error}")

		#Otherwise write parsed data and meta-data
		else
			parse_time = Time.now - t0
			$stderr.puts "Round trip parsing time: #{parse_time} seconds".green
	    
	    end

	rescue RuntimeError => error
		if retries < max_retries
			retries += 1
			$stderr.puts "[#{Time.now}] Encountered error: #{error}. Will retry #{max_retries} times. Processing attempt #{retries}".white           
			
			if retries < (max_retries/2)
				sleep 30 * retries
				retry
			else
				sleep 5 ** retries
				retry
			end

		
		#If attempts exceed maximum, record parsing timeout
		else
			$stderr.puts "Timeout: #{error}".blue
			parsed_doc_data = nil
		end
	
	end

	request_parsed_reply ? parsed_doc_data_out = parsed_doc_data : parsed_doc_data_out = parsed_doc_uri

	return(parsed_doc_data_out)

end
#!/usr/bin/env ruby

def parse_document(document_id:, parser_keys:, parser:, pdf_attachment:, request_parsed_reply: false)

	pdf = pdf_attachment.decoded
	file_content = Base64.encode64(pdf)
	parser_endpoint = 'https://api.docparser.com/v1/document/upload/' + parser[parser_keys.index("parser_id")]
	auth = {username: ENV['DOCPARSER_KEY'], password: '' }

	$stderr.puts "[#{Time.now}] Sending #{pdf_attachment.filename} to #{parser[parser_keys.index('parser_name')]}".magenta
	parse_post = HTTParty.post(parser_endpoint, basic_auth: auth, body: {remote_id: document_id, file_content: file_content})

	#write parsed results
	doc_id = parse_post.select {|k,v| k == "id"}.values[0].to_s
	
	parsed_doc_uri = "https://api.docparser.com/v1/results/#{parser[parser_keys.index("parser_id")]}/#{doc_id}"
	parsed_doc_data = nil
	
	#if immediate response requested, call on DP now
	if (request_parsed_reply)

		$stderr.puts "[#{Time.now}] Fetching #{pdf_attachment.filename}, docid #{doc_id} from #{parser[parser_keys.index('parser_name')]} try #{retries}".magenta
		parsed_doc_data = fetch_parsed_data(endpoint: parsed_doc_uri)
		
	end

	if parsed_doc_data.nil?
		
		parsed_doc_data_out = parsed_doc_uri
		output = {"response_type" => 'URI',
			"response" => parsed_doc_data_out}
	
	else 

		$stderr.puts "Received parsed output".green			
		parsed_doc_data_out = parsed_doc_data
		output = {"response_type" => 'Parsed Data',
				"response" => parsed_doc_data_out}
				
	end

	return(output)

end
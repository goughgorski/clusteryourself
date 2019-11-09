#!/usr/bin/env ruby

def add_transaction_documents(schema:, metadata: {}, documents: [], conn:, store_doc_dir: '', try_doc_parse: false, request_parsed_reply: false, form_type: [])
 			
	#Get PDF text
	$stderr.puts "[#{Time.now}] Storing pdf attachment data".green

	if !(try_doc_parse)
		parser_name = "Document parsing not requested"	
		parsed_doc_data = {}
		parsers = []
	end

	documents.each_with_index do |pdf_attachment,p|

		pdf = pdf_attachment.decoded
		$stderr.puts "  PDF attachment: #{pdf_attachment.filename}"

		if text = extract_pdf_text(pdf) and text.size > 300
	    	$stderr.puts "    Text size without OCR: #{text.size}"
		
		elsif text = extract_ocr_text(pdf)
	    	$stderr.puts "    Text size from OCR: #{text.size}"
	  	
	  	end

		if !(store_doc_dir.empty?)
			md5 = Digest::MD5.hexdigest(pdf)
			internal_filename = "#{metadata['event_id']}_#{metadata['state']}_#{md5}" + p.to_s + ".pdf"
			f = File.new("#{store_doc_dir}#{internal_filename}", "w")
			f.write(pdf.force_encoding("UTF-8"))
			f.close
		end

		if (text.size == 0)
	        
	        $stderr.puts "No text found in PDF attachment: #{pdf_attachment.filename}".blue
			
		elsif text.size > 0

			if (try_doc_parse)

				md5 = Digest::MD5.hexdigest(text)
		
				#Select parsers 					
				i = 1
				params = []
				form_type.each { params.push(i); i+=1; }

				begin

					uniqnum = (rand * 1000000).to_int.to_s	
					state = metadata["state"]
					parser_query = "SELECT * FROM " + schema.to_s + ".document_parsers WHERE state = '#{state}' AND parser_type IN ($" + params.join(',$').to_s + ")"
					
					conn.prepare('parser_select_' + uniqnum, parser_query)
					parsers = conn.exec_prepared('parser_select_' + uniqnum, form_type)
					parser_keys = parsers.fields
					parsers = parsers.values
							
				rescue PG::DuplicatePstatement => p_dup_statement_error
					#Deallocate insert
					conn.exec("DEALLOCATE parser_select_" + uniqnum); nil
					retry

				end

				if parsers.count == 0

					parser_name = "No applicable parsers found"
					$stderr.puts "#{parser_name}".blue

					data = [metadata["property_transaction_id"], metadata["message_id"], pdf_attachment.filename, text, parser_name] 
				    insert_update(conn: conn, operation: 'insert', schema: schema, table: 'offer_documents', data: data, update_where: "") 

				elsif parsers.count > 0

					parsers.each_with_index do |parser,d|

		        		parser_name = parser[parser_keys.index("parser_name")]
						update_where = "property_transaction_id = #{metadata["property_transaction_id"]} AND message_id = '#{metadata["message_id"]}' AND parser_name = '#{parser_name}' AND md5 = '#{md5}'"

						data = [metadata["property_transaction_id"], metadata["message_id"], pdf_attachment.filename, text, parser_name] 
						insert_update(conn: conn, operation: 'upsert', schema: schema, table: 'offer_documents', data: data, update_where: update_where)

						uniqnum = (rand * 1000000).to_int.to_s	
	 					query = "SELECT id FROM #{schema}.offer_documents WHERE " + update_where
	 					conn.prepare('select_' + uniqnum, query)
						doc_matches = conn.exec_prepared('select_' + uniqnum).values.flatten
						conn.exec("DEALLOCATE " + 'select_' + uniqnum); nil
						
						if doc_matches.count == 1
							
							doc_id = doc_matches.first
			        		$stderr.puts "Attempting to parse #{pdf_attachment.filename} using #{parser_name}".magenta
							
							if 	text.grep(Regexp.new parser[parser_keys.index("form_id")]).any? &&
								text.grep(Regexp.new parser[parser_keys.index("form_date")]).any? &&
								text.grep(Regexp.new parser[parser_keys.index("search_string")]).any?
							
								parsed_doc_data_out = parse_document(document_id: doc_id, parser_keys: parser_keys, parser: parser, pdf_attachment: pdf_attachment, request_parsed_reply: request_parsed_reply)

							else

				        		$stderr.puts "Attempt to parse #{pdf_attachment.filename} using #{parser_name} failed".blue
						    	parsed_doc_data_out = {"response_type" => 'Not Attempted',
														"response" => nil}

						    end

				        	update_where = "doc_id = #{doc_id}"
			    						    			
			    			if request_parsed_reply
			    				
			    				if (parsed_doc_data_out["response_type"] == 'Parsed Data')
			    					data = [doc_id, parsed_doc_data_out["response"]]
			    					insert_update(conn: conn, operation: 'upsert', schema: schema, table: 'offer_documents_parsing_payloads', data: data, update_where: update_where)
			    				
			    				elsif (parsed_doc_data_out["response_type"] == 'URI')
			    					data = [doc_id, nil]
			    					insert_update(conn: conn, operation: 'upsert', schema: schema, table: 'offer_documents_parsing_payloads', data: data, update_where: update_where)
			    				
			    				end
			    				
			    			elsif !request_parsed_reply && !(parsed_doc_data_out["response_type"] == 'Not Attempted')
			    				data = [doc_id, parsed_doc_data_out["response"]]
			    				insert_update(conn: conn, operation: 'upsert', schema: schema, table: 'offer_documents_parsing_payloads_queue', data: data, update_where: update_where) 		

			    			end

						elsif doc_matches.count > 1 
			        		raise StandardError.new("More than one record found for query: #{update_where}").
							return

			        	else
			        		raise StandardError.new("No records found for query: #{update_where}").
							return

			        	end
		        	end	
				end
			end
		end
	end
end

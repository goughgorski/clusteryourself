
qa_doc_parser_rules <- function(connection) {

  #Query to get data for Proposed Purchase Offers
	print(noquote("Processing purchase offer data"))
  	offer_messages_table_exists <- dbExistsTable(connection, c("datasci_mining","qa_rejects_offer_messages"))
  	offer_documents_table_exists <- dbExistsTable(connection, c("datasci_mining","qa_rejects_offer_documents"))
  	target_table_exists <- dbExistsTable(connection, c("datasci_mining","qa_doc_parser_rules"))

  	if (!offer_messages_table_exists) {
    	print(noquote("No offer messages table in Data Science found."))
    	count <- 0
  		}

  	if (!offer_documents_table_exists) {
    	print(noquote("No offer documents table in Data Science found."))
    	count <- 0
  		}

  	if (!target_table_exists) {
    	print(noquote("No target table found."))
    	count <- 0
  		}

refactor{
	if (offer_messages_table_exists & offer_documents_table_exists & target_table_exists){
	    count <- dbGetQuery(connection,"SELECT COUNT(*)
	                                          FROM datasci_mining.qa_rejects_offer_documents
	                                          WHERE CHAR_LENGTH(parsed_document_data)::decimal > 3000")
	    print(noquote(paste(count, "records found in parsed purchase offers table")))  
	  	}

  	#Get max id from parsed purchase offers
  	if (count > 0){
    	max_id <- dbGetQuery(connection, "SELECT MAX(om_id) 
                                          FROM datasci_mining.parsed_purchase_offers")
    	} else {max_id <- 0 }
}
	# I modified the search query here
  	data <- dbGetQuery(connection,paste("SELECT * FROM datasci_mining.qa_rejects_offer_documents
                                       WHERE parser_name is not null
                                       AND parsed_document_data is not null
                                       AND CHAR_LENGTH(parsed_document_data)::decimal > 3000
                                       AND created_at > '4-16-2018'"))
refactor{
	#Check if any new data
	if (nrow(data)==0) {
        stop(print('No new purchase offer data found.')) 
	 } else {print(noquote(paste(nrow(data), "new records found to be processed.")))}
}
	## insert here split by parser (CA, TX, etc.)

  # data <- lapply(split(data, data[, 'parser_name']), function(data) {})

	#Structure Parsed Data fields:
  # converting hash to JSON
	parsed_data <- gsub('=>', ':', data$parsed_document_data) 
	parsed_data <- gsub(':nil', ':""', parsed_data)	
  	parsed_data <- gsub('\\\\[#$]', '', parsed_data) 
    parsed_data <- apply(as.data.frame(parsed_data), 1, FUN = fromJSON)

    parsed_data <- lapply(parsed_data, function(x){
      i <- sapply(x, is.data.frame)
      x[i] <- lapply(x[i], function(y){return(as.character(y[1]))})
      return(x)})
    parsed_data <- lapply(parsed_data, function(x) { x <- x[,!colnames(x) %in% c('zipcode')]}) # why remove zipcode?





    parsed_data <- ldply(parsed_data, rbind)




    char_length <- sapply(parsed_data, FUN = nchar, simplify = TRUE)
    char_length <- ldply(char_length, rbind.fill)
    
    char_length_ave <- apply(char_length, 2, )
















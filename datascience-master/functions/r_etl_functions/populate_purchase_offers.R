
populate_purchase_offers <- function(connection) {

	#Query to get data for Proposed Purchase Offers
	print(noquote("Processing purchase offer data"))
  	offer_table_exists <- dbExistsTable(connection, c("datasci","offer_messages"))

  	if (!offer_table_exists) {
    	print(noquote("No offer messages table in Data Science found."))
    	count <- 0
  		}

	if (offer_table_exists){
	    count <- dbGetQuery(connection,"SELECT COUNT(*)
	                                          FROM datasci_projects.vw_offer_extraction")
	    print(noquote(paste(count, "records found in offer extraction view")))  
	  	}

  	#Get max(message_id) from offer messages
  	if (count > 0){
    	max_id <- dbGetQuery(connection, "SELECT MAX(id) 
                                          FROM datasci_projects.purchase_offers")
    	} else {max_id <- 0 }

	data_purchase_offers <- dbGetQuery(connection,paste("SELECT * FROM datasci_projects.vw_offer_extraction
	                                       WHERE id >", max_id))
	
	#Check if any new data
	if (nrow(data_purchase_offers)==0) {
        stop(print('No new purchase offer data found.')) 
	 }

	print(noquote(paste(nrow(data_purchase_offers), "new records found to be processed.")))  

	#Clean Docs
	print(noquote("Cleaning OCR debris, and multiple newlines"))
	data_purchase_offers$document_content_clean <- gsub("__|--", "", data_purchase_offers$document_content)
	data_purchase_offers$document_content_clean <- gsub("\n+", "\n", data_purchase_offers$document_content_clean)

	print(noquote("Extracting form offer text segments"))
	purchase_order_data <- regexpr("(?Uis)(?=california residential purchase agreement[[:space:]+]\\(rpa-ca page 1 of 10).*(?<=Property Address)", data_purchase_offers$document_content_clean, perl=TRUE)
	purchase_order_data_sub <- substr(data_purchase_offers$document_content_clean,purchase_order_data,purchase_order_data + attr(purchase_order_data,"match.length"))

	print(noquote("Splitting form offer text"))
	doc_split <- strsplit(purchase_order_data_sub,'\n', perl = TRUE)
	doc_split2 <- lapply(doc_split, function(x) {x<-trimws(gsub('\\s+', ' ', x, perl = TRUE))})
	doc_split3 <- lapply(doc_split2, function(x) {x<-x[x!="" & x!=" " & x!="-"]})
	doc_split4 <- lapply(doc_split3, function(x) {t(as.data.frame(x))})
	doc_split5 <- ldply(doc_split4, rbind.fill)
	doc_split5$id <- data_purchase_offers$id

	print(noquote("Checking record matching rules"))

	match <- apply(doc_split5,1,function(x){sum(!is.na(x))>1})
	
	print(noquote(paste("Able to identify relevant segments in "
						,round((sum(match)/length(match))*100)
						,"% of records", sep = '')))
	
	if (round((sum(match)/length(match))*100)==0) {
        stop(print('No new purchase offer data found.')) 
	 }
	
	offer_date_match <- grepl('^(\\d\\d)/(\\d\\d)/(\\d\\d)',doc_split5[,4],perl = TRUE)

	print(noquote(paste("Able to identify offer date in "
						,round(((sum(offer_date_match)/length(offer_date_match))*100))
						,"% of records, and ", round((sum(offer_date_match)/sum(match))*100), "% of matched records", sep = '')))

	offer_buyer_match <- grepl('^[[:alpha:][:punct:]]+',doc_split5[,5],perl = TRUE)
	
	print(noquote(paste("Able to identify buyer in "
						,round(((sum(offer_buyer_match)/length(offer_buyer_match))*100))
						,"% of records, and ", round((sum(offer_buyer_match)/sum(match))*100), "% of matched records", sep = '')))

	offer_price_match <- grepl('^(\\d+[,.])',doc_split5[,9],perl = TRUE)
	
	print(noquote(paste("Able to identify price in "
						,round(((sum(offer_price_match)/length(offer_price_match))*100))
						,"% of records, and ", round((sum(offer_price_match)/sum(match))*100), "% of matched records", sep = '')))

	offer_escrow_match <- grepl('^X [[:alnum:][:punct:]]+',doc_split5[,10],perl = TRUE)

	print(noquote(paste("Able to identify escrow time in "
						,round(((sum(offer_escrow_match)/length(offer_escrow_match))*100))
						,"% of records, and ", round((sum(offer_escrow_match)/sum(match))*100), "% of matched records", sep = '')))

	offer_listing_agent <- grepl('[[:alnum:][:punct:]]{2,}',doc_split5[,12],perl = TRUE)

	print(noquote(paste("Able to identify listing agent in "
						,round(((sum(offer_listing_agent)/length(offer_listing_agent))*100))
						,"% of records, and ", round((sum(offer_listing_agent)/sum(match))*100), "% of matched records", sep = '')))

	offer_buying_agent <- grepl('[[:alnum:][:punct:]]{2,}',doc_split5[,14],perl = TRUE)

	print(noquote(paste("Able to identify buying agent in "
						,round(((sum(offer_buying_agent)/length(offer_buying_agent))*100))
						,"% of records, and ", round((sum(offer_buying_agent)/sum(match))*100), "% of matched records", sep = '')))

	valid_offer <- match & offer_date_match & offer_buyer_match & offer_price_match & offer_escrow_match & offer_listing_agent & offer_buying_agent

	print(noquote(paste("Able to identify all above elements in "
						,round(((sum(valid_offer)/length(valid_offer))*100))
						,"% of records, and ", round((sum(valid_offer)/sum(match))*100), "% of matched records", sep = '')))

	offer_data <- cbind(doc_split5$id[valid_offer], doc_split5[valid_offer,c(4,5,9,10,12,14)])
	colnames(offer_data) <- c('id', 'offer_date', 'offer_buyer', 'offer_price', 'offer_escrow_time', 'offer_listing_agent', 'offer_buying_agent')

	offer_data_uniq <- offer_data[!duplicated(offer_data[,c('offer_date', 'offer_price')]),]

	#Write Tables
  	#purchase_offers
  	offer_table_exists <- dbExistsTable(connection, c("datasci_projects","purchase_offers"))

  	if (offer_table_exists) { 
    	append = TRUE
    	overwrite = FALSE
    	colnames <- dbGetQuery(connection, "SELECT column_name 
                           FROM information_schema.columns 
                           WHERE table_name = 'purchase_offers'
                            AND column_default IS NULL")

    if (!setequal(colnames(offer_data_uniq),colnames[,1]) & max_id == 0) { 
      	overwrite = TRUE
      	append = FALSE
    	}
    
    if (!setequal(colnames(offer_data_uniq),colnames[,1]) & max_id > 0) { 
      	stop(print('Processed table structure has changed but existing table contains data. Drop table and rerun, or adjust processing to fit existing structure.'))	
    	}
    } else {
      append = FALSE
      overwrite = FALSE
  	}


	dbWriteTable(connection, c('datasci_projects','purchase_offers'), value = offer_data_uniq, overwrite = overwrite, append = append, row.names = FALSE)

	}

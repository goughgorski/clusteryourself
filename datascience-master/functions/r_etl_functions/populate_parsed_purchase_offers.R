
populate_parsed_purchase_offers <- function(connection) {

  #Query to get data for Proposed Purchase Offers
	print(noquote("Processing purchase offer data"))
  	offer_messages_table_exists <- dbExistsTable(connection, c("datasci_mining","offer_messages"))
  	offer_documents_table_exists <- dbExistsTable(connection, c("datasci_mining","offer_documents_parsing_payloads"))
  	target_table_exists <- dbExistsTable(connection, c("datasci_mining","parsed_purchase_offers"))

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

	if (offer_messages_table_exists & offer_documents_table_exists & target_table_exists){
	    count <- dbGetQuery(connection,"SELECT COUNT(*)
	                                          FROM datasci_mining.parsed_purchase_offers")
	    print(noquote(paste(count, "records found in parsed purchase offers table")))  
	  	}

  	# construct query to pull data
  	if (count > 0){
    	query <- "SELECT * from datasci_mining.offer_documents_parsing_payloads dsmodpp
                LEFT JOIN datasci_mining.parsed_purchase_offers dsmppo
                  on dsmodpp.doc_id = dsmppo.doc_id
                WHERE 1=1 
                AND parsed_document_data is not null
                AND (dsmppo.id IS NULL OR date_trunc('day', dsmodpp.updated_at) > date_trunc('day', dsmppo.uploaded_at::timestamp))"

    	} else {query <-  "SELECT * from datasci_mining.offer_documents_parsing_payloads
                         WHERE 1=1
                         AND parsed_document_data is not null"}

	# pull existing data from offer_documents
  data <- dbGetQuery(connection, query)

	#Check if any new data
	if (nrow(data) < 2) {
        stop(print('No new purchase offer data found.')) 
	 } else {print(noquote(paste(nrow(data), "new records found to be processed.")))}

	#Structure Parsed Data fields:
  # converting hash to JSON
  
  ### START modularize JSON conversion ###
	parsed_data <- gsub('=>', ':', data$parsed_document_data) 
	parsed_data <- gsub(':nil', ':""', parsed_data)	
  parsed_data <- gsub('\\\\[#$]', '', parsed_data) 
  parsed_data <- split(parsed_data, data[,'id'])
  
  parsed_data <- lapply(parsed_data, FUN = fromJSON)
  
  # removing multiple values from field-level output (keeping only the first)
  parsed_data <- lapply(parsed_data, function(x){
    i <- sapply(x, is.data.frame)
    x[i] <- lapply(x[i], function(y){return(as.character(y[1]))})
    return(x)})
  
  #drop old columns here from lk table?
  parsed_data <- lapply(parsed_data, function(x) { x <- x[,!colnames(x) %in% c('zipcode')]})
  
  parsed_data <- ldply(parsed_data, rbind.fill)
  ### END modularize JSON conversion ###
	
  colnames(parsed_data)[colnames(parsed_data) == '.id'] <- 'doc_id'

  # trim whitespace
  parsed_data <- apply(parsed_data, 2, FUN = trimws)
  # need established corpus of find/replace
  parsed_data <- apply(parsed_data, 2, function(x) { gsub('\\s+', ' ', x, perl = TRUE)})
  parsed_data <- apply(parsed_data, 2, function(x) { gsub(' ,', ',', x, perl = TRUE)})
  parsed_data <- apply(parsed_data, 2, function(x) { gsub(' _ ', ' ', x, perl = TRUE)})
  parsed_data <- apply(parsed_data, 2, function(x) { gsub(',$', '', x, perl = TRUE)})
  parsed_data <- apply(parsed_data, 2, function(x) { gsub('^\\.$', '', x, perl = TRUE)})
  parsed_data <- apply(parsed_data, 2, function(x) { gsub('^, ', '', x, perl = TRUE)})
  parsed_data <- apply(parsed_data, 2, function(x) { gsub(' \\.', '.', x, perl = TRUE)})
  parsed_data <- apply(parsed_data, 2, function(x) { gsub('\\$ ', '\\$', x, perl = TRUE)})
  parsed_data <- apply(parsed_data, 2, function(x) { gsub('^[[:space:]]$', '', x, perl = TRUE)})
  parsed_data <- apply(parsed_data, 2, function(x) { gsub("[^\001-\177]",'', x, perl = TRUE)})

  parsed_data <- as.data.frame(parsed_data)

	#Convert Factors to Characters
	i <- sapply(parsed_data,is.factor)
	parsed_data[i] <- lapply(parsed_data[i], as.character)
	i <- sapply(parsed_data,function(x){x==""})
	parsed_data[i] <- NA

  # import config table
  config <- dbGetQuery(connection, paste("SELECT * from datasci_mining.parsed_purchase_offers_config"))

  # number column standardization
  num_cols <- config$input_col[config$type == 'number'][config$input_col[config$type == 'number'] %in% colnames(parsed_data)]
  parsed_data[, num_cols] <- apply(parsed_data[, num_cols], 2, function(x) {
    x <- gsub("[^0-9\\.]", "", x, perl = TRUE) # removes everything other than numerics and '.'
    x <- gsub("^\\.+", "", x, perl = TRUE) # removes leading '.'
    x <- ifelse(x == "", NA, x)
    return(x)})

    # date column standardization
  date_cols <- config$input_col[config$type == 'date'][config$input_col[config$type == 'date'] %in% colnames(parsed_data)]
  parsed_data[, date_cols] <- apply(parsed_data[, date_cols], 2, function(x) {
    x <- as.data.frame(x)
    colnames(x) <- 'date'
    x$date <- as.character(x$date)
    
    pattern <- '\\d{1,2}/\\d{1,2}/\\d{2,4}'
    x$pos <- regexpr(pattern, x$date)
    x$length <- attr(regexpr(pattern, x$date), 'match.length')
    go<-x$pos!=-1 & !is.na(x$pos) 
    x$date[go] <- substr(x$date[go], x$pos[go], x$pos[go] + x$length[go])
    x$date[go] <- gsub('\\"', '', x$date[go], perl = TRUE)
    x$date[go] <- as.character(mdy(x$date[go]))
    
    pattern <- '\\d\\d\\d\\d-\\d\\d-\\d\\d'
    x$pos <- regexpr(pattern, x$date)
    x$length <- attr(regexpr(pattern, x$date), 'match.length')
    go<-x$pos!=-1 & !is.na(x$pos) 
    x$date[go] <- substr(x$date[go], x$pos[go], x$pos[go] + x$length[go])
    x$date[go] <- gsub('\\"', '', x$date[go], perl = TRUE)
    x$date[go] <- as.character(ymd(x$date[go]))
    x$order <- 1:nrow(x)
  
    tmp <- x[x$pos==-1 & !is.na(x$pos),]
    tmp$date <- suppressWarnings(ifelse(!is.na(mdy(tmp$date)), as.character(mdy(tmp$date)), as.character(dmy(tmp$date))))
    x <- rbind(tmp, x[!x$order %in% tmp$order,])
    
    return(x$date[order(x$order)])})

    # binary column standardization function
    binary_clean <- function(data, bi_col_1, bi_col_2, bi_tag_1, bi_tag_2){

    cols <- c(bi_col_1, bi_col_2)

    tmp <- data[, cols[!is.na(cols)]]
    tmp <- as.data.frame(tmp)
    tmp <- as.data.frame(apply(tmp, 2, function(x){
      x <- gsub("[^Xx]", "", x)
      x <- gsub("[Xx]", "x", x)
      x[x!='x'] <- NA
      return(x)}))

    if(is.na(bi_col_2)){
      tmp$name <- ifelse(!is.na(tmp[,1]), bi_tag_1, NA)} else
      {tmp$name <- ifelse(!is.na(tmp[,bi_col_1]) & is.na(tmp[,bi_col_2]), bi_tag_1,
                          ifelse(!is.na(tmp[,bi_col_2]) & is.na(tmp[,bi_col_1]), bi_tag_2, NA))}

    return(tmp$name)}

    # loop function over existing binaries
    bi_cols <- unique(config$output_col[config$type == 'binary'])[unique(config$output_col[config$type == 'binary']) %in% config$output_col[config$type == 'binary'][config$input_col[config$type == 'binary'] %in% colnames(parsed_data)]]

    for(i in 1:length(bi_cols)){
      parsed_data[, bi_cols[i]] <- 
      binary_clean(parsed_data,
        unique(config$input_col[config$output_col == bi_cols[i]])[1],
        unique(config$input_col[config$output_col == bi_cols[i]])[2],
        unique(config$tag[config$output_col == bi_cols[i]])[1],
        unique(config$tag[config$output_col == bi_cols[i]])[2])
                                                                                       
    if(!is.na(unique(config$input_col[config$output_col == bi_cols[i]])[2])){
      include_col <- !(colnames(parsed_data) %in% c(unique(config$input_col[config$output_col == bi_cols[i]])[1],
                                                  unique(config$input_col[config$output_col == bi_cols[i]])[2]))
      parsed_data <- parsed_data[,include_col]}}
  


	#Columnar ETL 
	
    #Select Columns
    include_col <- !(colnames(parsed_data) %in% c('field_32', 'rep_hash_2', 'rep_hash_1'))
    parsed_purchase_offers <- parsed_data[,include_col]
    colnames(parsed_purchase_offers)[which(colnames(parsed_purchase_offers) == 'closing_date_acceptance_relatve')] <- 'closing_date_acceptance_relative'

    #Check for whole-document parsing failures
    check <- apply(parsed_purchase_offers[,13:ncol(parsed_purchase_offers)],1,function(x) { all(is.na(x))})
    parsed_purchase_offers <- parsed_purchase_offers[!check,]

  # write unique results to parsed_purchase_offers table
 	dbWriteTable(connection, c('datasci_mining','parsed_purchase_offers'), value = parsed_purchase_offers, overwrite = FALSE, append = TRUE, row.names = FALSE)
}
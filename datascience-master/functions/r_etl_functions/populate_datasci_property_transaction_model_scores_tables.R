populate_datasci_property_transaction_model_scores_tables <- function(connection) {
	
	#Check to see if Tables Exist
	required_tables <- c('property_transaction_events','property_transaction_not_created_events', 'quarantined_property_transaction_not_created_events', 'quarantined_property_transaction_events', 'property_transaction_model_scores')
	for (i in 1:length(required_tables)) {
		table_check <- dbExistsTable(connection, c("datasci",required_tables[i]))
		if (!table_check) {stop(print(paste(required_tables[i], " table is missing")))}
		}

	#Scores Below Threshold
	#Query to get new data for property_transaction
	print(noquote("Getting new data for scored transactions from data science tables."))
	data <- dbGetQuery(connection, "SELECT * FROM datasci.vw_property_transaction_model_scores_queue")

	#Check if any new data
	if (nrow(data)==0) {
	    print('No new event data found.')
	} else {

	print(noquote(paste(nrow(data), "new records found to be processed.")))  

	print(table(data$type))

  	count <- dbGetQuery(connection,"SELECT COUNT(*)
                                          FROM datasci.property_transaction_model_scores")
  	print(noquote(paste(count, "records found in property transactions model scores table.")))

	#Dump columns with no data
	data <- dump_blank_columns(data)

	#Convert Factors to Characters
	i <- sapply(data,is.factor)
	data[i] <- lapply(data[i], as.character)
	    
	#Extract fields from event_data
	print(noquote("Performing model scoring ETL on event data"))

	#Model Scores
	#model_ids
	model_data <- gregexpr('model_id: [[:digit:]]+',data$event_data)
	model_ids <- vector("list", length(model_data))
	for (i in 1:length(model_data)) {
		for (j in 1:length(model_data[[i]])) {
		  model_ids[[i]][j] <- substr(data$event_data[i], model_data[[i]][j], model_data[[i]][j] + attr(model_data[[i]],"match.length")[j])
		}
	}
	model_ids <- lapply(model_ids, function(x) {gsub('model_id: |\n', '', x)})
	model_ids <- lapply(model_ids, cbind)

	#model_scores
	model_data <- gregexpr('model_score: [[:digit:][:punct:]]+',data$event_data)
	model_scores <- vector("list", length(model_data))
	for (i in 1:length(model_data)) {
		for (j in 1:length(model_data[[i]])) {
		  model_scores[[i]][j] <- substr(data$event_data[i], model_data[[i]][j], model_data[[i]][j] + attr(model_data[[i]],"match.length")[j])
		}
	}
	model_scores <- lapply(model_scores, function(x) {gsub('model_score: |\n', '', x)})
	model_scores <- lapply(model_scores, cbind)

	#model_thresholds
	model_data <- gregexpr('model_threshold: [[:digit:][:punct:]]+',data$event_data)
	model_thresholds <- vector("list", length(model_data))
	for (i in 1:length(model_data)) {
		for (j in 1:length(model_data[[i]])) {
		  model_thresholds[[i]][j] <- substr(data$event_data[i], model_data[[i]][j], model_data[[i]][j] + attr(model_data[[i]],"match.length")[j])
		}
	}
	model_thresholds <- lapply(model_thresholds, function(x) {gsub('model_threshold: |\n', '', x)})
	model_thresholds <- lapply(model_thresholds, cbind)

	#model_votes
	model_data <- gregexpr('model_vote: [[:alpha:]]+',data$event_data)
	model_votes <- vector("list", length(model_data))
	for (i in 1:length(model_data)) {
		for (j in 1:length(model_data[[i]])) {
		  model_votes[[i]][j] <- substr(data$event_data[i], model_data[[i]][j], model_data[[i]][j] + attr(model_data[[i]],"match.length")[j])
		}
	}
	model_votes <- lapply(model_votes, function(x) {gsub('model_vote: |\n', '', x)})
	model_votes <- lapply(model_votes, cbind)

	model_score_cols <- unlist(dbGetQuery(connection, "SELECT column_name 
	               FROM information_schema.columns 
	               WHERE table_name = 'property_transaction_model_scores'
	               and table_schema = 'datasci'
	               AND column_default IS NULL")) 
	ids <- factor(data$event_id, levels=unique(data$event_id))
	model_data <- lapply(seq_along(model_ids),function(x) {cbind.data.frame(event_id = as.numeric(as.character(rep(ids[x],dim(model_ids[[x]])[1]))),model_id = as.numeric(gsub("^\\s+|\\s+$", "", model_ids[[x]])), score = as.numeric(gsub("^\\s+|\\s+$", "", model_scores[[x]])), threshold = as.numeric(gsub("^\\s+|\\s+$", "", model_thresholds[[x]])), vote = as.logical(gsub("^\\s+|\\s+$", "", model_votes[[x]])))})
	model_data <- ldply(model_data, rbind)

  	print(noquote(paste("Writing", nrow(model_data), "records to property transactions model scores table.")))
	#Write Model Scores Table
	dbWriteTable(connection, c('datasci','property_transaction_model_scores'), value = model_data, overwrite = FALSE, append = TRUE, row.names = FALSE)

	}

}



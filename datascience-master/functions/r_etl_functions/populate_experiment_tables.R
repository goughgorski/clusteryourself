populate_experiment_tables <- function(connection) {

	#Check for necessary tables
	exp_table_exists <- dbExistsTable(connection, c("datasci_modeling", "experiments"))
	lk_user_exp_table_exists <- dbExistsTable(connection, c("datasci_modeling", "lk_user_experiments"))

	tables_exist <- c(exp_table_exists, lk_user_exp_table_exists)
	required_tables <- c('exp_table_exists', 'lk_user_exp_table_exists')
	if(any(tables_exist == FALSE)) {
		stop(print(paste(required_tables[tables_exist == FALSE], " table is missing")))
	}

	#Query to get new data from events table
	print(noquote("Getting new data from production events table"))
	if(dbGetQuery(connection, "SELECT COUNT(*) FROM datasci_modeling.lk_user_experiments")$count == 0) {
		data <- dbGetQuery(connection, "SELECT *
									FROM public.events
									WHERE type = 'UserSegmentationEvent'")
		} else {
		data <- dbGetQuery(connection, "SELECT *
										FROM public.events
										WHERE type = 'UserSegmentationEvent'
										AND created_at > (SELECT MAX(created_at)
																FROM datasci_modeling.lk_user_experiments)")
	}

	#Check if any new data
	if (nrow(data)==0) {
	      print('No new event data found.')
	} else {
	  
	print(noquote(paste(nrow(data), "new records found to be processed.")))  

	print(table(data$type))
	print(noquote(paste("Includes user segmentation events from ", min(data$created_at), "to ", max(data$created_at))))

	#Dump columns with no data
	data <- dump_blank_columns(data)

	#Convert Factors to Characters
	i <- sapply(data,is.factor)
	data[i] <- lapply(data[i], as.character)
	    
	lk_user_experiments <- dbGetQuery(connection,"SELECT * FROM datasci_modeling.lk_user_experiments")
	print(noquote(paste(nrow(lk_user_experiments), "records found in user experiments lookup table.")))

	#Extract fields from event_data
	print(noquote("Performing ETL on event data"))  

  	#external_account_id
  	data$external_account_id <- regexpr('token_id: [[:digit:]]+',data$event_data)
  	data$external_account_id <- substr(data$event_data,data$external_account_id,data$external_account_id + attr(data$external_account_id,"match.length"))
  	data$external_account_id <- gsub('token_id: |\n', '', data$external_account_id)
  	data$external_account_id <- ifelse(data$external_account_id == '', NA, data$external_account_id)

  	data$experiment_name <- regexpr('experiment_name: [[:print:]]+',data$event_data)
  	data$experiment_name <- substr(data$event_data,data$experiment_name,data$experiment_name + attr(data$experiment_name,"match.length"))
  	data$experiment_name <- gsub('experiment_name: |\n', '', data$experiment_name)
  	data$experiment_name <- gsub(':', '', data$experiment_name) #Here to fix typo in event_data
  	data$experiment_name <- ifelse(data$experiment_name == '', NA, data$experiment_name)

  	data$experiment_group <- regexpr('experiment_group: [[:print:]]+',data$event_data)
  	data$experiment_group <- substr(data$event_data,data$experiment_group,data$experiment_group + attr(data$experiment_group,"match.length"))
  	data$experiment_group <- gsub('experiment_group: |\n', '', data$experiment_group)
  	data$experiment_group <- ifelse(data$experiment_group == '', NA, data$experiment_group)

  	event_cols <- unlist(dbGetQuery(connection, "SELECT column_name 
                         FROM information_schema.columns 
                         WHERE table_name = 'lk_user_experiments'
                         and table_schema = 'datasci_modeling'
                         AND column_default IS NULL"))

	colnames(data)[colnames(data)=='id'] <- 'event_id'

	#Factor to Character
	i <- sapply(data,is.factor)
	data[i] <- lapply(data[i], as.character)
	  
	#Clean white spaces in character vectors
	i <- sapply(data,is.character)
	data[i] <- lapply(data[i], function(x){gsub("^\\s+|\\s+$", "", x)})
	data[i] <- lapply(data[i], function(x){gsub("\\s+", " ", x)})

	#Clean duplicate scans by user
  	print(noquote("Cleaning Duplicate and Unmatched Scans"))

  	#Keep most recent scan by user, experiment_name
  	data <- data[with(data, order(data$external_account_id, data$experiment_name, data$created_at, decreasing = TRUE)),]
  	data <- data[!duplicated(data$external_account_id), ]

  	#Remove scans with no matching experiment_name from experiments table
  	experiments <- dbGetQuery(connection, "SELECT * from datasci_modeling.experiments")
  	data <- merge(data, experiments, by = 'experiment_name')
 	
  	#Remove scans after experiment end date
  	data <- data[is.na(data$end_date) | data$created_at < data$end_date,]

  	#Remove users not in datasci.users
  	users <- dbGetQuery(connection, "SELECT external_account_id, acting_user_id FROM datasci.users")
  	data <- merge(data, users, by = c('external_account_id', 'acting_user_id'))

  	colnames(data)[colnames(data)=='id'] <- 'experiment_id'

  	user_experiments <- data[, event_cols]

  	#Check for existing acting_user_id, experiment_id combinations
  	if(dbGetQuery(connection, "SELECT COUNT(*) FROM datasci_modeling.lk_user_experiments")$count > 0) {
		user_experiments_update <- merge(user_experiments, lk_user_experiments[, c('acting_user_id', 'experiment_id')])
	}

  	#Update lk_user_experiments table
  	if(!is.null(user_experiments_update) && nrow(user_experiments_update) > 0) {
  		for(i in 1:nrow(user_experiments_update)) {
  			dbGetQuery(connection, paste(paste("UPDATE datasci_modeling.lk_user_experiments
  										SET created_at = ", user_experiments_update$created_at[i], " ", sep = "'"),
  										paste("WHERE acting_user_id = ", user_experiments_update$acting_user_id[i],
  										" AND experiment_id = ", user_experiments_update$experiment_id[i], ";", sep = "")))
  		}
  	}

  	#Separate new lk_user_experiments rows
  	if(!is.null(user_experiments_update) && nrow(user_experiments_update) > 0) {
		user_experiments_new <- anti_join(user_experiments, lk_user_experiments[, c('acting_user_id', 'experiment_id')], by = c('acting_user_id', 'experiment_id'))  	
		} else {user_experiments_new <- user_experiments}

  	#Write User Experiment Data
  	if(nrow(user_experiments_new) > 0) {
    	dbWriteTable(connection, c('datasci_modeling','lk_user_experiments'), value = user_experiments_new, overwrite = FALSE, append = TRUE, row.names = FALSE)
    }
	}
}
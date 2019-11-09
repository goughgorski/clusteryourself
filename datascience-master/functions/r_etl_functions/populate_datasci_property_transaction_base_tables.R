populate_datasci_property_transaction_base_tables <- function(connection) {
    
  #Check to see if Tables Exist
  required_tables <- c('property_transaction_events', 'property_transactions', 'properties', 'quarantined_property_transaction_events')
  for (i in 1:length(required_tables)) {
      table_check <- dbExistsTable(connection, c("datasci",required_tables[i]))
      if (!table_check) {stop(print(paste(required_tables[i], " table is missing")))}
  }

  #Query to get new data for property_transaction
  print(noquote("Getting new data from production events table"))
  data <- dbGetQuery(connection, "SELECT * FROM datasci.vw_property_transaction_events_queue")

  #Check if any new data
  if (nrow(data)==0) {
        print('No new event data found.')
  } else {

  print(noquote(paste(nrow(data), "new records found to be processed.")))  
  print(table(data$type))
  print(noquote(paste("Includes property transaction events from ", min(data$created_at), "to ", max(data$created_at))))
    
  #Dump columns with no data
  data <- dump_blank_columns(data)

  #Convert Factors to Characters
  i <- sapply(data,is.factor)
  data[i] <- lapply(data[i], as.character)
    
  count <- dbGetQuery(connection,"SELECT COUNT(*)
                                          FROM datasci.property_transaction_events")
  print(noquote(paste(count, "records found in property transactions events table.")))

  #Extract fields from event_data
  print(noquote("Performing ETL on event data"))
    
  #Use to check for new information in event data
  #event_data <- strsplit(data$event_data,'\n')
  
  #property_transaction_id
  data$property_transaction_id <- regexpr('property_transaction_id: [[:digit:]]+',data$event_data)
  data$property_transaction_id <- substr(data$event_data,data$property_transaction_id,data$property_transaction_id + attr(data$property_transaction_id,"match.length"))
  data$property_transaction_id <- gsub('property_transaction_id: |\n', '', data$property_transaction_id)
  
  #address
  data$address <- regexpr('address: [^\n]+',data$event_data)
  data$address <- substr(data$event_data,data$address,data$address + attr(data$address,"match.length"))
  data$address <- gsub('address: |\n', '', data$address)
  
  #street_address
  data$street_addr <- regexpr('street_addr[_standard]*: [^,\n]+',data$event_data)
  data$street_addr <- substr(data$event_data,data$street_addr,data$street_addr + attr(data$street_addr,"match.length"))
  data$street_addr <- gsub('street_addr[_standard]*: |\n', '', data$street_addr)
  data$street_addr <- gsub("^'+|'+$", "",  data$street_addr)  
  
  #Parse address
  parse_address <- strsplit(data$address, split = ',')
  street_address <- ldply(lapply(parse_address, function(x) {x[1]}),rbind)
  street_address <- as.character(street_address[,1])
  city <- ldply(lapply(parse_address, function(x) {x[length(x)-1]}),rbind)
  city <- trimws(as.character(city[,1]))
  state_zip <- ldply(as.character(lapply(parse_address, function(x) {x[length(x)]}),rbind))
  state <- substr(state_zip[,1], 2, 3)
  zipcode <- substr(state_zip[,1], 5, 9)
  data <- cbind(data, city, state, zipcode)
  print(noquote(paste(sum(data$street_addr==''), "records with missing street address information.")))
  data$street_addr[data$street_addr==''] <- street_address[data$street_addr=='']

  #external_account_id
  data$external_account_id <- regexpr('gmail_token_id: [[:digit:]]+|external_account_id: [[:digit:]]+',data$event_data)
  data$external_account_id <- substr(data$event_data,data$external_account_id,data$external_account_id + attr(data$external_account_id,"match.length"))
  data$external_account_id <- gsub('gmail_token_id: |external_account_id: |\n', '', data$external_account_id)
  
  #originating_message_id
  data$originating_message_id <- regexpr('originating_message_id: [[:alnum:]]+',data$event_data)
  data$originating_message_id <- substr(data$event_data,data$originating_message_id,data$originating_message_id + attr(data$originating_message_id,"match.length"))
  data$originating_message_id <- gsub('originating_message_id: |\n', '', data$originating_message_id)
  data$originating_message_id <- ifelse(data$originating_message_id == '', NA, data$originating_message_id)
  
  #ensemble_name
  data$ensemble_name <- regexpr('ensemble: [[:digit:][:alpha:]]+',data$event_data)
  data$ensemble_name <- substr(data$event_data,data$ensemble_name,data$ensemble_name + attr(data$ensemble_name,"match.length"))
  data$ensemble_name <- gsub('ensemble: |\n', '', data$ensemble_name)
  data$ensemble_name <- ifelse(data$ensemble_name == '', NA, data$ensemble_name)

  #ensemble_score
  data$ensemble_score  <- regexpr('ensemble_score: [[:digit:][:punct:]]+',data$event_data)
  data$ensemble_score <- substr(data$event_data,data$ensemble_score,data$ensemble_score + attr(data$ensemble_score,"match.length"))
  data$ensemble_score <- gsub('ensemble_score: |\n', '', data$ensemble_score)
  data$ensemble_score <- as.numeric(data$ensemble_score)  

  #ensemble_boolean
  data$ensemble_boolean  <- regexpr('boolean: [[:alpha:]]+',data$event_data)
  data$ensemble_boolean <- substr(data$event_data,data$ensemble_boolean,data$ensemble_boolean + attr(data$ensemble_boolean,"match.length"))
  data$ensemble_boolean <- gsub('boolean: |\n', '', data$ensemble_boolean)
  data$ensemble_boolean <- ifelse(data$ensemble_boolean == '', NA, data$ensemble_boolean)
  
  #deletion_method
  data$deletion_method <- regexpr('deletion_method: [[:alpha:]_]+',data$event_data)
  data$deletion_method <- substr(data$event_data,data$deletion_method,data$deletion_method + attr(data$deletion_method,"match.length"))
  data$deletion_method <- gsub('deletion_method: |\n', '', data$deletion_method)
  
  #rejection_reason
  data$rejection_reason <- regexpr('rejection_reason: [-[:alpha:]]+',data$event_data)
  data$rejection_reason <- substr(data$event_data,data$rejection_reason,data$rejection_reason + attr(data$rejection_reason,"match.length"))
  data$rejection_reason <- gsub('rejection_reason: |\n', '', data$rejection_reason)
  
  #rejection_notes
  data$rejection_notes <- regexpr('rejection_notes: [[:print:]]+',data$event_data)
  data$rejection_notes <- substr(data$event_data,data$rejection_notes,data$rejection_notes + attr(data$rejection_notes,"match.length"))
  data$rejection_notes <- gsub('rejection_notes: |\n', '', data$rejection_notes)
  
  #scan_type
  data$scan_type <- regexpr('scan_type: [[:print:]]+',data$event_data)
  data$scan_type <- substr(data$event_data,data$scan_type,data$scan_type + attr(data$scan_type,"match.length"))
  data$scan_type <- gsub('scan_type: |\n', '', data$scan_type)
  data$scan_type <- ifelse(data$scan_type == '', NA, data$scan_type)

  #tx timeline id
  data$transaction_timeline_id <- regexpr('transaction_timeline_id: [[:digit:]]+',data$event_data)
  data$transaction_timeline_id <- substr(data$event_data,data$transaction_timeline_id,data$transaction_timeline_id + attr(data$transaction_timeline_id,"match.length"))
  data$transaction_timeline_id <- gsub('transaction_timeline_id: |\n', '', data$transaction_timeline_id)
  data$transaction_timeline_id <- ifelse(data$transaction_timeline_id == '', NA, data$transaction_timeline_id)

  #TEMPORARY FIX - convert non-ASCII characters to allow for fidelity in merging with Postgres data
  data[,c('street_addr', 'city', 'state', 'zipcode')] <- as.data.frame(apply(data[,c('street_addr', 'city', 'state', 'zipcode')], 2, function(x) {iconv(x, "latin1", "ASCII", "byte")}))

  event_cols <- unlist(dbGetQuery(connection, "SELECT column_name 
                         FROM information_schema.columns 
                         WHERE table_name = 'property_transaction_events'
                         and table_schema = 'datasci'
                         AND column_default IS NULL"))

  colnames(data)[colnames(data)=='id'] <- 'event_id'

  #Factor to Character
  i <- sapply(data,is.factor)
  data[i] <- lapply(data[i], as.character)
  
  #Clean white spaces in character vectors
  i <- sapply(data,is.character)
  data[i] <- lapply(data[i], function(x){gsub("^\\s+|\\s+$", "", x)})
  data[i] <- lapply(data[i], function(x){gsub("\\s+", " ", x)})

  #Clean duplicate event types by property transaction
  print(noquote("Cleaning Duplicate Events"))
  
  first_events <- aggregate(data[,'event_id'], by = list(property_transaction_id = data$property_transaction_id, type = data$type), FUN = min, na.rm = TRUE)
  colnames(first_events)[colnames(first_events)=='x'] <- 'event_id'
  first_events$unique <- 1
  data <- merge(data, first_events, by = c('property_transaction_id', 'type', 'event_id'), all.x = TRUE, sort = FALSE)    
  duplicate_events <- data[is.na(data$unique),event_cols]
  
  print(noquote(paste(nrow(duplicate_events), "duplicate events found to be cleaned")))  
  if (nrow(duplicate_events)>0){print(table(duplicate_events$type))}

  data <- data[!is.na(data$unique),]
  
  #check for duplicates in new data
  if (count >0) {
    prop_tx_event_types <- dbGetQuery(connection, "SELECT distinct property_transaction_id,
                                                          type  
                                                   FROM datasci.property_transaction_events")
    duplicate_events_new <- merge(data[,event_cols], prop_tx_event_types, by = c('property_transaction_id', 'type'), all = FALSE)
    
    print(noquote(paste(nrow(duplicate_events_new), "referential duplicate events found to be cleaned")))  
    if (nrow(duplicate_events_new)>0){print(table(duplicate_events_new$type))}
    
    duplicate_events <- rbind(duplicate_events, duplicate_events_new)
    data <- data[!(data$event_id %in% duplicate_events_new$event_id),]
  }

  #Quarantine duplicate events
  if (nrow(duplicate_events) > 0) {
    duplicate_events$quarantine_reason <- 'duplicate property_transaction_id event type'
    dbWriteTable(connection, c('datasci','quarantined_property_transaction_events'), value = duplicate_events, overwrite = FALSE, append = TRUE, row.names = FALSE)
  }
  
  #Find Txs that have been both confirmed AND rejected
    #Check new data for conflicting confirmation/rejection 
  property_transactions_con_rej <- setNames(as.data.frame(intersect(data[data$type == 'PropertyTransactionConfirmedEvent','property_transaction_id'], data[data$type == 'PropertyTransactionRejectedEvent','property_transaction_id'])),'property_transaction_id')
  print(noquote(paste(nrow(property_transactions_con_rej), "transactions found to be both confirmed and rejected.")))

  #Quarantine both confirmed and rejected transactions
  if (nrow(property_transactions_con_rej) > 0) {
    bad_confirmations <- data[(data$type == 'PropertyTransactionConfirmedEvent' & data$property_transaction_id %in% property_transactions_con_rej$property_transaction_id),event_cols]
    bad_confirmations$quarantine_reason <- 'confirmed event subsequently rejected'
    dbWriteTable(connection, c('datasci','quarantined_property_transaction_events'), value = bad_confirmations, overwrite = FALSE, append = TRUE, row.names = FALSE)
  }

  data <- data[!(data$type == 'PropertyTransactionConfirmedEvent' & data$property_transaction_id %in% property_transactions_con_rej$property_transaction_id),]
  
  event_data <- data[,event_cols]
                                  
  #Properties Table
  print(noquote("Populating Properties Table"))

    pcount <- dbGetQuery(connection,"SELECT COUNT(*)
                                          FROM datasci.properties")
    print(noquote(paste(pcount, "records found in properties table.")))
    prop_cols <- unlist(dbGetQuery(connection, "SELECT column_name 
                     FROM information_schema.columns 
                     WHERE table_name = 'properties'
                     and table_schema = 'datasci'
                     AND column_default IS NULL"))  
    

    data_properties <- data[!duplicated(data[,c('street_addr', 'city', 'state', 'zipcode')]), intersect(prop_cols, colnames(data))]

    #ETL for Properties 
    if (pcount > 0) {
      print(noquote("Getting data from properties table."))
      properties <- dbGetQuery(connection, "SELECT * FROM datasci.properties")
      new_properties <- merge(data_properties, properties, by = c('street_addr', 'city', 'state', 'zipcode'), all.x = TRUE)  
      new_properties <- new_properties[is.na(new_properties$id),c('address.x','street_addr', 'city', 'state', 'zipcode')] 
      print(noquote(paste(nrow(new_properties), "new properties found to be processed.")))  
      colnames(new_properties) <- c('address', 'street_addr', 'city', 'state', 'zipcode')
      if (nrow(new_properties)>0) {
        print(noquote("Writing properties data."))
        dbWriteTable(connection, c('datasci','properties'), value = new_properties, overwrite = FALSE, append = TRUE, row.names = FALSE)
      }
      properties <- dbGetQuery(connection, "SELECT * FROM datasci.properties")
    } else {
      dbWriteTable(connection, c('datasci','properties'), value = data_properties, overwrite = FALSE, append = TRUE, row.names = FALSE)
      properties <- dbGetQuery(connection, "SELECT * FROM datasci.properties")
    } 
    
    colnames(properties)[colnames(properties)=='id'] <- 'property_id'
    data <- merge(data,properties[,c('property_id','street_addr', 'city', 'state', 'zipcode')], by = c('street_addr', 'city', 'state', 'zipcode'), all.x = TRUE, sort = FALSE)

  #Property Transactions Table  
  print(noquote("Populating Property Transactions Table"))

      ptxcount <- dbGetQuery(connection,"SELECT COUNT(*)
                                            FROM datasci.property_transactions")
      print(noquote(paste(ptxcount, "records found in property transactions table.")))
      prop_tx_cols <- unlist(dbGetQuery(connection, "SELECT column_name 
                 FROM information_schema.columns 
                 WHERE table_name = 'property_transactions'
                 and table_schema = 'datasci'
                 AND column_default IS NULL"))
        
    #ETL for Property Transactions 
    auto_tx <- data[data$type == 'PropertyTransactionAutomaticallyCreatedEvent', intersect(prop_tx_cols, colnames(data))]     
    auto_tx$creation_method <- 'automatic'    
    auto_tx$confirmation_status <- 'unconfirmed'

    manual_tx <- data[data$type == 'PropertyTransactionManuallyCreatedEvent', intersect(prop_tx_cols, colnames(data))]
    manual_tx$creation_method <- 'manual'    
    manual_tx$confirmation_status <- 'manual'
    manual_tx$scan_type <- 'manual'
    
    property_transactions <- rbind(auto_tx, manual_tx)

    #Move this to select the first auto created transaction from pe
    auto_create_date <- dbGetQuery(connection, "SELECT min(created_at) FROM public.events WHERE type = 'PropertyTransactionAutomaticallyCreatedEvent'")
    preauto_tx <- !(data$property_transaction_id %in% property_transactions$property_transaction_id) & (as.Date(auto_create_date$min) > as.Date(data$created_at))

    if (sum(preauto_tx) > 0) {
      preauto_tx <- data[data$type %in% c('PropertyTransactionConfirmedEvent','PropertyTransactionRejectedEvent') & preauto_tx, c(intersect(prop_tx_cols, colnames(data)),'type')]     
      preauto_tx$creation_method <- 'automatic'    
      preauto_tx$confirmation_status <- ifelse(preauto_tx$type == 'PropertyTransactionConfirmedEvent', 'confirmed','rejected')
      preauto_tx$scan_type <- ifelse(is.na(preauto_tx$scan_type), 'unknown', preauto_tx$scan_type)
      preauto_tx <- preauto_tx[, intersect(prop_tx_cols, colnames(preauto_tx))]
      property_transactions <- rbind(property_transactions, preauto_tx)
    }
    property_transactions$fell_through <- ifelse(property_transactions$confirmation_status == 'rejected' & property_transactions$rejection_reason == 'fell-through', TRUE, FALSE)    
    property_transactions$deleted <- FALSE

    #Write Property Transaction Data
    dbWriteTable(connection, c('datasci','property_transactions'), value = property_transactions, overwrite = FALSE, append = TRUE, row.names = FALSE)

    #Check for missing property transactions
    property_transactions <- dbGetQuery(connection,"SELECT * FROM datasci.property_transactions")
    lost_property_transaction_events <- data[!(data$property_transaction_id %in% property_transactions$property_transaction_id),]
    lost_property_transaction_events$quarantine_reason <- 'originating event not found'
    print(noquote(paste(nrow(lost_property_transaction_events), "transaction events found without a creation event.")))

    #Exclude quarantined events
    event_data <- event_data[!(event_data$event_id %in% lost_property_transaction_events$event_id),]

    #Write Property Transaction Event Data
    dbWriteTable(connection, c('datasci','property_transaction_events'), value = event_data, overwrite = FALSE, append = TRUE, row.names = FALSE)
    
    #Write Quarantined Property Transaction Event Data
    quarantined_events <- lost_property_transaction_events[,c(event_cols,'quarantine_reason')]
    print(noquote("Quarantined Events:"))
    print(table(quarantined_events$type))
   
    dbWriteTable(connection, c('datasci','quarantined_property_transaction_events'), value = quarantined_events, overwrite = FALSE, append = TRUE, row.names = FALSE)

  }

}
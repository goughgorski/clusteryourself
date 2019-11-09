populate_datasci_property_transaction_not_created_tables <- function(connection) {
  
  #Check to see if Tables Exist
  required_tables <- c('property_transaction_not_created_events', 'property_transactions_not_created', 'properties', 'quarantined_property_transaction_not_created_events')
  for (i in 1:length(required_tables)) {
      table_check <- dbExistsTable(connection, c("datasci",required_tables[i]))
      if (!table_check) {stop(print(paste(required_tables[i], " table is missing")))}
  }
    
  #Scores Below Threshold
  #Query to get new data for property_transaction
  print(noquote("Getting new data for not created events from production events table"))
  time <- Sys.time()
  data <- dbGetQuery(connection, "SELECT * FROM datasci.vw_property_transaction_not_created_events_queue")
  print(Sys.time() - time)
  #Check if any new data
  if (nrow(data)==0) {
        print('No new event data found.')
  } else {

  print(noquote(paste(nrow(data), "new records found to be processed.")))  

  print(table(data$type))
  print(noquote(paste("Includes property transaction not created events from ", min(data$created_at), "to ", max(data$created_at))))
    
  #Dump columns with no data
  data <- dump_blank_columns(data)

  #Convert Factors to Characters
  i <- sapply(data,is.factor)
  data[i] <- lapply(data[i], as.character)
        
  count <- dbGetQuery(connection,"SELECT COUNT(*)
                                          FROM datasci.property_transactions_not_created")
  print(noquote(paste(count, "records found in property transactions not created table.")))

  #Extract fields from event_data
  print(noquote("Performing ETL on event data"))

  #address
  data$address <- regexpr('address: [^\n]+',data$event_data)
  data$address <- substr(data$event_data,data$address,data$address + attr(data$address,"match.length"))
  data$address <- gsub('address: |\n', '', data$address)
      
  #Parse address
  parse_address <- strsplit(data$address, split = ',')
  street_address <- ldply(lapply(parse_address, function(x) {x[1]}),rbind)
  street_address <- as.character(street_address[,1])
  city <- ldply(lapply(parse_address, function(x) {x[length(x)-1]}),rbind)
  city <- trimws(as.character(city[,1]))
  state_zip <- ldply(as.character(lapply(parse_address, function(x) {ifelse(length(x) == 0, '', x[length(x)])}),rbind))
  state <- substr(state_zip[,1], 2, 3)
  zipcode <- substr(state_zip[,1], 5, 9)
  data <- cbind(data, city, state, zipcode)
  print(noquote(paste(sum(is.na(street_address)), "records with missing street address information.")))
  data$street_addr <- street_address

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
  data$ensemble_score  <- regexpr('ensemble_score: [[:digit:][:punct:][:alpha:]]+',data$event_data)
  data$ensemble_score <- substr(data$event_data,data$ensemble_score,data$ensemble_score + attr(data$ensemble_score,"match.length"))
  data$ensemble_score <- gsub('ensemble_score: |\n', '', data$ensemble_score)
  data$ensemble_score <- ifelse(grepl("e-", data$ensemble_score), 0, data$ensemble_score)
  data$ensemble_score <- as.numeric(data$ensemble_score)  

  #ensemble_boolean
  data$ensemble_boolean  <- regexpr('boolean: [[:alpha:]]+',data$event_data)
  data$ensemble_boolean <- substr(data$event_data,data$ensemble_boolean,data$ensemble_boolean + attr(data$ensemble_boolean,"match.length"))
  data$ensemble_boolean <- gsub('boolean: |\n', '', data$ensemble_boolean)
  data$ensemble_boolean <- ifelse(data$ensemble_boolean == '', NA, data$ensemble_boolean)

  event_cols <- unlist(dbGetQuery(connection, "SELECT column_name 
                         FROM information_schema.columns 
                         WHERE table_name = 'property_transaction_not_created_events'
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
  data_properties <- data_properties[!is.na(data_properties$street_addr),]

  #ETL for Properties 
    if (pcount > 0) {
      print(noquote("Getting data from properties table."))
      properties <- dbGetQuery(connection, "SELECT * FROM datasci.properties")
      new_properties <- merge(data_properties, properties, by = c('street_addr', 'city', 'state', 'zipcode'), all.x = TRUE, sort = FALSE)  
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

  quarantine_no_address <- is.na(data$property_id)

  print(noquote(paste(sum(quarantine_no_address), "events without address data found")))  

  #Clean duplicate event types by property transaction
  print(noquote("Cleaning Duplicate Events"))
  
  first_events <- aggregate(data[,'event_id'], by = list(property_id = data$property_id, external_account_id = data$external_account_id, originating_message_id = data$originating_message_id), FUN = min, na.rm = TRUE)
  colnames(first_events)[colnames(first_events)=='x'] <- 'event_id'
  first_events$unique <- 1
  data <- merge(data, first_events, by = c('property_id', 'external_account_id', 'originating_message_id', 'event_id'), all.x = TRUE, sort = FALSE)    
  quarantine_duplicate_events <- is.na(data$unique)
  
  print(noquote(paste(sum(quarantine_duplicate_events), "duplicate events found to be cleaned")))  
  if (sum(quarantine_duplicate_events)>0){print(table(data$type[quarantine_duplicate_events]))}

  #check for referential duplicates in new data
  prop_tx_nc_events <- dbGetQuery(connection, "SELECT distinct property_id, 
                                                          external_account_id,
                                                          originating_message_id  
                                                   FROM datasci.property_transaction_not_created_events")

  quarantine_referential_duplicate_events <- paste(as.character(data$property_id), as.character(data$external_account_id), data$originating_message_id, sep = '') %in% paste(as.character(prop_tx_nc_events$property_id), as.character(prop_tx_nc_events$external_account_id),prop_tx_nc_events$originating_message_id, sep = '')
    
  print(noquote(paste(sum(quarantine_referential_duplicate_events), "referential duplicate events found to be cleaned")))  
  if (sum(quarantine_referential_duplicate_events)>0){print(table(data$type[quarantine_referential_duplicate_events]))} else {quarantine_referential_duplicate_events <- rep(FALSE, nrow(data))}
  
  #write quarantined events
  quarantined_no_address <- data[quarantine_no_address,]
  if (sum(quarantine_no_address)>0) {quarantined_no_address$quarantine_reason <- 'No address data for event.'}  
  quarantined_duplicate_events <- data[quarantine_duplicate_events | quarantine_referential_duplicate_events,]
  if (sum(quarantine_duplicate_events, quarantine_referential_duplicate_events)>0) {quarantined_duplicate_events$quarantine_reason <- 'Duplicate property_id, external_account_id, originating_message_id.'}
  quarantined_data <- rbind(quarantined_no_address,quarantined_duplicate_events)

  quarantined_data <- ldply(lapply(split(quarantined_data, quarantined_data[, 'event_id']), function(x){
        x$quarantine_reason <- paste(x[, 'quarantine_reason'], collapse = ', ')
        return(x)
        }), rbind)

  quarantined_data <- quarantined_data[!duplicated(quarantined_data[,]),]

  quar_prop_tx_nc_ev_cols <- unlist(dbGetQuery(connection, "SELECT column_name 
                     FROM information_schema.columns 
                     WHERE table_name = 'quarantined_property_transaction_not_created_events'
                     and table_schema = 'datasci'
                     AND column_default IS NULL"))  

  quarantined_data <- quarantined_data[,quar_prop_tx_nc_ev_cols]

  dbWriteTable(connection, c('datasci','quarantined_property_transaction_not_created_events'), value = quarantined_data, overwrite = FALSE, append = TRUE, row.names = FALSE)    

  data <- data[!(quarantine_no_address | quarantine_duplicate_events | quarantine_referential_duplicate_events),]
  
  #Property Transactions Not Created Table  
  print(noquote("Populating Property Transactions Not Created Table"))

    ptxnccount <- dbGetQuery(connection,"SELECT COUNT(*)
                                          FROM datasci.property_transactions_not_created")
    print(noquote(paste(ptxnccount, "records found in property transactions not created table.")))
    
    prop_tx_nc_cols <- unlist(dbGetQuery(connection, "SELECT column_name 
               FROM information_schema.columns 
               WHERE table_name = 'property_transactions_not_created'
               and table_schema = 'datasci'
               AND column_default IS NULL"))
        
    #ETL for Property Transactions Not Created
    if (ptxnccount > 0) {
      prop_tx_nc <- dbGetQuery(connection,"SELECT * FROM datasci.property_transactions_not_created")
      new_prop_tx_nc <- data[!(paste(as.character(data$property_id), as.character(data$external_account_id), sep = '') %in% paste(as.character(prop_tx_nc$property_id), as.character(prop_tx_nc$external_account_id), sep = '')),]
    } else {new_prop_tx_nc <- data}
    
    #Select the first not created events by external_account_id, property_id
    first_events <- aggregate(new_prop_tx_nc[,'event_id'], by = list(external_account_id = new_prop_tx_nc$external_account_id, property_id = new_prop_tx_nc$property_id), FUN = min, na.rm = TRUE)
    colnames(first_events)[colnames(first_events)=='x'] <- 'event_id'
    first_events <- merge(first_events, data[,], by = c('external_account_id', 'property_id', 'event_id'), all.x = TRUE, sort = FALSE)    
    first_events$first_event_id <- first_events$event_id
    first_events$first_originating_message_id <- first_events$originating_message_id
    first_events$last_event_id <- first_events$first_event_id
    first_events$last_originating_message_id <- first_events$first_originating_message_id
    first_events$updated_at <- first_events$created_at

    prop_tx_nc <- first_events[,prop_tx_nc_cols]
    prop_tx_nc_events <- first_events[,event_cols]

    #Write first events to prop tx not created events and self table (last events will be updated with triggers)
    dbWriteTable(connection, c('datasci','property_transactions_not_created'), value = prop_tx_nc, overwrite = FALSE, append = TRUE, row.names = FALSE)
    dbWriteTable(connection, c('datasci','property_transaction_not_created_events'), value = prop_tx_nc_events, overwrite = FALSE, append = TRUE, row.names = FALSE)    

    #Write additional data to prop tx not created events table
    additional_event_data <- data[!(data$event_id %in% prop_tx_nc_events$event_id), event_cols]
    dbWriteTable(connection, c('datasci','property_transaction_not_created_events'), value = additional_event_data, overwrite = FALSE, append = TRUE, row.names = FALSE)

    #Write data to lookup table
    prop_tx_nc_lk_cols <- unlist(dbGetQuery(connection, "SELECT column_name 
               FROM information_schema.columns 
               WHERE table_name = 'lk_property_transactions_not_created_to_events'
               and table_schema = 'datasci'
               AND column_default IS NULL"))

    prop_tx_nc_lk <- dbGetQuery(connection, "SELECT id, 
                                                      external_account_id,
                                                      property_id  
                                               FROM datasci.property_transactions_not_created")

    prop_tx_nc_lk <- merge(data, prop_tx_nc_lk, by = c('external_account_id', 'property_id'), all.x = TRUE, sort = FALSE)  
    colnames(prop_tx_nc_lk)[colnames(prop_tx_nc_lk)=='id'] <- 'property_transactions_not_created_id'
    dbWriteTable(connection, c('datasci','lk_property_transactions_not_created_to_events'), value = prop_tx_nc_lk[,prop_tx_nc_lk_cols], overwrite = FALSE, append = TRUE, row.names = FALSE)

  }
}


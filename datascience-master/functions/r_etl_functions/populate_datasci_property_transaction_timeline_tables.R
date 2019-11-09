#Function to populate DS property transactions table
populate_datasci_property_transaction_timeline_tables <- function(connection) {
    
  #Check to see if Tables Exist
  required_tables <- c('property_transaction_timeline_events', 'property_transaction_events', 'property_transactions', 'properties')
  for (i in 1:length(required_tables)) {
     table_check <- dbExistsTable(connection, c("datasci",required_tables[i]))
     if (!table_check) {stop(print(paste(required_tables[i], " table is missing")))}
  }

    ########## Timeline Event Data ###########
    print(noquote("Getting new timeline data from production events table"))
    data <- dbGetQuery(connection, "SELECT * FROM datasci.vw_property_transaction_timeline_events_queue")
    
    #Check if any new data
    if (nrow(data)==0) {
        print('No new timeline event data found.')
    } else {

    print(noquote(paste(nrow(data), "new records found to be processed.")))  

    print(table(data$type))
    print(noquote(paste("Includes property transaction timeline events from ", min(data$created_at), "to ", max(data$created_at))))
    
    #Dump columns with no data
    data <- dump_blank_columns(data)

    #Convert Factors to Characters
    i <- sapply(data,is.factor)
    data[i] <- lapply(data[i], as.character)

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
    city <- ldply(lapply(parse_address, function(x) {x[length(x)-1]}),rbind)
    city <- as.character(city[,1])
    state_zip <- ldply(as.character(lapply(parse_address, function(x) {x[length(x)]}),rbind))
    state <- substr(state_zip[,1], 2, 3)
    zipcode <- substr(state_zip[,1], 5, 9)
    data <- cbind(data, city, state, zipcode)

    #external_account_id
    data$external_account_id <- regexpr('external_account_id: [[:digit:]]+',data$event_data)
    data$external_account_id <- substr(data$event_data,data$external_account_id,data$external_account_id + attr(data$external_account_id,"match.length"))
    data$external_account_id <- gsub('external_account_id: |\n', '', data$external_account_id)
    #originating_message_id
    data$originating_message_id <- regexpr('originating_message_id: [[:alnum:]]+',data$event_data)
    data$originating_message_id <- substr(data$event_data,data$originating_message_id,data$originating_message_id + attr(data$originating_message_id,"match.length"))
    data$originating_message_id <- gsub('originating_message_id: |\n', '', data$originating_message_id)
    
    #transaction_timeline_event_id
    data$transaction_timeline_event_id <- regexpr('transaction_timeline_event_id: [[:digit:]]+',data$event_data)
    data$transaction_timeline_event_id <- substr(data$event_data,data$transaction_timeline_event_id,data$transaction_timeline_event_id + attr(data$transaction_timeline_event_id,"match.length"))
    data$transaction_timeline_event_id <- gsub('transaction_timeline_event_id: |\n', '', data$transaction_timeline_event_id)
    data$transaction_timeline_event_id <- ifelse(data$transaction_timeline_event_id == "", NA, data$transaction_timeline_event_id)    

    #transaction_timeline_event_description
    data$transaction_timeline_event_description <- regexpr('transaction_timeline_event_description: [^\n]+',data$event_data)
    data$transaction_timeline_event_description <- substr(data$event_data,data$transaction_timeline_event_description,data$transaction_timeline_event_description + attr(data$transaction_timeline_event_description,"match.length"))
    data$transaction_timeline_event_description <- gsub('transaction_timeline_event_description: |\n', '', data$transaction_timeline_event_description)

    #transaction_timeline_event_date
    data$transaction_timeline_event_date <- regexpr('transaction_timeline_event_date: [-[:digit:]]+',data$event_data)
    data$transaction_timeline_event_date <- substr(data$event_data,data$transaction_timeline_event_date,data$transaction_timeline_event_date + attr(data$transaction_timeline_event_date,"match.length"))
    data$transaction_timeline_event_date <- gsub('transaction_timeline_event_date: |\n', '', data$transaction_timeline_event_date)  
    data$transaction_timeline_event_date <- ifelse(data$transaction_timeline_event_date == "", NA, data$transaction_timeline_event_date)    

    #transaction_timeline_event_start_at
    data$transaction_timeline_event_start_at <- regexpr('transaction_timeline_event_start_at: [-[:digit:]: ]+',data$event_data)
    data$transaction_timeline_event_start_at <- substr(data$event_data,data$transaction_timeline_event_start_at,data$transaction_timeline_event_start_at + attr(data$transaction_timeline_event_start_at,"match.length")-1)
    data$transaction_timeline_event_start_at <- gsub('transaction_timeline_event_start_at: |\n', '', data$transaction_timeline_event_start_at)
    data$transaction_timeline_event_start_at <- ifelse(data$transaction_timeline_event_start_at == "", NA, data$transaction_timeline_event_start_at)    

    #transaction_timeline_event_end_at
    data$transaction_timeline_event_end_at <- regexpr('transaction_timeline_event_end_at: [-[:digit:]: ]+',data$event_data)
    data$transaction_timeline_event_end_at <- substr(data$event_data,data$transaction_timeline_event_end_at,data$transaction_timeline_event_end_at + attr(data$transaction_timeline_event_end_at,"match.length")-1)
    data$transaction_timeline_event_end_at <- gsub('transaction_timeline_event_end_at: |\n', '', data$transaction_timeline_event_end_at)
    data$transaction_timeline_event_end_at <- ifelse(data$transaction_timeline_event_end_at == "", NA, data$transaction_timeline_event_end_at)    

    #transaction_timeline_event_due_date_time
    data$transaction_timeline_event_due_date_time <- regexpr('transaction_timeline_event_due_date_time: [[:alnum:]]+',data$event_data)
    data$transaction_timeline_event_due_date_time <- substr(data$event_data,data$transaction_timeline_event_due_date_time,data$transaction_timeline_event_due_date_time + attr(data$transaction_timeline_event_due_date_time,"match.length"))
    data$transaction_timeline_event_due_date_time <- gsub('transaction_timeline_event_due_date_time: |\n', '', data$transaction_timeline_event_due_date_time)
    data$transaction_timeline_event_due_date_time <- ifelse(data$transaction_timeline_event_due_date_time == "", NA, data$transaction_timeline_event_due_date_time)    

    #transaction_timeline_event_status
    data$transaction_timeline_event_status <- regexpr('transaction_timeline_event_status: [[:alpha:]]+',data$event_data)
    data$transaction_timeline_event_status <- substr(data$event_data,data$transaction_timeline_event_status,data$transaction_timeline_event_status + attr(data$transaction_timeline_event_status,"match.length"))
    data$transaction_timeline_event_status <- gsub('transaction_timeline_event_status: |\n', '', data$transaction_timeline_event_status)
    data$transaction_timeline_event_status <- ifelse(data$transaction_timeline_event_status == "", NA, data$transaction_timeline_event_status)    

    #transaction_timeline_event_status_color
    data$transaction_timeline_event_status_color <- regexpr('transaction_timeline_event_status_color: [[:digit:]]+',data$event_data)
    data$transaction_timeline_event_status_color <- substr(data$event_data,data$transaction_timeline_event_status_color,data$transaction_timeline_event_status_color + attr(data$transaction_timeline_event_status_color,"match.length"))
    data$transaction_timeline_event_status_color <- gsub('transaction_timeline_event_status_color: |\n', '', data$transaction_timeline_event_status_color)
    data$transaction_timeline_event_status_color <- ifelse(data$transaction_timeline_event_status_color == "", NA, data$transaction_timeline_event_status_color)    

    #transaction_timeline_event_status_notes
    data$transaction_timeline_event_notes <- regexpr('transaction_timeline_event_notes: [^\n].*added_attributes:',data$event_data)
    data$transaction_timeline_event_notes <- substr(data$event_data,data$transaction_timeline_event_notes,data$transaction_timeline_event_notes + attr(data$transaction_timeline_event_notes,"match.length"))
    data$transaction_timeline_event_notes <- gsub('transaction_timeline_event_notes: |\n| added_attributes:', '', data$transaction_timeline_event_notes)
    data$transaction_timeline_event_notes <- ifelse(data$transaction_timeline_event_notes == "", NA, data$transaction_timeline_event_notes)    

    #Factor to Character
    i <- sapply(data,is.factor)
    data[i] <- lapply(data[i], as.character)
    
    #Clean white spaces in character vectors
    i <- sapply(data,is.character)
    data[i] <- lapply(data[i], function(x){gsub("^\\s+|\\s+$", "", x)})
    data[i] <- lapply(data[i], function(x){gsub("\\s+", " ", x)})

    #Property Transaction Timeline Table  
    print(noquote("Populating Property Transaction Timelines Table"))

      ptxticount <- dbGetQuery(connection,"SELECT COUNT(*)
                                            FROM datasci.property_transaction_timelines")
      print(noquote(paste(ptxticount, "records found in property transaction timelines table.")))
      prop_tx_ti_cols <- unlist(dbGetQuery(connection, "SELECT column_name 
                 FROM information_schema.columns 
                 WHERE table_name = 'property_transaction_timelines'
                 and table_schema = 'datasci'
                 AND column_default IS NULL"))

    colnames(data)[colnames(data)=='id'] <- 'event_id'
    colnames(data) <- gsub('transaction_timeline_event', 'timeline_event', colnames(data))

    #Check for missing property transactions / missing timeline_event_ids
    property_transactions <- dbGetQuery(connection,"SELECT * FROM datasci.property_transactions")
    lost_property_transaction_timeline_events <- !(data$property_transaction_id %in% property_transactions$property_transaction_id)
    bad_property_transaction_timeline_events <- data$timeline_event_id == ""

    #Exclude quarantined events
    timeline_event_data <- data[!(lost_property_transaction_timeline_events | bad_property_transaction_timeline_events),]
    timeline_event_data$deleted <- FALSE

    #Write Property Transaction Timeline Data
    property_transaction_timelines <- timeline_event_data[timeline_event_data$type == 'TransactionTimelineEventCreatedEvent', intersect(prop_tx_ti_cols, colnames(timeline_event_data))] 
    dbWriteTable(connection, c('datasci','property_transaction_timelines'), value = property_transaction_timelines, overwrite = FALSE, append = TRUE, row.names = FALSE)

    #Property Transaction Timeline Events Table  
      ptxtievcount <- dbGetQuery(connection,"SELECT COUNT(*)
                                            FROM datasci.property_transaction_timeline_events")
      print(noquote(paste(ptxtievcount, "records found in property transaction timeline events table.")))
      prop_tx_ti_ev_cols <- unlist(dbGetQuery(connection, "SELECT column_name 
                 FROM information_schema.columns 
                 WHERE table_name = 'property_transaction_timeline_events'
                 and table_schema = 'datasci'
                 AND column_default IS NULL"))

    #Check for missing property transaction timeline events
    property_transaction_timelines <- dbGetQuery(connection, "SELECT * FROM datasci.property_transaction_timelines")    
    lost_property_transaction_timeline_events_ids <- !(data$timeline_event_id %in% property_transaction_timelines$timeline_event_id)

    #Quarantined Events
    timeline_event_data <- data[!(lost_property_transaction_timeline_events | bad_property_transaction_timeline_events | lost_property_transaction_timeline_events_ids),intersect(prop_tx_ti_ev_cols, colnames(data))]

    #Write Property Transaction Timeline Event Data
    dbWriteTable(connection, c('datasci','property_transaction_timeline_events'), value = timeline_event_data, overwrite = FALSE, append = TRUE, row.names = FALSE)

    #write quarantined events
    quarantined_lost_property_transaction_timeline_events <- data[lost_property_transaction_timeline_events,]
    if (sum(lost_property_transaction_timeline_events)>0) {quarantined_lost_property_transaction_timeline_events$quarantine_reason <- 'Invalid property_transaction_id for event.'}      
    quarantined_bad_property_transaction_timeline_events <- data[bad_property_transaction_timeline_events,]
    if (sum(bad_property_transaction_timeline_events)>0) {quarantined_bad_property_transaction_timeline_events$quarantine_reason <- 'missing timeline_event_id'}      
    quarantined_lost_property_transaction_timeline_events_ids <- data[lost_property_transaction_timeline_events_ids,]
    if (sum(lost_property_transaction_timeline_events_ids)>0) {quarantined_lost_property_transaction_timeline_events_ids$quarantine_reason <- 'no originating event found'}      

    quarantined_data <- rbind(quarantined_lost_property_transaction_timeline_events,quarantined_bad_property_transaction_timeline_events,quarantined_lost_property_transaction_timeline_events_ids)

    quarantined_data <- ldply(lapply(split(quarantined_data, quarantined_data[, 'event_id']), function(x){
        x$quarantine_reason <- paste(x[, 'quarantine_reason'], collapse = ', ')
        return(x)
        }), rbind)

    quarantined_data <- quarantined_data[!duplicated(quarantined_data[,]),]

    quar_prop_tx_ti_ev_cols <- unlist(dbGetQuery(connection, "SELECT column_name 
                     FROM information_schema.columns 
                     WHERE table_name = 'quarantined_property_transaction_timeline_events'
                     and table_schema = 'datasci'
                     AND column_default IS NULL"))  

    quarantined_data <- quarantined_data[,quar_prop_tx_ti_ev_cols]

    dbWriteTable(connection, c('datasci','quarantined_property_transaction_timeline_events'), value = quarantined_data, overwrite = FALSE, append = TRUE, row.names = FALSE)    
    

    }

}

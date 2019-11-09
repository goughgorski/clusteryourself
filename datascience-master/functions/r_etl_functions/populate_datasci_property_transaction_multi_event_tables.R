populate_datasci_property_transaction_multi_event_tables <- function(connection) {
    
  #Check to see if Tables Exist
  required_tables <- c('property_transaction_multi_events', 'property_transactions', 'quarantined_property_transaction_multi_events')
  for (i in 1:length(required_tables)) {
      table_check <- dbExistsTable(connection, c("datasci",required_tables[i]))
      if (!table_check) {stop(print(paste(required_tables[i], " table is missing")))}
  }

  #Property Transaction Multi Events
  print(noquote("Populating Property Transaction Multi Events Table"))  
    #Query to get new data for property_transaction_multi_events
    print(noquote("Getting new data from production events table"))
    data <- dbGetQuery(connection, "SELECT * FROM datasci.vw_property_transaction_multi_events_queue")

    #Check if any new data    
    if (nrow(data)==0) {
          print('No new multi event data found.')
    } else {
   
    print(noquote(paste(nrow(data), "new records found to be processed.")))  
    print(table(data$type))
    print(noquote(paste("Includes property transaction multi events from ", min(data$created_at), "to ", max(data$created_at))))
      
    #Dump columns with no data
    data <- dump_blank_columns(data)

    #Convert Factors to Characters
    i <- sapply(data,is.factor)
    data[i] <- lapply(data[i], as.character)
      
    count <- dbGetQuery(connection,"SELECT COUNT(*)
                                            FROM datasci.property_transaction_multi_events")
    print(noquote(paste(count, "records found in property transactions multi events table.")))

    #Extract fields from event_data
    print(noquote("Performing ETL on multi event data"))

    #property_transaction_id
    data$property_transaction_id <- regexpr('property_transaction_id: [[:digit:]]+',data$event_data)
    data$property_transaction_id <- substr(data$event_data,data$property_transaction_id,data$property_transaction_id + attr(data$property_transaction_id,"match.length"))
    data$property_transaction_id <- gsub('property_transaction_id: |\n', '', data$property_transaction_id)

    #Factor to Character
    i <- sapply(data,is.factor)
    data[i] <- lapply(data[i], as.character)
    
    #Clean white spaces in character vectors
    i <- sapply(data,is.character)
    data[i] <- lapply(data[i], function(x){gsub("^\\s+|\\s+$", "", x)})
    data[i] <- lapply(data[i], function(x){gsub("\\s+", " ", x)})
    
    #Shared with user_id
    print(noquote("Expanding share data"))
    shares <- gregexpr('user_id: [[:digit:]]+', data$event_data)
    share_ids <- vector("list", length(shares))
    for (i in 1:length(shares)) {
        for (j in 1:length(shares[[i]])) {
          share_ids[[i]][j] <- substr(data$event_data[i], shares[[i]][j], shares[[i]][j] + attr(shares[[i]],"match.length")[j])
        }
    }
    share_ids <- lapply(share_ids, function(x) {gsub('user_id: |\n', '', x)})
    share_ids <- lapply(share_ids, cbind)
    ids <- factor(data$id, levels=unique(data$id))
    share_data <- lapply(seq_along(share_ids),function(x) {cbind.data.frame(event_id = as.numeric(as.character(rep(ids[x],dim(share_ids[[x]])[1]))), value = as.numeric(share_ids[[x]]))})
    share_data <- ldply(share_data, rbind)

    multi_event_data <- merge(share_data, data, by.x = 'event_id', by.y = 'id', all.x = TRUE, sort = FALSE)
    multi_event_data$key <- 'user_id'    
    event_cols <- unlist(dbGetQuery(connection, "SELECT column_name 
                         FROM information_schema.columns 
                         WHERE table_name = 'property_transaction_multi_events'
                         and table_schema = 'datasci'
                         AND column_default IS NULL"))

    multi_event_data <- multi_event_data[,event_cols]
 
    #Check for missing shared with user_id
    print(noquote("Processing Multi-event quarantine"))
    lost_shared_with_multi_events <- aggregate(multi_event_data[,'value'], by = list(event_id = multi_event_data$event_id), FUN = function(x) { sum(is.na(x))})
    lost_shared_with_multi_events <- merge(multi_event_data, lost_shared_with_multi_events, by = 'event_id', all.x = TRUE, sort = FALSE)
    lost_shared_with_multi_events <- lost_shared_with_multi_events$x > 0
    print(paste(sum(lost_shared_with_multi_events), 'shares with no associated user id'))
    
    #Check for missing property transactions
    property_transactions <- dbGetQuery(connection,"SELECT * FROM datasci.property_transactions")
    lost_property_transaction_multi_events <- !(multi_event_data$property_transaction_id %in% property_transactions$property_transaction_id)
    print(paste(sum(lost_property_transaction_multi_events), 'sub events attached to a missing property transaction.'))

    #write quarantined events
    quarantined_no_prop_tx <- multi_event_data[lost_property_transaction_multi_events,]
    if (sum(lost_property_transaction_multi_events)>0) {quarantined_no_prop_tx$quarantine_reason <- 'originating event not found'}  

    quarantined_missing_shares <- multi_event_data[lost_shared_with_multi_events,]
    if (sum(lost_shared_with_multi_events)>0) {quarantined_missing_shares$quarantine_reason <- 'missing at least one shared with user_id'}
    
    quarantined_data <- rbind(quarantined_missing_shares, quarantined_no_prop_tx)
    
    grain <- c('event_id', 'key', 'value')
    quarantined_data_many <- quarantined_data[duplicated(quarantined_data[,grain]),grain]
    quarantined_data_many <- paste(quarantined_data$event_id, quarantined_data$key, quarantined_data$value, sep='') %in%  paste(quarantined_data_many$event_id, quarantined_data_many$key, quarantined_data_many$value, sep='')

    quarantined_data_single <- quarantined_data[!quarantined_data_many,]

    quarantined_data$value[is.na(quarantined_data$value)] <- ''
    quarantined_data_many <- ldply(lapply(split(quarantined_data[quarantined_data_many,], quarantined_data[quarantined_data_many,grain], drop = TRUE), function(x){
        x$quarantine_reason <- paste(x[, 'quarantine_reason'], collapse = ', ')
        return(x)
        }), rbind)

    quarantined_data_many <- quarantined_data_many[!duplicated(quarantined_data_many),]

    quarantined_data <- rbind(quarantined_data_single, quarantined_data_many[,colnames(quarantined_data_many) != '.id'])

    quar_event_cols <- unlist(dbGetQuery(connection, "SELECT column_name 
                 FROM information_schema.columns 
                 WHERE table_name = 'quarantined_property_transaction_multi_events'
                 and table_schema = 'datasci'
                 AND column_default IS NULL"))  

    quarantined_data <- quarantined_data[,quar_event_cols]

    dbWriteTable(connection, c('datasci','quarantined_property_transaction_multi_events'), value = quarantined_data, overwrite = FALSE, append = TRUE, row.names = FALSE)    

    #Exclude quarantined events   
    multi_event_data <- multi_event_data[!(lost_property_transaction_multi_events | lost_shared_with_multi_events),]

    #Drop duplicate within event user shares
    multi_event_data <- multi_event_data[!duplicated(multi_event_data[,grain]),]
    #Write Property Transaction Event Data
    dbWriteTable(connection, c('datasci','property_transaction_multi_events'), value = multi_event_data, overwrite = FALSE, append = TRUE, row.names = FALSE)


    }
}
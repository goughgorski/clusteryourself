populate_datasci_client_tables <- function(connection_source, connection_target) {
    
  #Check to see if Tables Exist
  required_tables <- c('client_events', 'clients', 'quarantined_client_events')
  for (i in 1:length(required_tables)) {
      table_check <- dbExistsTable(connection_target, c("datasci",required_tables[i]))
      if (!table_check) {stop(print(paste(required_tables[i], " table is missing")))}
  }

  #Check to see if Tables Exist
  required_tables <- c('events')
  for (i in 1:length(required_tables)) {
    table_check <- dbExistsTable(connection_source, c("public",required_tables[i]))
    if (!table_check) {stop(print(paste(required_tables[i], " table is missing")))}
  }

  #Query to get new data for property_transaction
  print(noquote("Getting new data from production events table"))
  last_found_client_event <- dbGetQuery(connection_target, "SELECT MAX(TIMEZONE('UTC', created_at)) FROM datasci.client_events")

  if (is.na(last_found_client_event[1,1])){last_found_client_event <- as.data.frame('01-01-1900')}

  data <- dbGetQuery(connection_source, paste("SELECT * FROM public.events WHERE type IN (
                                              'ProspectFolderAutomaticallyCreatedEvent',
                                              'ProspectFolderConfirmedEvent',
                                              'ProspectFolderDeletedEvent',
                                              'ProspectFolderManuallyCreatedEvent',
                                              'ProspectFolderRejectedEvent') AND (
                                              created_at >=",paste("'",last_found_client_event[1,1],"'", sep = ''), 
                                              "OR updated_at >=",paste("'",last_found_client_event[1,1],"'", sep = ''),")"))
  #Check if any new data
  if (nrow(data)==0) {
        print('No new event data found.')
  } else {

    print(noquote(paste(nrow(data), "new records found to be processed.")))  

    print(table(data$type))
    print(noquote(paste("Includes client events from ", min(data$created_at), "to ", max(data$created_at))))
      
    #Dump columns with no data
    data <- dump_blank_columns(data)

    #Convert Factors to Characters
    i <- sapply(data,is.factor)
    data[i] <- lapply(data[i], as.character)
     
    count <- dbGetQuery(connection_target,"SELECT COUNT(*)
                                            FROM datasci.client_events")
    print(noquote(paste(count, "records found in client events table.")))

    #Extract fields from event_data
    print(noquote("Performing ETL on event data"))
      
    #Use to check for new information in event data
    #event_data <- strsplit(data$event_data,'\n')
    
    #prospect_folder_id
    data$prospect_folder_id <- regexpr('prospect_folder_id: [[:digit:]]+',data$event_data)
    data$prospect_folder_id <- substr(data$event_data,data$prospect_folder_id,data$prospect_folder_id + attr(data$prospect_folder_id,"match.length"))
    data$prospect_folder_id <- gsub('prospect_folder_id: |\n', '', data$prospect_folder_id)
    data$prospect_folder_id <- ifelse(data$prospect_folder_id == '', NA, data$prospect_folder_id)
    
    #external_account_id
    data$external_account_id <- regexpr('gmail_token_id: [[:digit:]]+|external_account_id: [[:digit:]]+',data$event_data)
    data$external_account_id <- substr(data$event_data,data$external_account_id,data$external_account_id + attr(data$external_account_id,"match.length"))
    data$external_account_id <- gsub('gmail_token_id: |external_account_id: |\n', '', data$external_account_id)
    data$external_account_id <- ifelse(data$external_account_id == '', NA, data$external_account_id)

    #originating_message_id
    data$originating_message_id <- regexpr('originating_message_id: [[:alnum:]]+',data$event_data)
    data$originating_message_id <- substr(data$event_data,data$originating_message_id,data$originating_message_id + attr(data$originating_message_id,"match.length"))
    data$originating_message_id <- gsub('originating_message_id: |\n', '', data$originating_message_id)
    data$originating_message_id <- ifelse(data$originating_message_id == '', NA, data$originating_message_id)
      
    #scan_type
    data$scan_type <- regexpr('scan_type: [[:print:]]+',data$event_data)
    data$scan_type <- substr(data$event_data,data$scan_type,data$scan_type + attr(data$scan_type,"match.length"))
    data$scan_type <- gsub('scan_type: |\n', '', data$scan_type)
    data$scan_type <- ifelse(data$scan_type == '', NA, data$scan_type)
    #fix inconsistent scan types
    data$scan_type <- paste(data$scan_type, '_scan', sep = '')

    #representation
    data$representation <- regexpr('representation: [[:digit:][:alpha:][:space:]]+',data$event_data)
    data$representation <- substr(data$representation,data$representation,data$representation + attr(data$representation,"match.length"))
    data$representation <- gsub('representation: |\n', '', data$representation)
    data$representation <- ifelse(data$representation == '', NA, data$representation)

    #client_ids
    data$client_ids <- regexpr('client_ids?:\n?-? [[:digit:]]+',data$event_data)
    data$client_ids <- substr(data$event_data,data$client_ids,data$client_ids + attr(data$client_ids,"match.length"))
    data$client_ids <- gsub('client_ids?:\n?-? |\n', '', data$client_ids)
    data$client_ids <- ifelse(data$client_ids == '', NA, data$client_ids)

    #client_emails
    data$client_emails <- regexpr('client_emails?: ?\n?-? [[:digit:][:alpha:][:punct:]]+[:space:]?',data$event_data)
    data$client_emails <- substr(data$event_data,data$client_emails,data$client_emails + attr(data$client_emails,"match.length"))
    data$client_emails <- gsub('client_emails?: ?\n?-? |[[:space:]]', '', data$client_emails)
    data$client_emails <- ifelse(data$client_emails == '', NA, data$client_emails)

    #rejection_reason
    data$rejection_reason <- regexpr('rejection_reason: [-[:alpha:]]+',data$event_data)
    data$rejection_reason <- substr(data$event_data,data$rejection_reason,data$rejection_reason + attr(data$rejection_reason,"match.length"))
    data$rejection_reason <- gsub('rejection_reason: |\n', '', data$rejection_reason)
    
    #rejection_notes
    data$rejection_notes <- regexpr('rejection_notes: [[:print:]]+',data$event_data)
    data$rejection_notes <- substr(data$event_data,data$rejection_notes,data$rejection_notes + attr(data$rejection_notes,"match.length"))
    data$rejection_notes <- gsub('rejection_notes: |\n', '', data$rejection_notes)

    #deletion_method
    data$deletion_method <- regexpr('deletion_method: [[:alpha:]_]+',data$event_data)
    data$deletion_method <- substr(data$event_data,data$deletion_method,data$deletion_method + attr(data$deletion_method,"match.length"))
    data$deletion_method <- gsub('deletion_method: |\n', '', data$deletion_method)

    event_cols <- unlist(dbGetQuery(connection_target, "SELECT column_name 
                           FROM information_schema.columns 
                           WHERE table_name = 'client_events'
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

    #Quarantine ghost events
    required_columns <- unlist(dbGetQuery(connection_target, "SELECT column_name
                           FROM information_schema.columns 
                           WHERE table_name = 'client_events'
                           AND table_schema = 'datasci'
                           AND column_default IS NULL
                           AND is_nullable = 'NO'"))

    data$prospect_folder_id <- as.integer(data$prospect_folder_id)
    data$external_account_id <- as.integer(data$external_account_id)
    data$event_id <- as.integer(data$event_id)

    i <- sapply(data[,required_columns],function(x) {as.character(x) == '' | is.na(x)}) 
    i <- apply(i,1,any)
    ghost_events <- data[i,event_cols]
    data <- data[!i,]

    i <- sapply(data[,'client_emails'],function(x) {as.character(x) == '[]' | regexpr('^[\'"]', as.character(x)) > 0 }) 
    i <- as.vector(i)
    no_email_events <- data[i,event_cols]
    data <- data[!i,]      

    print(noquote(paste(nrow(ghost_events), "ghost events found to be cleaned")))  
    if (nrow(ghost_events)>0){
      print(table(ghost_events$type))
      ghost_events$quarantine_reason <- 'missing required data'
      dbWriteTable(connection_target, c('datasci','quarantined_client_events'), value = ghost_events, overwrite = FALSE, append = TRUE, row.names = FALSE)
    }

    print(noquote(paste(nrow(no_email_events), "missing or broken client email address events found to be cleaned")))  
    if (nrow(no_email_events)>0){
      print(table(no_email_events$type))
      no_email_events$quarantine_reason <- 'missing or broken client email address'
      dbWriteTable(connection_target, c('datasci','quarantined_client_events'), value = no_email_events, overwrite = FALSE, append = TRUE, row.names = FALSE)
    }

    #Clean duplicate event types by client
    print(noquote("Cleaning Duplicate Events"))
    
    first_events <- aggregate(data[,'event_id'], by = list(prospect_folder_id = data$prospect_folder_id, type = data$type), FUN = min, na.rm = TRUE)
    colnames(first_events)[colnames(first_events)=='x'] <- 'event_id'
    first_events$unique <- 1
    data <- merge(data, first_events, by = c('prospect_folder_id', 'type', 'event_id'), all.x = TRUE, sort = FALSE)    
    duplicate_events <- data[is.na(data$unique),event_cols]
    
    print(noquote(paste(nrow(duplicate_events), "duplicate events found to be cleaned")))  
    if (nrow(duplicate_events)>0){print(table(duplicate_events$type))}

    data <- data[!is.na(data$unique),]
    
    #check for duplicates in new data
    if (count >0) {
      client_event_types <- dbGetQuery(connection_target, "SELECT distinct prospect_folder_id,
                                                            type  
                                                     FROM datasci.client_events")
      duplicate_events_new <- merge(data[,event_cols], client_event_types, by = c('prospect_folder_id', 'type'), all = FALSE)
      
      print(noquote(paste(nrow(duplicate_events_new), "referential duplicate events found to be cleaned")))  
      if (nrow(duplicate_events_new)>0){print(table(duplicate_events_new$type))}
      
      duplicate_events <- rbind(duplicate_events, duplicate_events_new)
      data <- data[!(data$event_id %in% duplicate_events_new$event_id),]
    }

    #Quarantine duplicate events
    if (nrow(duplicate_events) > 0) {
      duplicate_events$quarantine_reason <- 'duplicate prospect_folder_id event type'
      dbWriteTable(connection_target, c('datasci','quarantined_client_events'), value = duplicate_events, overwrite = FALSE, append = TRUE, row.names = FALSE)
    }

    #Find Clients that have been both confirmed AND rejected
      #Check new data for conflicting confirmation/rejection 
    clients_con_rej <- setNames(as.data.frame(intersect(data[data$type == 'ProspectFolderConfirmedEvent','prospect_folder_id'], data[data$type == 'ProspectFolderRejectedEvent','prospect_folder_id'])),'prospect_folder_id')
    print(noquote(paste(nrow(clients_con_rej), "clients found to be both confirmed and rejected.")))

    #Quarantine both confirmed and rejected transactions
    if (nrow(clients_con_rej) > 0) {
      bad_confirmations <- data[(data$type == 'ProspectFolderConfirmedEvent' & data$prospect_folder_id %in% clients_con_rej$prospect_folder_id),event_cols]
      bad_confirmations$quarantine_reason <- 'confirmed event subsequently rejected'
      dbWriteTable(connection_target, c('datasci','quarantined_client_events'), value = bad_confirmations, overwrite = FALSE, append = TRUE, row.names = FALSE)
    }

    data <- data[!(data$type == 'ProspectFolderConfirmedEvent' & data$prospect_folder_id %in% clients_con_rej$prospect_folder_id),]
    
    event_data <- data[,event_cols]
                                    
    #Clients Table  
    print(noquote("Populating Clients Table"))

    cli_table <- dbGetQuery(connection_target,"SELECT *
                                              FROM datasci.clients")
    print(noquote(paste(nrow(cli_table), "records found in clients table.")))
    
    cli_cols <- unlist(dbGetQuery(connection_target, "SELECT column_name 
                   FROM information_schema.columns 
                   WHERE table_name = 'clients'
                   and table_schema = 'datasci'
                   AND column_default IS NULL"))
          
    #ETL for Clients 
    auto_cli <- data[data$type == 'ProspectFolderAutomaticallyCreatedEvent',]     
    auto_cli$creation_method <- 'automatic'    
    auto_cli$confirmation_status <- 'unconfirmed'

    manual_cli <- data[data$type == 'ProspectFolderManuallyCreatedEvent',]
    manual_cli$creation_method <- 'manual'    
    manual_cli$confirmation_status <- 'manual'
    manual_cli$scan_type <- 'manual'
      
    clients <- rbind(auto_cli, manual_cli)
    clients$deleted <- FALSE
    clients$client_emails <- tolower(clients$client_emails)

    #Get Contact Ids 
    print(noquote("Getting data from contacts table."))
    contacts <- dbGetQuery(connection_target, "SELECT * FROM datasci.contacts")
    colnames(contacts)[colnames(contacts)=='id'] <- 'client_email_address_id'    
    clients <- merge(clients,contacts[,c('client_email_address_id','email')], by.x = 'client_emails', by.y = 'email', all.x = TRUE, sort = FALSE)

    contacts_cols <- unlist(dbGetQuery(connection_target, "SELECT column_name 
                   FROM information_schema.columns 
                   WHERE table_name = 'contacts'
                   and table_schema = 'datasci'
                   AND column_default IS NULL"))

    contacts_update <- clients[is.na(clients$client_email_address_id),]

    #Store update for lk_contacts_to_business_objects
    lk_bo_contacts <- contacts_update[, c('client_emails', 'prospect_folder_id')]
    lk_bo_contacts$business_object_identifier <- 'prospect_folder_id'
    lk_bo_contacts$role <- NULL
    colnames(lk_bo_contacts)[which(colnames(lk_bo_contacts) == 'prospect_folder_id')] <- 'business_object_id_value'

    contacts_update <- data.frame(email = contacts_update[!duplicated(contacts_update$client_emails),c('client_emails')])
 
    #Write Update back to Contacts
    print(noquote("Writing deleted clients to contacts table."))
    dbWriteTable(connection_target, c('datasci', 'contacts'), value = contacts_update, overwrite = FALSE, append = TRUE, row.names = FALSE)
    
    #Get updated contact ids and add to clients
    new_contacts <- dbGetQuery(connection_target, "SELECT * FROM datasci.contacts")
    new_contacts <- merge(contacts_update, new_contacts[, c('email', 'id')], by = 'email')
    clients <- merge(clients, new_contacts, by.x = 'client_emails', by.y = 'email', all.x = T)
    clients$client_email_address_id <- ifelse(is.na(clients$client_email_address_id), clients$id, clients$client_email_address_id)

    clients <- clients[, cli_cols]

    #Quarantine?   

    #Remove existing clients from data before being written to db
    clients <- clients[!clients$client_email_address_id %in% cli_table$client_email_address_id,]

    #Write Client Data
    print(noquote("Writing to clients table."))
    dbWriteTable(connection_target, c('datasci','clients'), value = clients, overwrite = FALSE, append = TRUE, row.names = FALSE)

    #Update lk_contacts_to_business_objects

    lk_bo_contacts_cols <- unlist(dbGetQuery(connection_target, "SELECT column_name 
                   FROM information_schema.columns 
                   WHERE table_name = 'lk_contacts_to_business_objects'
                   and table_schema = 'datasci'
                   AND column_default IS NULL"))

    #Merge in email_address_id from new_contacts
    lk_bo_contacts <- merge(lk_bo_contacts, new_contacts, by.x = 'client_emails', by.y = 'email', all.x = TRUE)
    colnames(lk_bo_contacts)[which(colnames(lk_bo_contacts) == 'id')] <- 'email_address_id'
    lk_bo_contacts$role <- NA
    lk_bo_contacts <- lk_bo_contacts[, lk_bo_contacts_cols]

    #Write new records to lk_contacts_to_business_objects
    print(noquote("Writing to contacts to business objects lookup table."))
    dbWriteTable(connection_target, c('datasci','lk_contacts_to_business_objects'), value = lk_bo_contacts, overwrite = FALSE, append = TRUE, row.names = FALSE)

    #Write Client Event Data
    print(noquote("Writing to client events table."))
    dbWriteTable(connection_target, c('datasci','client_events'), value = event_data, overwrite = FALSE, append = TRUE, row.names = FALSE)

}
}
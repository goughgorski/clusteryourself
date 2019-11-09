populate_datasci_user_tables <- function(connection) {

  #Check to see if User Events Table Exists and has data
  user_ev_table_exists <- dbExistsTable(connection, c("datasci","user_events"))
  users_table_exists <- dbExistsTable(connection, c("datasci","users"))
  hist_user_el_table_exists <- dbExistsTable(connection, c("datasci_historical","user_elements"))
  hist_ext_acct_table_exists <- dbExistsTable(connection, c("datasci_historical","external_accounts"))
  hist_conf_rej_table_exists <- dbExistsTable(connection, c("datasci_historical","confirmed_rejected_ids"))
  
  tables_exist <- c(user_ev_table_exists, users_table_exists, hist_user_el_table_exists, hist_ext_acct_table_exists, hist_conf_rej_table_exists)
  required_tables <- c('user_events', 'users', 'hist_user_elements', 'hist_external_accounts', 'hist_confirmed_rejected_ids')
  if (any(tables_exist== FALSE)) {
    stop(print(paste(required_tables[tables_exist == FALSE], " table is missing")))
  }

  #Query to get new data for property_transaction
  print(noquote("Getting new data from production events table"))
  data <- dbGetQuery(connection, "SELECT * FROM datasci.vw_user_events_queue")

  #Check if any new data
  if (nrow(data)==0) {
        print('No new event data found.')
  } else {
  
  print(noquote(paste(nrow(data), "new records found to be processed.")))  

  print(table(data$type))
  print(noquote(paste("Includes user events from ", min(data$created_at), "to ", max(data$created_at))))

  #Dump columns with no data
  data <- dump_blank_columns(data)

  #Convert Factors to Characters
  i <- sapply(data,is.factor)
  data[i] <- lapply(data[i], as.character)
    
  count <- dbGetQuery(connection,"SELECT COUNT(*)
                                          FROM datasci.user_events")
  print(noquote(paste(count, "records found in user events table.")))

  #Extract fields from event_data
  print(noquote("Performing ETL on event data"))  

  #external_account_id
  data$external_account_id <- regexpr('token_id: [[:digit:]]+',data$event_data)
  data$external_account_id <- substr(data$event_data,data$external_account_id,data$external_account_id + attr(data$external_account_id,"match.length"))
  data$external_account_id <- gsub('token_id: |\n', '', data$external_account_id)
  data$external_account_id <- ifelse(data$external_account_id == '', NA, data$external_account_id)
  
  #email
  data$email <- regexpr('token_email: [[:print:]]+',data$event_data)
  data$email <- substr(data$event_data,data$email,data$email + attr(data$email,"match.length"))
  data$email <- gsub('token_email: |\n', '', data$email)
  data$email <- ifelse(data$email == '', NA, data$email)

  #subscription_plan_name
  data$subscription_plan_name <- regexpr('plan_name: [[:print:]]+',data$event_data)
  data$subscription_plan_name <- substr(data$event_data,data$subscription_plan_name,data$subscription_plan_name + attr(data$subscription_plan_name,"match.length"))
  data$subscription_plan_name <- gsub('plan_name: |\n', '', data$subscription_plan_name)
  data$subscription_plan_name <- ifelse(data$subscription_plan_name == '', NA, data$subscription_plan_name)

  event_cols <- unlist(dbGetQuery(connection, "SELECT column_name 
                         FROM information_schema.columns 
                         WHERE table_name = 'user_events'
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

  #Clean duplicate event types by user
  print(noquote("Cleaning Unmatched and Duplicate Events"))

  #Pull exhaustive list of external_account_id and acting_user_id pairs
  ids <- dbGetQuery(connection, "SELECT acting_user_id, event_data FROM public.events
                                  WHERE type = 'GmailLinkedEvent'")

  #ETL for external_account_id
  ids$external_account_id <- regexpr('token_id: [[:digit:]]+',ids$event_data)
  ids$external_account_id <- substr(ids$event_data,ids$external_account_id,ids$external_account_id + attr(ids$external_account_id,"match.length"))
  ids$external_account_id <- gsub('token_id: |\n', '', ids$external_account_id)
  ids$external_account_id <- ifelse(ids$external_account_id == '', NA, ids$external_account_id)

  #Make historical tables
  ids2 <- dbGetQuery(connection, "SELECT * FROM datasci_historical.external_accounts")

  ids3 <- dbGetQuery(connection, "SELECT * FROM datasci_historical.confirmed_rejected_ids")

  ids <- rbind.fill(ids, ids2, ids3)

  #Keep only unique pairs of id 
  ids <- ids[!is.na(ids$external_account_id) & !is.na(ids$acting_user_id),]
  ids <- ids[!duplicated(ids$acting_user_id) & !duplicated(ids$external_account_id), c('acting_user_id', 'external_account_id')]

  #Separate events without either external_account_id or acting_user_id matching ids
  unmatched_events <- data[!data$acting_user_id %in% ids$acting_user_id  & !data$external_account_id %in% ids$external_account_id, event_cols]
  if (nrow(unmatched_events) > 0) {
    unmatched_events$quarantine_reason <- 'external_account_id and acting_user_id unmatched'
    unmatched_events$quarantine_reason[is.na(unmatched_events$external_account_id)] <- 'external_account_id missing and acting_user_id unmatched'
    unmatched_events$quarantine_reason[is.na(unmatched_events$acting_user_id)] <- 'acting_user_id missing and external_account_id unmatched'
    unmatched_events$quarantine_reason[is.na(unmatched_events$acting_user_id) &
     is.na(unmatched_events$external_account_id)] <- 'both external_account_id and acting_user_id missing'
  }
  print(noquote(paste(nrow(unmatched_events), "unmatched events found to be cleaned")))  
  if (nrow(unmatched_events)>0){print(table(unmatched_events$type))}
  if (nrow(unmatched_events)>0){print(table(unmatched_events$quarantine_reason))}

  #Remove unmatched events and merge ids into data
  data <- data[!data$event_id %in% unmatched_events$event_id,]
  data_acting <- merge(data[!is.na(data$acting_user_id),], ids, by = 'acting_user_id', all.x = TRUE, sort = FALSE)
  data_acting$external_account_id.x <- NULL
  colnames(data_acting)[colnames(data_acting) == 'external_account_id.y'] <- 'external_account_id'
  data_external <- merge(data[!is.na(data$external_account_id),], ids, by = 'external_account_id', all.x = TRUE, sort = FALSE)
  data_external$acting_user_id.x <- NULL
  colnames(data_external)[colnames(data_external) == 'acting_user_id.y'] <- 'acting_user_id'
  data <- rbind(data_acting, data_external)
  data <- data[!duplicated(data[, 'event_id']),]

  #Separate users not found in event data
  no_event <- ids[!ids$external_account_id %in% data$external_account_id,]

  first_events <- aggregate(data[,'event_id'],
    by = list(external_account_id = data$external_account_id, acting_user_id = data$acting_user_id, type = data$type), FUN = min, na.rm = TRUE)
  colnames(first_events)[colnames(first_events)=='x'] <- 'event_id'
  first_events$unique <- 1
  data <- merge(data, first_events, by = c('external_account_id','acting_user_id', 'type', 'event_id'), all.x = TRUE, sort = FALSE)    
  duplicate_events <- data[is.na(data$unique),event_cols]
  
  print(noquote(paste(nrow(duplicate_events), "duplicate events found to be cleaned")))  
  if (nrow(duplicate_events)>0){print(table(duplicate_events$type))}

  data <- data[!is.na(data$unique),]
  
  #Check for duplicates in new data
  if (count > 0) {
    user_event_types <- dbGetQuery(connection, "SELECT distinct acting_user_id,
                                                          type  
                                                   FROM datasci.user_events")
    duplicate_events_new <- merge(data[,event_cols], user_event_types, by = c('acting_user_id', 'type'), all = FALSE)
    
    print(noquote(paste(nrow(duplicate_events_new), "referential duplicate events found to be cleaned")))  
    if (nrow(duplicate_events_new)>0){print(table(duplicate_events_new$type))}
    
    duplicate_events <- rbind(duplicate_events, duplicate_events_new)
    data <- data[!(data$event_id %in% duplicate_events_new$event_id),]
  }

  #Quarantine unmatched and duplicate events
  if (nrow(unmatched_events) > 0) {
    dbWriteTable(connection, c('datasci','quarantined_user_events'), value = unmatched_events, overwrite = FALSE, append = TRUE, row.names = FALSE)
  }
  
  if (nrow(duplicate_events) > 0) {
    duplicate_events$quarantine_reason <- 'duplicate acting_user_id event type'
    dbWriteTable(connection, c('datasci','quarantined_user_events'), value = duplicate_events, overwrite = FALSE, append = TRUE, row.names = FALSE)
  }

  event_data <- data[,event_cols]
                                  
  #Users Table  
  print(noquote("Populating Users Table"))

  #Add no event users
  data <- rbind.fill(data, no_event)

    #Check to see if Users Table Exists and has data

    if (!users_table_exists) {
      print(noquote("No users table in Data Science found."))
      ucount <- 0
    }

    if (users_table_exists){
      users_table <- dbGetQuery(connection,"SELECT *
                                            FROM datasci.users")
      ucount <- nrow(users_table)
      print(noquote(paste(ucount, "records found in users table.")))
      users_cols <- unlist(dbGetQuery(connection, "SELECT column_name 
                 FROM information_schema.columns 
                 WHERE table_name = 'users'
                 and table_schema = 'datasci'
                 AND column_default IS NULL"))
    }
    
    #Pull signup and Stripe information from historical table if necessary
    if (ucount > 0) {
      user_table_max_date <- dbGetQuery(connection, "SELECT MAX(updated_at) FROM datasci.users")
      hist_data_max_date <- dbGetQuery(connection, "SELECT MAX(created_at) FROM datasci_historical.user_elements")
      if (hist_data_max_date > user_table_max_date) {
        print(noquote("Relevant data found in historical table"))
        hist_data <- dbGetQuery(connection, "SELECT * FROM datasci_historical.user_elements")
      } else {
        print(noquote("No relevant data found in historical table"))}
    } else {
      print(noquote("Relevant data found in historical table"))
      hist_data <- dbGetQuery(connection, "SELECT * FROM datasci_historical.user_elements")}

    if (!is.null(hist_data)) {
      hist_data <- hist_data[!duplicated(hist_data[, 'external_account_id']),]
      data <- merge(data, hist_data[!is.na(hist_data$external_account_id), !colnames(hist_data) %in% 'created_at'],
         by = 'external_account_id', all.x = T, sort = F)
    }
    #Remove id columns before splitting wide
    data_id <- data[, c('external_account_id', 'acting_user_id', 'email', 'subscription_plan_name',
     'signup_method', 'stripe_customer_id', 'subscription_canceled_on', 'updated_at')]
    data_id <- suppressWarnings(aggregate(data_id, by=list(data_id$external_account_id, data_id$acting_user_id),
      FUN = max, na.rm = T))
    data_id <- data_id[, c('external_account_id', 'acting_user_id', 'email', 'subscription_plan_name',
     'signup_method', 'stripe_customer_id', 'subscription_canceled_on', 'updated_at')]
    data_id$signup_method[data_id$signup_method == -Inf] <- NA

    #Convert created_at column to character to split wide
    data$created_at <- as.character(data$created_at)

    data <- dcast(data, external_account_id + acting_user_id ~ type, value.var = 'created_at')
    
    date_cols <- c('FolioUninstalledEvent', 'GmailLinkedEvent', 'GmailUnlinkedEvent', 'SubscriptionCreatedEvent')

    #Convert back to date
    data[, date_cols] <- sapply(data[, date_cols], as.POSIXct, simplify = FALSE)

    colnames(data)[colnames(data) %in% date_cols] <- c('account_deleted_at', 'start_date', 'uninstalled_at', 'subscribed_at')

    #Merge id data back in
    data <- merge(data, data_id, by = c('external_account_id', 'acting_user_id'), all.x = T, sort = FALSE)

    #Create proxy install date for users who installed prior to existence of GmailLinkedEvent
    data$start_type <- ifelse(is.na(data$start_date), NA, 'install')

    #Select the first GmailLinkedEvent from pe
    gmail_linked_date <- dbGetQuery(connection, "SELECT min(created_at) FROM public.events WHERE type = 'GmailLinkedEvent'")
    no_start_users <- event_data$external_account_id[event_data$created_at < gmail_linked_date$min]
    no_start_users <- c(no_start_users, data$external_account_id[!data$external_account_id %in% 
      event_data$external_account_id[event_data$type == 'GmailLinkedEvent']])

    if (length(no_start_users) > 0) {
      first_tx <- dbGetQuery(connection, "SELECT external_account_id, MIN(created_at) 
              FROM (
              SELECT external_account_id, created_at
              FROM datasci.property_transaction_events
              WHERE type IN ('PropertyTransactionAutomaticallyCreatedEvent', 'PropertyTransactionConfirmedEvent', 'PropertyTransactionManuallyCreatedEvent', 'PropertyTransactionRejectedEvent')
              ) a
              GROUP BY external_account_id")
      proxy_start_users <- c(no_start_users, no_event$external_account_id)
      first_tx <- first_tx[first_tx$external_account_id %in% intersect(first_tx$external_account_id, proxy_start_users),]
      pre <- data[data$external_account_id %in% first_tx$external_account_id,]
      post <- data[!data$external_account_id %in% first_tx$external_account_id,]
      pre <- merge(pre, first_tx, by = 'external_account_id', all.x = T, sort = FALSE)
      pre$start_type <- 'proxy'
      pre$start_date <- NULL
      colnames(pre)[colnames(pre) == 'min'] <- 'start_date'
      data <- rbind(pre, post)
    }

    user_data <- data[!is.na(data$start_type), users_cols]

    #Remove existing users from user_data before being written to db
    user_data <- user_data[!user_data$acting_user_id %in% users_table$acting_user_id,]
    user_data <- user_data[!user_data$external_account_id %in% users_table$external_account_id,]

    #Write User Data
    dbWriteTable(connection, c('datasci','users'), value = user_data, overwrite = FALSE, append = TRUE, row.names = FALSE)
    
    #Check for missing start date
    users <- dbGetQuery(connection,"SELECT * FROM datasci.users")
    lost_user_events <- event_data[!(event_data$external_account_id %in% users$external_account_id),]
    if (nrow(lost_user_events) > 0) {lost_user_events$quarantine_reason <- 'originating event not found'}
    print(noquote(paste(nrow(lost_user_events), "user events found without a creation event.")))

    #Exclude quarantined events
    event_data <- event_data[!(event_data$event_id %in% lost_user_events$event_id),]
    
    #Write User Event Data
    dbWriteTable(connection, c('datasci','user_events'), value = event_data, overwrite = FALSE, append = TRUE, row.names = FALSE)
    
    #Write Quarantined Property Transaction Event Data
    if (nrow(lost_user_events) > 0) { 
      quarantined_events <- lost_user_events[,c(event_cols,'quarantine_reason')]
      print(noquote("Quarantined Events:"))
      print(table(quarantined_events$type))
   
      dbWriteTable(connection, c('datasci','quarantined_user_events'), value = quarantined_events, overwrite = FALSE, append = TRUE, row.names = FALSE)

    }

  }
  
}
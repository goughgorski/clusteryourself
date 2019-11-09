#Complexity Index - Create a metric reflecting the "complexity" of a transaction.
populate_complexity_index <- function(connection) {

    print(noquote("Processing complexity index data"))

    #Get Counts of activity by Transaction
    print(noquote("Fetching data"))
    email_counts <- dbGetQuery(connection, "SELECT id as property_transaction_id
                                                        , email_count
                                                        , created_at
                                                        FROM public.property_transactions")

    document_counts <- dbGetQuery(connection, "SELECT property_transaction_id
                                                        , created_at
                                                        FROM public.documents")

    contact_counts <- dbGetQuery(connection, "SELECT property_transaction_id
                                                        , created_at
                                                        FROM prod.gmail_contacts")
    
    print(noquote("Computing complexity index"))
    #DeDupe Email Counts- Unclear where there are duplicates for some 
    dupe_check <- duplicated(email_counts[,'property_transaction_id'])
    sum(dupe_check)
    names(email_counts)[names(email_counts) == "created_at"] <- "min_email_created_at"        
    email_counts$max_email_created_at <- email_counts$min_email_created_at

    #Document Counts
    document_counts$document_count <- 1
    document_agg <- aggregate(document_count ~ property_transaction_id, data = document_counts, FUN = sum, na.rm = T)
    document_date_max <- aggregate(created_at ~ property_transaction_id, data = document_counts, FUN = max, na.rm = T)
    document_date_min <- aggregate(created_at ~ property_transaction_id, data = document_counts, FUN = min, na.rm = T)
    document_agg <- merge(document_agg, document_date_min, by = 'property_transaction_id', all = T)
    names(document_agg)[names(document_agg) == "created_at"] <- "min_document_created_at"        
    document_agg <- merge(document_agg, document_date_max, by = 'property_transaction_id', all = T)
    names(document_agg)[names(document_agg) == "created_at"] <- "max_document_created_at"        

    #Contact Counts
    contact_counts$contact_count <- 1
    contact_agg <- aggregate(contact_count ~ property_transaction_id, data = contact_counts, FUN = sum, na.rm = T)
    contact_date_max <- aggregate(created_at ~ property_transaction_id, data = contact_counts, FUN = max, na.rm = T)
    contact_date_min <- aggregate(created_at ~ property_transaction_id, data = contact_counts, FUN = min, na.rm = T)
    contact_agg <- merge(contact_agg, contact_date_min, by = 'property_transaction_id', all = T)
    names(contact_agg)[names(contact_agg) == "created_at"] <- "min_contact_created_at"        
    contact_agg <- merge(contact_agg, contact_date_max, by = 'property_transaction_id', all = T)
    names(contact_agg)[names(contact_agg) == "created_at"] <- "max_contact_created_at"        

    #Merge 
    complexity <- merge(email_counts, document_agg, by = 'property_transaction_id', all = T)
    complexity <- merge(complexity, contact_agg, by = 'property_transaction_id', all = T)
    nrow(complexity)

    #Reconnect to get property transaction data
    property_transactions <- dbGetQuery(connection, "SELECT a.* 
                                                        FROM ( SELECT *
                                                                FROM datasci.property_transaction_events
                                                                WHERE type IN ('PropertyTransactionConfirmedEvent'
                                                                        ,'PropertyTransactionManuallyCreatedEvent')
                                                            ) a 
                                                        LEFT JOIN ( SELECT *
                                                                FROM datasci.property_transaction_events
                                                                WHERE type IN ('PropertyTransactionDeletedEvent'))b 
                                                            ON a.property_transaction_id = b.property_transaction_id
                                                         LEFT JOIN ( SELECT *
                                                                FROM prod.events
                                                                WHERE type IN ('GmailUnlinkedEvent'))c
                                                            ON a.acting_user_id = c.acting_user_id
                                                        WHERE b.property_transaction_id IS NULL
                                                            AND c.acting_user_id IS NULL")

    nrow(property_transactions)

    #DeDupe Property Transaction Events
    dupe_check <- duplicated(property_transactions[,'property_transaction_id'])
    sum(dupe_check)
    property_transactions <- property_transactions[!dupe_check,]
    nrow(property_transactions)
    dupe_check <- duplicated(property_transactions[,c('acting_user_id','address','created_at')])
    sum(dupe_check)
    property_transactions <- property_transactions[!dupe_check,]

    #Merge with complexity data
    complexity_index <- merge(property_transactions, complexity, by = 'property_transaction_id', all.x = T) 
    nrow(complexity_index)
    dupe_check <- duplicated(complexity_index[,'property_transaction_id'])
    sum(dupe_check)

    #Check column missings
    #check <- apply(complexity_confirmed,2,function(x){sum(is.na(x))})

    #Define date of product change tracking label deletion
    pre_label_del_date <- as.Date(complexity_index$created_at) < '2017-01-06'
    #Tag transactions with 0 emails
    no_emails <- is.na(complexity_index$email_count)

    #Drop records with no emails before label deletion recording date AND recods with negative email_counts
    complexity_index <- complexity_index[!(no_emails&pre_label_del_date),]
    complexity_index <- complexity_index[!complexity_index$email_count<0,]

    #Replace NAs with 0s
    complexity_index[, c('email_count','contact_count','document_count')][is.na(complexity_index[, c('email_count','contact_count','document_count')])] <- 0

    #Compute Percentiles
    complexity_index$pct_email <- ecdf(complexity_index$email_count)(complexity_index$email_count)
    complexity_index$pct_document <- ecdf(complexity_index$document_count)(complexity_index$document_count)
    complexity_index$pct_contact <- ecdf(complexity_index$contact_count)(complexity_index$contact_count)

    #Write data to table
    dbWriteTable(connection, c('datasci_projects','complexity_confirmed'), value = complexity_index, overwrite = TRUE, append = FALSE, row.names = FALSE)
}






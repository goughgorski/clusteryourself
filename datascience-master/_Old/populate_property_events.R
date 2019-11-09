
#Clear Environment
rm(list=ls())

#Load libraries
source('~/Development/Workspace/datasci/functions/load_libraries.R')
load_libraries('~/Development/Workspace/datasci/functions/')

#Set Seed
set.seed(123456789)

#Connect to Heroku PSQL Database
connection <- heroku_connect_app_db('amitree')
#Connect to Local PSQL Database
local_connection <- local_db_conect('development')

#Check to see if PropertyEvents Table Exists
prop_table_exists <- dbExistsTable(local_connection, c("datasci","property_events"))

#Get max(event_id) from PropertyEvents
if (prop_table_exists){
  max_id <- dbGetQuery(local_connection, "SELECT MAX(id) 
                                        FROM datasci.property_events 
                     WHERE type IN ('PropertyTransactionConfirmedEvent'
                     ,'PropertyTransactionDeletedEvent'
                     ,'PropertyTransactionManuallyCreatedEvent'
                     ,'PropertyTransactionRejectedEvent')"
                  )
  } else {max_id <- 0 }

#Query to get new data for PropertyEvents Table
data <- dbGetQuery(connection, paste("SELECT * 
                                      FROM events 
                                      WHERE type IN ('PropertyTransactionConfirmedEvent'
                                                      ,'PropertyTransactionDeletedEvent'
                                                      ,'PropertyTransactionManuallyCreatedEvent'
                                                      ,'PropertyTransactionRejectedEvent')
                                     AND id >", max_id)
                  )

#Dump columns with no data
data <- data[,!(apply(data,2,function(x)all(is.na(x))))]

#Convert Factors to Characters
i <- sapply(data,is.factor)
data[i] <- lapply(data[i], as.character)

#Extract fields from event_data
  #property_transaction_id
  data$property_transaction_id <- regexpr('property_transaction_id: [[:digit:]]+',data$event_data)
  data$property_transaction_id <- substr(data$event,data$property_transaction_id,data$property_transaction_id + attr(data$property_transaction_id,"match.length"))
  data$property_transaction_id <- gsub('property_transaction_id: |\n', '', data$property_transaction_id)
  #address
  data$address <- regexpr('address: [^\n]+',data$event_data)
  data$address <- substr(data$event,data$address,data$address + attr(data$address,"match.length"))
  data$address <- gsub('address: |\n', '', data$address)
  #street_address
  data$street_address <- regexpr('street_addr[_standard]*: [^,\n]+',data$event_data)
  data$street_address <- substr(data$event,data$street_address,data$street_address + attr(data$street_address,"match.length"))
  data$street_address <- gsub('street_addr[_standard]*: |\n', '', data$street_address)
  #Parse address
  parse_address <- strsplit(data$address, split = ',')
  city <- ldply(lapply(parse_address, function(x) {x[length(x)-1]}),rbind)
  city <- as.character(city[,1])
  state_zip <- ldply(as.character(lapply(parse_address, function(x) {x[length(x)]}),rbind))
  state <- substr(state_zip[,1], 2, 3)
  zip <- substr(state_zip[,1], 5, 9)
  data <- cbind(data, city, state, zip)
  #external_account_id
  data$external_account_id <- regexpr('external_account_id: [[:digit:]]+',data$event_data)
  data$external_account_id <- substr(data$event,data$external_account_id,data$external_account_id + attr(data$external_account_id,"match.length"))
  data$external_account_id <- gsub('external_account_id: |\n', '', data$external_account_id)
  #originating_message_id
  data$originating_message_id <- regexpr('originating_message_id: [[:alnum:]]+',data$event_data)
  data$originating_message_id <- substr(data$event,data$originating_message_id,data$originating_message_id + attr(data$originating_message_id,"match.length"))
  data$originating_message_id <- gsub('originating_message_id: |\n', '', data$originating_message_id)
  #email_classification_score
  data$email_classification_score <- regexpr('email_classification_score: !ruby/object:BigDecimal [[:digit:][:punct:]]+',data$event_data)
  data$email_classification_score <- substr(data$event,data$email_classification_score,data$email_classification_score + attr(data$email_classification_score,"match.length"))
  data$email_classification_score <- gsub('email_classification_score: !ruby/object:BigDecimal [[:digit:]]+:|\n', '', data$email_classification_score)
  data$email_classification_score <- as.numeric(data$email_classification_score)
  
  #Add email_classification_version HERE

  #deletion_method
  data$deletion_method <- regexpr('deletion_method: [[:alpha:]_]+',data$event_data)
  data$deletion_method <- substr(data$event,data$deletion_method,data$deletion_method + attr(data$deletion_method,"match.length"))
  data$deletion_method <- gsub('deletion_method: |\n', '', data$deletion_method)
  #rejection_reason
  data$rejection_reason <- regexpr('rejection_reason: [-[:alpha:]]+',data$event_data)
  data$rejection_reason <- substr(data$event,data$rejection_reason,data$rejection_reason + attr(data$rejection_reason,"match.length"))
  data$rejection_reason <- gsub('rejection_reason: |\n', '', data$rejection_reason)
  #rejection_notes
  data$rejection_notes <- regexpr('rejection_notes: [[:print:]]+',data$event_data)
  data$rejection_notes <- substr(data$event,data$rejection_notes,data$rejection_notes + attr(data$rejection_notes,"match.length"))
  data$rejection_notes <- gsub('rejection_notes: |\n', '', data$rejection_notes)
  
#Tag Transaction Outcomes
data$two_class <- ifelse(data$type == 'PropertyTransactionConfirmedEvent',1,
                         ifelse(data$type == 'PropertyTransactionRejectedEvent'& data$rejection_reason == 'not-a-transaction',0,NA))
#Write Tables
#property_transactions
if (prop_table_exists) { 
  append = TRUE
  overwrite = FALSE
  colnames <- dbGetQuery(local_connection, "SELECT column_name 
                         FROM information_schema.columns 
                         WHERE table_name = 'property_events'"
  )
  if ((colnames != colnames(data)) & max_id == 0) { 
    overwrite = TRUE
    append = FALSE
  }
  if ((colnames != colnames(data)) & max_id >0) { 
    stop(print('Processed table structure has changed but existing table contains data.
               Drop table and rerun, or adjust processing to fit existing structure.'))	
  }
  } else {
    append = FALSE
    overwrite = FALSE
}

dbWriteTable(local_connection, c('datasci','property_events'), value = data, overwrite = overwrite, append = append, row.names = FALSE)





#Clear Environment
rm(list=ls())

#Load libraries
source('~/workspace/datascience/functions/load_libraries.R')
load_libraries('~/workspace/datascience/functions/')

#Set Seed
set.seed(123456789)

#Connect to Local PSQL Database
print(noquote("Connecting to local PostgreSQL Database"))
local_connection <- local_db_conect('development')

#Check to see if Property Transactions Table Exists and has data
prop_table_exists <- dbExistsTable(local_connection, c("datasci","property_transactions_ds"))

if (!prop_table_exists) {
  print(noquote("No property transactions table in Data Science found."))
  count <- 0
}

if (prop_table_exists){
  count <- dbGetQuery(local_connection,"SELECT COUNT(*)
                                        FROM datasci.property_transactions_ds")
  print(noquote(paste(count, "records found in property transactions table.")))  
}

#Get max(event_id) from Property Transactions 
if (count > 0){
  max_id <- dbGetQuery(local_connection, "SELECT MAX(id) 
                                        FROM datasci.property_transactions_ds 
                     WHERE type IN ('PropertyTransactionConfirmedEvent'
                     ,'PropertyTransactionDeletedEvent'
                     ,'PropertyTransactionManuallyCreatedEvent'
                     ,'PropertyTransactionRejectedEvent'
                     ,'PropertyTransactionAutomaticallyCreatedEvent')"
                  )
  } else {max_id <- 0 }

#Query to get new data for Property Transactions  Table
print(noquote("Getting new data from production events table"))
data <- dbGetQuery(local_connection, paste("SELECT * 
                                      FROM public.events 
                                      WHERE type IN ('PropertyTransactionConfirmedEvent'
                                                      ,'PropertyTransactionDeletedEvent'
                                                      ,'PropertyTransactionManuallyCreatedEvent'
                                                      ,'PropertyTransactionRejectedEvent'
                                                      ,'PropertyTransactionAutomaticallyCreatedEvent')
                                     AND id >", max_id)
                  )

print(noquote(paste(nrow(data), "new records found to be processed.")))  

table(data$type)

#Check if any new data
if (nrow(data)==0) {
      stop(print('No new event data found.')) 
}

#Dump columns with no data
data <- data[,!(apply(data,2,function(x)all(is.na(x))))]

#Convert Factors to Characters
i <- sapply(data,is.factor)
data[i] <- lapply(data[i], as.character)

#Extract fields from event_data and reshape
print(noquote("Performing ETL on event data"))
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
  #Latitude
  data$lat <- regexpr('latitude: [-[:digit:][:punct:]]+',data$event_data)
  data$lat <- substr(data$event_data,data$lat,data$lat + attr(data$lat,"match.length"))
  data$lat <- gsub('latitude: |\n', '', data$lat)
  data$lat <- as.numeric(data$lat)  
  #Longitude
  data$lon <- regexpr('longitude: [-[:digit:][:punct:]]+',data$event_data)
  data$lon <- substr(data$event_data,data$lon,data$lon + attr(data$lon,"match.length"))
  data$lon <- gsub('longitude: |\n', '', data$lon)
  data$lon <- as.numeric(data$lon)  
  #external_account_id
  data$external_account_id <- regexpr('external_account_id: [[:digit:]]+',data$event_data)
  data$external_account_id <- substr(data$event_data,data$external_account_id,data$external_account_id + attr(data$external_account_id,"match.length"))
  data$external_account_id <- gsub('external_account_id: |\n', '', data$external_account_id)
  #originating_message_id
  data$originating_message_id <- regexpr('originating_message_id: [[:alnum:]]+',data$event_data)
  data$originating_message_id <- substr(data$event_data,data$originating_message_id,data$originating_message_id + attr(data$originating_message_id,"match.length"))
  data$originating_message_id <- gsub('originating_message_id: |\n', '', data$originating_message_id)
  #email_classification_score
  data$email_classification_score <- regexpr('email_classification_score: !ruby/object:BigDecimal [[:digit:][:punct:]]+',data$event_data)
  data$email_classification_score <- substr(data$event_data,data$email_classification_score,data$email_classification_score + attr(data$email_classification_score,"match.length"))
  data$email_classification_score <- gsub('email_classification_score: !ruby/object:BigDecimal [[:digit:]]+:|\n', '', data$email_classification_score)
  data$email_classification_score <- as.numeric(data$email_classification_score)  
  #Add email_classification_version HERE
  data$email_classification_version <- regexpr('email_classification_version: [[:digit:]]+', data$event_data)
  data$email_classification_version <- substr(data$event_data,data$email_classification_version,data$email_classification_version + attr(data$email_classification_version,"match.length"))
  data$email_classification_version <- gsub('email_classification_version: |\n','', data$email_classification_version)
  data$email_classification_version <- as.numeric(data$email_classification_version)
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

#Factor to Character
i <- sapply(data,is.factor)
data[i] <- lapply(data[i], as.character)

#Check duplicates
  #user - day - address - event dupes
  print(noquote("Cleaning Duplicates"))
  data$day <- substr(data$created_at,1,10)
  dupes <- data[duplicated(data[,c('external_account_id', 'day', 'address', 'type')]),!(colnames(data) %in% 'day')]
  print(noquote(paste(nrow(dupes), "user - day - address - event type duplicates found. These will not be cleaned.")))
  #Write Dupes
  if (max_id == 0) {
    dbGetQuery(local_connection,"DROP TABLE IF EXISTS datasci_projects.dupes_user_day_address_event") 
    overwrite = FALSE
    append = FALSE
    } else {
    overwrite = FALSE
    append = TRUE
    }
  dbWriteTable(local_connection, c('datasci_projects','dupes_user_day_address_event'), value = dupes, overwrite = overwrite, append = append, row.names = FALSE)

  #property_transaction_id - event dupes
  dupes <- data[duplicated(data[,c('property_transaction_id', 'type')]),]
  print(noquote(paste(nrow(dupes), "property_transaction_id - event type duplicates found. These will be cleaned.")))
  #Write Dupes
  if (max_id == 0) {
    dbGetQuery(local_connection,"DROP TABLE IF EXISTS datasci_projects.dupes_property_transaction_events") 
    overwrite = FALSE
    append = FALSE
    } else {
    overwrite = FALSE
    append = TRUE
    }
  dbWriteTable(local_connection, c('datasci_projects','datasci_projects.dupes_property_transaction_events'), value = dupes, overwrite = overwrite, append = append, row.names = FALSE)

  #Dedupe
  data <- data[!duplicated(data[,c('property_transaction_id', 'type')]),!(colnames(data) %in% 'day')]

#Split Data
print(noquote("Splitting data."))
data_man_created <- data[data$type == 'PropertyTransactionManuallyCreatedEvent', ]
data_auto_created <- data[data$type == 'PropertyTransactionAutomaticallyCreatedEvent',]  
data_confirmed <- data[data$type == 'PropertyTransactionConfirmedEvent', ]  
data_rejected <- data[data$type == 'PropertyTransactionRejectedEvent', ] 
data_del <- data[data$type == 'PropertyTransactionDeletedEvent', ]
print(noquote(paste(nrow(data_man_created)+nrow(data_auto_created)+nrow(data_confirmed)+nrow(data_rejected)+nrow(data_del)-nrow(data),
                    'unaccounted for event types in data during split.')))

#Dump columns with no data
data_man_created <- dump_blank_columns(data_man_created)
data_auto_created <- dump_blank_columns(data_auto_created)
data_confirmed <- dump_blank_columns(data_confirmed)
data_rejected <- dump_blank_columns(data_rejected)
data_del <- dump_blank_columns(data_del)

#Combine data
  #Find Txs that have been both confirmed AND rejected
  property_transactions_con_rej <- as.data.frame(intersect(data_confirmed$property_transaction_id, data_rejected$property_transaction_id))
  colnames(property_transactions_con_rej) <- 'property_transaction_id'
  print(noquote(paste(nrow(property_transactions_con_rej), "transactions found to be both confirmed and rejected.")))
  #Write Dupes
  if (max_id == 0) {
    dbGetQuery(local_connection,"DROP TABLE IF EXISTS datasci_projects.dupes_property_transaction_confirmed_and_rejected") 
    overwrite = FALSE
    append = FALSE
    } else {
    overwrite = FALSE
    append = TRUE
    }
  dbWriteTable(local_connection, c('datasci_projects','dupes_property_transaction_confirmed_and_rejected'), value = property_transactions_con_rej, overwrite = overwrite, append = append, row.names = FALSE)

  #Remove these from confirmed
  data_confirmed <- data_confirmed[!(data_confirmed$property_transaction_id %in% property_transactions_con_rej$property_transaction_id),]
  #Union results
  data_conrej <- rbind.fill(data_confirmed,data_rejected)
  nrow(data_conrej) - (nrow(data_confirmed) + nrow(data_rejected))
  names(data_conrej)[names(data_conrej) == "type"] <- "confirmed_or_rejected"

  #Back-propogate auto-created transactions
  data_auto_created_old <- intersect(setdiff(data_conrej$property_transaction_id, data_auto_created$property_transaction_id),data_conrej$property_transaction_id)
  data_auto_created_old <- data_conrej[!(data_conrej$property_transaction_id %in% data_auto_created_old),colnames(data_conrej) %in% colnames(data_auto_created)]
  data_auto_created_old$type <- 'PropertyTransactionAutomaticallyCreatedEvent'
  data_auto_created_con <- intersect(data_conrej$property_transaction_id, data_auto_created$property_transaction_id)

  #Check created transactions
  property_transactions_man_auto <- intersect(data_man_created$property_transaction_id, data_auto_created$property_transaction_id)
  print(noquote(paste(length(property_transactions_con_rej), "transactions found to be both confirmed and rejected.")))  









#Merge
data_wide <- merge(data_created,data_conrej,by="property_transaction_id",all = T)
#nrow(data_created)+nrow(data_conrej)+nrow(data_del)-nrow(data)
nrow(data_wide)

#Clean white spaces in character vectors
i <- sapply(data,is.factor)
data[i] <- lapply(data[i], function(x){gsub("^\\s+|\\s+$", "", x)})
data[i] <- lapply(data[i], function(x){gsub("\\s+", " ", x)})

#Create place holders for census data
census <- data.frame(gid = integer(nrow(data)),
                    statefp = character(nrow(data)),
                    countyfp = character(nrow(data)),
                    tractce = character(nrow(data)),
                    tract_id = character(nrow(data)),
                    name = character(nrow(data)),
                    namelsad = character(nrow(data)))
census[census == 0 | census == ''] <- NA
data <- cbind(data, census)

#Write Tables
#property_transactions
if (prop_table_exists) { 
  append = TRUE
  overwrite = FALSE
  colnames <- dbGetQuery(local_connection, "SELECT column_name 
                         FROM information_schema.columns 
                         WHERE table_name = 'property_transactions_ds'
                          AND column_default IS NULL"
  )
  if (!setequal(colnames(data),colnames[,1]) & max_id == 0) { 
    overwrite = TRUE
    append = FALSE
  }
  if (!setequal(colnames(data),colnames[,1]) & max_id > 0) { 
    stop(print('Processed table structure has changed but existing table contains data. Drop table and rerun, or adjust processing to fit existing structure.'))	
  }
  } else {
    append = FALSE
    overwrite = FALSE
}

#If events data doesn't contain any lat/lons (ie addition of this data point hasn't happened), get address data from address table
if (all(is.na(data$lat))) {
  print(noquote("No Lat Lon found - backfilling with address data"))
  
  #Get address data to populate lat lons for old data
  address_data <- dbGetQuery(local_connection, paste("SELECT * 
                                        FROM public.addresses"))
  ids <- c('street_addr', 'city', 'state', 'zipcode')
  names(address_data)[names(address_data) == "created_at"] <- "add_created_at"    
  address_data <- address_data[,c(ids, 'country_code', 'latitude', 'longitude', 'add_created_at')]

  #Convert Factors to Characters
  i <- sapply(address_data,is.factor)
  address_data[i] <- lapply(address_data[i], as.character)

  #Mege in address data
  full_data <- merge(data, address_data, by = ids, all.x = T)

  #Sort and tag 
  full_data <- full_data[order(full_data$id, full_data$street_addr, full_data$city, full_data$state, full_data$zipcode, full_data$latitude),]
  idsvec <- with(full_data, paste(id, street_addr, city, state, zipcode, sep = ".", collapse = NULL))
  full_data$obsnum <- sequence(rle(idsvec)$lengths)

  full_data$lat <- ifelse(is.na(full_data$lat), full_data$latitude, full_data$lat)
  full_data$lon <- ifelse(is.na(full_data$lon), full_data$longitude, full_data$lon)

  data <- full_data[full_data$obsnum == 1, !colnames(full_data) %in% c('obsnum','add_created_at', 'latitude', 'longitude', 'country_code')]
}

print(noquote("Writing new event data"))

dbWriteTable(local_connection, c('datasci','property_transactions_ds'), value = data, overwrite = overwrite, append = append, row.names = FALSE)

print(noquote("Cleaning Duplicates"))
dupes <- data[duplicated(data[,c('address', 'type')]),]
dupe_count <- nrow(dupes)
print(noquote(paste(dupe_count, "duplicates found")))

if (max_id == 0) {
  dbGetQuery(local_connection,"DROP TABLE IF EXISTS datasci_projects.property_event_dupes") 
  overwrite = FALSE
  append = FALSE
  }

dbWriteTable(local_connection, c('datasci_projects','property_event_dupes'), value = dupes, overwrite = overwrite, append = append, row.names = FALSE)
dbGetQuery(local_connection, paste("DELETE
                                    FROM datasci.property_transactions_ds 
                                    WHERE rid IN (
                                      SELECT rid
                                      FROM (
                                        SELECT rid
                                          , ROW_NUMBER() OVER(PARTITION BY property_transaction_id, type ORDER BY id ASC) count
                                          , property_transaction_id
                                          , type
                                        FROM datasci.property_transactions_ds)a
                                      WHERE count >1)"))

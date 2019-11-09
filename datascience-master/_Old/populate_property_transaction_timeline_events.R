#Function to populate DS property transactions timeline events table
pop_property_tx_timelines_ds <- function(connection) {

  #Check to see if Property Transactions Table Exists and has data
  timeline_table_exists <- dbExistsTable(connection, c("datasci","property_transaction_timelines_ds"))

  if (!timeline_table_exists) {
    print(noquote("No property transaction timelines table in Data Science found."))
    count <- 0
  }

  if (timeline_table_exists){
    count <- dbGetQuery(connection,"SELECT COUNT(*)
                                          FROM datasci.property_transaction_timelines_ds")
    print(noquote(paste(count, "records found in property transaction timelines table.")))  
  }

  #Get max(event_id) from Property Transaction Timelines
  if (count > 0){
    max_id <- dbGetQuery(connection, "SELECT MAX(id) 
                                          FROM datasci.property_transaction_timelines_ds 
                       WHERE (type LIKE ('%Timeline%') 
                                  OR type = 'PropertyTransactionSharedEvent')"
                    )
    } else {max_id <- 0 }

  #Query to get new data for Property Transaction Timelines Table
  print(noquote("Getting new data from production events table"))
  data <- dbGetQuery(connection, paste("SELECT * 
                                        FROM public.events 
                                        WHERE (type LIKE ('%Timeline%') 
                                          OR type = 'PropertyTransactionSharedEvent')
                                        AND id >", max_id)
                    )

  #Check if any new data
  if (nrow(data)==0) {
        stop(print('No new event data found.')) 
  }

  print(noquote(paste(nrow(data), "new records found to be processed.")))  

  table(data$type)

  #Dump columns with no data
  data <- dump_blank_columns(data)

  #Convert Factors to Characters
  i <- sapply(data,is.factor)
  data[i] <- lapply(data[i], as.character)

  #Extract fields from event_data
  print(noquote("Performing ETL on timeline event data"))
    
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
    #transaction_timeline_event_id
    data$transaction_timeline_event_id <- regexpr('transaction_timeline_event_id: [[:digit:]]+',data$event_data)
    data$transaction_timeline_event_id <- substr(data$event_data,data$transaction_timeline_event_id,data$transaction_timeline_event_id + attr(data$transaction_timeline_event_id,"match.length"))
    data$transaction_timeline_event_id <- gsub('transaction_timeline_event_id: |\n', '', data$transaction_timeline_event_id)
    #transaction_timeline_event_description
    data$transaction_timeline_event_description <- regexpr('transaction_timeline_event_description: [^\n]+',data$event_data)
    data$transaction_timeline_event_description <- substr(data$event_data,data$transaction_timeline_event_description,data$transaction_timeline_event_description + attr(data$transaction_timeline_event_description,"match.length"))
    data$transaction_timeline_event_description <- gsub('transaction_timeline_event_description: |\n', '', data$transaction_timeline_event_description)
    #transaction_timeline_event_date
    data$transaction_timeline_event_date <- regexpr('transaction_timeline_event_date: [-[:digit:]]+',data$event_data)
    data$transaction_timeline_event_date <- substr(data$event_data,data$transaction_timeline_event_date,data$transaction_timeline_event_date + attr(data$transaction_timeline_event_date,"match.length"))
    data$transaction_timeline_event_date <- gsub('transaction_timeline_event_date: |\n', '', data$transaction_timeline_event_date)  
    #transaction_timeline_event_start_at
    data$transaction_timeline_event_start_at <- regexpr('transaction_timeline_event_start_at: [-[:digit:]: ]+',data$event_data)
    data$transaction_timeline_event_start_at <- substr(data$event_data,data$transaction_timeline_event_start_at,data$transaction_timeline_event_start_at + attr(data$transaction_timeline_event_start_at,"match.length")-1)
    data$transaction_timeline_event_start_at <- gsub('transaction_timeline_event_start_at: |\n', '', data$transaction_timeline_event_start_at)
    #transaction_timeline_event_end_at
    data$transaction_timeline_event_end_at <- regexpr('transaction_timeline_event_end_at: [-[:digit:]: ]+',data$event_data)
    data$transaction_timeline_event_end_at <- substr(data$event_data,data$transaction_timeline_event_end_at,data$transaction_timeline_event_end_at + attr(data$transaction_timeline_event_end_at,"match.length")-1)
    data$transaction_timeline_event_end_at <- gsub('transaction_timeline_event_end_at: |\n', '', data$transaction_timeline_event_end_at)
    #transaction_timeline_event_due_date_time
    data$transaction_timeline_event_due_date_time <- regexpr('transaction_timeline_event_due_date_time: [[:alnum:]]+',data$event_data)
    data$transaction_timeline_event_due_date_time <- substr(data$event_data,data$transaction_timeline_event_due_date_time,data$transaction_timeline_event_due_date_time + attr(data$transaction_timeline_event_due_date_time,"match.length"))
    data$transaction_timeline_event_due_date_time <- gsub('transaction_timeline_event_due_date_time: |\n', '', data$transaction_timeline_event_due_date_time)
    #transaction_timeline_event_status
    data$transaction_timeline_event_status <- regexpr('transaction_timeline_event_status: [[:alpha:]]+',data$event_data)
    data$transaction_timeline_event_status <- substr(data$event_data,data$transaction_timeline_event_status,data$transaction_timeline_event_status + attr(data$transaction_timeline_event_status,"match.length"))
    data$transaction_timeline_event_status <- gsub('transaction_timeline_event_status: |\n', '', data$transaction_timeline_event_status)
    #transaction_timeline_event_status_color
    data$transaction_timeline_event_status_color <- regexpr('transaction_timeline_event_status_color: [[:digit:]]+',data$event_data)
    data$transaction_timeline_event_status_color <- substr(data$event_data,data$transaction_timeline_event_status_color,data$transaction_timeline_event_status_color + attr(data$transaction_timeline_event_status_color,"match.length"))
    data$transaction_timeline_event_status_color <- gsub('transaction_timeline_event_status_color: |\n', '', data$transaction_timeline_event_status_color)

  #Clean white spaces in character vectors
  i <- sapply(data,is.factor)
  data[i] <- lapply(data[i], function(x){gsub("^\\s+|\\s+$", "", x)})
  data[i] <- lapply(data[i], function(x){gsub("\\s+", " ", x)})

  #Write Tables
  #property_transactions
  if (timeline_table_exists) { 
    append = TRUE
    overwrite = FALSE
    colnames <- dbGetQuery(connection, "SELECT column_name 
                           FROM information_schema.columns 
                           WHERE table_name = 'property_transaction_timelines_ds'
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
    address_data <- dbGetQuery(connection, paste("SELECT * 
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

  print(noquote("Writing new timeline event data"))

  dbWriteTable(connection, c('datasci','property_transaction_timelines_ds'), value = data, overwrite = overwrite, append = append, row.names = FALSE)
}

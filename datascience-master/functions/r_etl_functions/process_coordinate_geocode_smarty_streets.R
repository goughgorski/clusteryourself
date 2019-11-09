
process_coordinate_geocode_smarty_streets <- function(connection, batchsize = 100) {

  #Set Smarty Streets Keys
  smartystreets_base_url <- 'https://us-street.api.smartystreets.com/street-address'
  smartystreets_base_query <- list("auth-id" = get_required_env("SMARTYSTREETS_API_AUTH_ID"),
                                 "auth-token" = get_required_env("SMARTYSTREETS_API_AUTH_TOKEN"))

  if(!exists('smartystreets_base_query')) {stop(print("No access to needed geocoding service connection information."))}
  
  #Get data to geocode
  data <- dbGetQuery(connection, "SELECT id, address FROM datasci.vw_property_geocode_queue WHERE needs_coordinate_geocode")
  
  #Check if any new data
  if (nrow(data)==0) {
    stop(print('No properties in geocoding queue.')) 
  }

  data <- split(data, data[,'id'])

  coords <- vector("list", round(length(data) / batchsize))

  print(noquote(paste(length(data), "properties found to be processed in", length(coords), "batches")))  

  data_index <- 1
  batch_index <- 1

  #Batch and Fetch Coordinates
  while (batch_index <= length(coords)) {
    cat('\r', batch_index/length(coords))
    batch <- data[data_index:(data_index + batchsize - 1)]
    #if its the last batch truncate null list elements
    if (batch_index == length(coords)) {
      batch <- batch[!sapply(names(batch),is.na)]
    }
    coords[[batch_index]] <- lapply(seq_along(batch), function(x){
      response <- fromJSON(modify_url(smartystreets_base_url, query = c(smartystreets_base_query, input_id = batch[[x]]$id, street = batch[[x]]$address)))
      if (length(response) == 0) {
        output <- list(id = batch[[x]]$id, lat = NA, lon = NA)
        } else {output <- list(id = response$input_id, lat = response$metadata$latitude, lon = response$metadata$longitude)
      }
      output 
      }
    )
    data_index <- data_index + batchsize
    batch_index <- batch_index + 1
  }

  #Segment failed results & remove empty elements
  outcome <- lapply(coords,function(l){sapply(l,function(x){ifelse(is.null(x$lat),TRUE,is.na(x$lat))})})
  successes <- lapply(seq_along(outcome),function(x){coords[[x]][!outcome[[x]]]})
  successes <- successes[lapply(successes,length)>0]
  failures <- lapply(seq_along(outcome),function(x){coords[[x]][outcome[[x]]]})
  failures <- failures[lapply(failures,length)>0]

  print(noquote("Completed coordinate retrieval, upating properties table"))
  #Update successes
  update_log <- lapply(seq_along(successes),function(s) {lapply(seq_along(successes[[s]]), function(x) {
    update_params <- list(lat = successes[[s]][[x]]$lat, lon = successes[[s]][[x]]$lon, id = successes[[s]][[x]]$id)
    dbExecute(connection, "UPDATE datasci.properties SET lat=$1, lon=$2 WHERE id = $3", update_params)
    })})

  #Write Failures
  failure_ids <- unlist(lapply(failures,function(f){lapply(f,function(x){x$id})}))
  failure_data <- cbind.data.frame(id = failure_ids, function_call = rep(deparse(sys.call()), length(failure_ids)))
  
  print(noquote(paste('Could not find coordinates for', nrow(failure_data), 'properties')))
  if (nrow(failure_data)>0) {dbWriteTable(connection, c('datasci','quarantined_property_geocode_failures'), value = failure_data, overwrite = FALSE, append = TRUE, row.names = FALSE)}

}

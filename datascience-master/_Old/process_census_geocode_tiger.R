process_census_geocode <- function(connection, batchsize = 1000) {

  #Set TIGERWeb Keys
  tigerweb_base_url <- 'https://tigerweb.geo.census.gov/arcgis/rest/services/TIGERweb/tigerWMS_Current/MapServer/8/query'
  key <- get_required_env("TIGER_API_KEY")
  tigerweb_base_query <- list(outFields = '*',
                            geometryType = 'esriGeometryPoint',
                            inSR = '4326',
                            returnGeometry = 'false',
                            f = 'json')

  if(!exists('tigerweb_base_query')|!exists(key)) {stop(print("No access to needed geocoding service connection information."))}

  data <- dbGetQuery(connection, "SELECT id, lat, lon FROM datasci.vw_property_geocode_queue WHERE needs_census_geocode AND NOT needs_coordinate_geocode")
  
  #Check if any new data
  if (nrow(data)==0) {
    stop(print('No properties in geocoding queue.')) 
  }

  data <- split(data, data[,'id'])

  censusinfo <- vector("list", round(length(data) / batchsize))

  print(noquote(paste(length(data), "properties found to be processed in", length(censusinfo), "batches")))  

  data_index <- 1
  batch_index <- 1

  #Batch and Fetch Census Data
  while (batch_index <= length(censusinfo)) {
    cat('\r', batch_index/length(censusinfo))
    batch <- data[data_index:(data_index + batchsize - 1)]
    #if its the last batch truncate null list elements
    if (batch_index == length(censusinfo)) {
      batch <- batch[!sapply(names(batch),is.na)]
    }
    censusinfo[[batch_index]] <- lapply(seq_along(batch), function(x){
      response <- fromJSON(modify_url(tigerweb_base_url, query = c(tigerweb_base_query, geometry = paste(x$lat, ",", x$lon, sep=""), key = key)))
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






      response <- fromJSON(modify_url(tigerweb_base_url, query = c(tigerweb_base_query, geometry = paste(data[[1]]$lat, ",", data[[1]]$lon, sep=""))))


  for(row in 1:nrow(geocoder_queue)) {
    id <- geocoder_queue$id[row]
    latitude <- geocoder_queue$lat[row]
    longitude <- geocoder_queue$lon[row]
    geometry <- paste(longitude, ",", latitude, sep="")

    print(noquote(c(id, latitude, longitude)))

    url <- modify_url(tigerweb_base_url, query = c(tigerweb_base_query, geometry = geometry))
    response <- fromJSON(url)
    attributes <- response$features$attributes
    if(is.null(attributes)) {
      print(noquote("No matches found"))
    } else {
      update_params <- list(gid = attributes$OBJECTID[1],
                            statefp = attributes$STATE[1],
                            countyfp = attributes$COUNTY[1],
                            tractce = attributes$GEOID[1],
                            tract_id = attributes$TRACT[1],
                            name = attributes$BASENAME[1],
                            namelsad = attributes$NAME[1],
                            id = id)
      str(update_params, no.list = TRUE)
      dbExecute(connection,
                "UPDATE datasci.properties SET gid=$1, statefp=$2, countyfp=$3, tractce=$4, tract_id=$5, name=$6, namelsad=$7 WHERE id = $8",
                update_params)
    }
  }
}

https://tigerweb.geo.census.gov/arcgis/rest/services/TIGERweb/tigerWMS_Current/MapServer/8/query?where=&text=&objectIds=&time=&geometry=-120.64516%20+35.25831&geometryType=esriGeometryPoint&inSR=&spatialRel=esriSpatialRelContains&relationParam=&outFields=*&returnGeometry=false&returnTrueCurves=false&maxAllowableOffset=&geometryPrecision=&outSR=&returnIdsOnly=false&returnCountOnly=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&returnZ=false&returnM=false&gdbVersion=&returnDistinctValues=false&resultOffset=&resultRecordCount=&queryByDistance=&returnExtentsOnly=false&datumTransformation=&parameterValues=&rangeValues=&f=pjson
https://tigerweb.geo.census.gov/arcgis/rest/services/TIGERweb/tigerWMS_Current/MapServer/8/query%20?outFields=*%20&geometryType=esriGeometryPoint%20&geometry=35.25831,-120.64516%20&inSR=4326%20&f=json



https://tigerweb.geo.census.gov/arcgis/rest/services/TIGERweb/tigerWMS_Current/MapServer/8/query?geometry=35.25831%2C-120.64516&geometryType=esriGeometryPoint&spatialRel=esriSpatialRelContains&outFields=*&returnGeometry=false&returnTrueCurves=false&returnIdsOnly=false&returnCountOnly=false&returnZ=false&returnM=false&returnDistinctValues=false&returnExtentsOnly=false&f=pjson
https://tigerweb.geo.census.gov/arcgis/rest/services/TIGERweb/tigerWMS_Current/MapServer/8/query?geometryType=esriGeometryPoint&spatialRel=esriSpatialRelContains&outFields=*&returnGeometry=false&returnTrueCurves=false&returnIdsOnly=false&returnCountOnly=false&returnZ=false&returnM=false&returnDistinctValues=false&returnExtentsOnly=false&f=pjson&geometry=35.25831%2C-120.64516

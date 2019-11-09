# Haversine formula (hf)
haversine_dist <- function(long1, lat1, long2, lat2, radians = FALSE) {
  R <- 6371 # Earth mean radius [km]
  if (any(is.na(long1), is.na(lat1))) {
    d <- NA
  } else {  
    if (radians == FALSE) {
      delta.long <- (deg2rad(long2) - deg2rad(long1))
      delta.lat <- (deg2rad(lat2) - deg2rad(lat1))
      a <- sin(delta.lat/2)^2 + cos(deg2rad(lat1)) * cos(deg2rad(lat2)) * sin(delta.long/2)^2
    } else {
      delta.long <- (long2 - long1)
      delta.lat <- (lat2 - lat1)
      a <- sin(delta.lat/2)^2 + cos(lat1) * cos(lat2) * sin(delta.long/2)^2
    }
    u <- 2 * asin(pmin(rep(1,length(a)),sqrt(a)))
    d = R * u
  }  
  return(d) # Distance in km
} 

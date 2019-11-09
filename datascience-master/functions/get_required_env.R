
get_required_env <- function(name) {
  value <- Sys.getenv(name)
  if(value == "") {
    stop(paste("Missing required environment variable:", name))
  }
  value
}
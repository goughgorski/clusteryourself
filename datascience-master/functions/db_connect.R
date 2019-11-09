library('RPostgreSQL')
library('httr')

db_connect <- function(envir_var = NULL) {
	
	if(is.null(envir_var)){envir_var <- 'DATABASE_URL'}
	
	url <- Sys.getenv(envir_var)
	parts <- parse_url(url)

	# loads the PostgreSQL driver
	drv <- dbDriver("PostgreSQL")

	#Establish local connection
	con <- dbConnect(drv, 
	         host     = ifelse(is.null(parts$hostname), '', parts$hostname),
	         port     = ifelse(is.null(parts$port), '', parts$port),
	         user     = ifelse(is.null(parts$username), '', parts$username),
	         password = ifelse(is.null(parts$password), '', parts$password),
	         dbname   = gsub('^/', '', parts$path)
	)

	return(con)
}


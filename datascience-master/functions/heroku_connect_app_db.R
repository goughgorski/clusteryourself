#This function connects to a Heroku Application PostgreSQL database
#Requires a user to be logged in to Heroku

heroku_connect_app_db <- function(appname) {
	
	#Fetch Host name
	DBhost <- system(paste('heroku config:get DATABASE_URL -a',appname), intern = TRUE)
	parts <- parse_url(DBhost)
	
	# loads the PostgreSQL driver
	drv <- dbDriver("PostgreSQL")
	
	# creates a connection to the postgres database
	# note that "con" will be used later in each connection to the database
	con <- dbConnect(drv, dbname = parts$path ,
                 host = parts$hostname, port = parts$port,
                 user = parts$username, password = parts$password)

    return(con)
                 	
	}


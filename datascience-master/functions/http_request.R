
# HTTP_CREDENTIALS environment variable list contains a comma-separated list of valid username:password pairs
# Should we move this out of environment and into data?
HTTP_CREDENTIALS = unlist(strsplit(Sys.getenv('HTTP_CREDENTIALS', unset='datascience:plipperoni'), ','))

# Converts the raw headers received from RServe into a list with downcased names
parse_headers <- function(raw_header) {
	header_lines <- unlist(strsplit(rawToChar(raw_header), "\n"))
	headers <- list()
	for(line in header_lines) {
		parts <- unlist(strsplit(line, ": "))
		headers[tolower(parts[1])] <- parts[2]
       	}
	headers
}

# Returns TRUE iff the request contains an Authorization header with a valid username and password
authorize <- function(headers) {
	if(!('authorization' %in% names(headers))) {
		return(FALSE)
	}
	parts <- unlist(strsplit(headers$authorization, ' '))
	if(parts[1] != 'Basic') {
		return(FALSE)
	}
	auth <- rawToChar(base64decode(parts[2]))
	auth %in% HTTP_CREDENTIALS
}

# Handle an HTTP request, ensuring that the user is authorized.
handle_request <- function(method, url, params, body, headers) {
	if(!authorize(headers)) {
		response <- list(error = 'Unauthorized', format = 'text/html', error_msg = 'WWW-Authenticate: Basic',  error_code = 401)
	}

	if(method == 'POST' && !is.null(url) && !is.null(body)) {
		#Get function call
		func <- substring(url,2)

		# if the client specifies the content-type,
		# body should be of type 'raw'
		body <- rawToChar(body)

		#validate JSON
		validation <- validate(body)

		if (validation) {

			#Convert json payload to list
			json <- fromJSON(body)

			#Construct function call
			call <- call_new(func, .args= json$payload$args)

			#Evaluate and return
			result <- eval(call)

			response <- list(toJSON(result, auto_unbox = TRUE, pretty = TRUE), "application/json")


		} else {

			print('Invalid JSON')
			print(attr(validation,"err"))
			response <- toJSON(list())

		}
	} else {

		response <- list(error = 'Not found', format = 'text/html', error_msg = NULL, error_code = 404)
	}

	return(response)

}

log_request_start <- function(method, url, params, headers) {
	remote_ip <- unlist(headers['x-forwarded-for'])
	flog.info("Processing %s %s%s%s",
	          method,
	          url,
	          if(length(params) == 0) '' else paste(' with params', toJSON(params, auto_unbox=TRUE)),
	          if(is.null(remote_ip)) '' else paste(' for', remote_ip)
	         )
}

# Request handler required by Rserve
.http.request <- function(url, raw_params, body, raw_header) {
	result <- tryCatch({
		headers <- parse_headers(raw_header)
		method <- headers['request-method']
		params <- as.list(raw_params)
		log_request_start(method, url, params, headers)
		handle_request(method, url, params, body, headers)
	}, error = function(error_condition) {
		flog.error("Error processing request: %s", error_condition)
		toJSON(list())
	})
	status <- if(class(result) == 'json' && length(fromJSON(result)) > 0) {
				200
			} else if (class(result) == 'json' && length(fromJSON(result)) == 0) {
				400
			} else {
			paste(result[4])
		}
	status_line <- if (status == 200) 'OK' else if (status == 400) 'Invalid JSON' else result[1]
	flog.info("Returning %s %s", status, status_line)
	result
}

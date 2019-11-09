

load_data <- function(connection, schema = NULL, table = NULL) {
	
	if (is.null(schema) & is.null(table)) {
		stop(print(noquote("No schema or table specified.")))
	}
	
	if (is.null(schema) & !is.null(table)) {
		stop(print(noquote(paste("No schema specified for table",table))))
	}

	if (!is.null(schema) & !is.null(table) ) {
		schema_tables <- as.vector(paste(schema, ".", table, sep =''))
		query <- paste("SELECT * FROM ", schema, ".", table, sep = '')
	}

	if (!is.null(schema) & is.null(table) ) {
		query <- paste("SELECT table_schema || '.' || table_name FROM information_schema.tables WHERE table_schema IN ('", paste(schema, collapse = "', '"), "')", sep = '')
		schema_tables <- as.vector(dbGetQuery(connection, query)[,1])
		query <- paste(rep("SELECT * FROM", length(schema_tables)), schema_tables)
	}

	pre_loaded_data <- lapply(query,function(x) {dbGetQuery(connection,x)})
	names(pre_loaded_data) <- schema_tables
	return(pre_loaded_data)
}

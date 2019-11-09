
model_storage <- function(model_obj, location_folder, model_script, strip_model_fn = strip_model, model_grain, model_response,
							model_outcome, model_type, model_active = TRUE, model_test = NA, model_test_cols = NA) {

	connection <- db_connect()
	location <- paste("amitree-datascience/models/messages/", location_folder, sep = '')
	assign(paste0(deparse(substitute(model))), model_obj)
	name <- paste0(deparse(substitute(model_obj)))
	obj_name <- name
	model_location <- paste(location, name, sep = '/')
	model_location <- paste(model_location, '.rda', sep = '')

	if (is.function(strip_model_fn)){

		print(noquote("Stripping model"))

		assign(paste(as.character(bquote(model)), '_stripped', sep = ''), strip_model_fn(model))
		name <- paste(name, '_stripped', sep = '')

		}

	print(noquote(paste("Saving ", obj_name, " model object to s3", sep = '')))

	s3save(model, bucket = location, object = paste(obj_name, ".rda", sep = ''))

	print(noquote(paste("Writing ", obj_name, " model details to datasci_modeling.models", sep = '')))

	model_data <- cbind(model_location, model_name = name, gen_script_location = model_script, grain_type = model_grain,
	response_type = model_response, outcome = model_outcome, model_type, active = model_active)
	dbWriteTable(connection, c('datasci_modeling','models'), value = as.data.frame(model_data), overwrite = FALSE, append = TRUE, row.names = FALSE)

	if (!is.na(model_test) && !is.na(model_test_cols)){

		print(noquote("Generating model threshold iterations and writing to datasci_modeling.model_threshold_iterations"))

		model_id <- dbGetQuery(connection, paste("SELECT max(id) FROM datasci_modeling.models WHERE model_location = '", model_location, "'", sep = ''))
		threshold_iterations <- thresh_iter(.0, 1, .01, model_test, model_test_cols[1], model_test_cols[2])
		threshold_iterations$model_id <- as.numeric(model_id)

		i <- sapply(threshold_iterations, is.infinite)
		threshold_iterations[i] <- NA

		dbWriteTable(connection, c('datasci_modeling','model_threshold_iterations'), value = as.data.frame(threshold_iterations), overwrite = FALSE, append = TRUE, row.names = FALSE)

		} else {print(noquote("model_test dataframe or model_test_cols not provided: model threshold iterations will not be stored."))}

	}

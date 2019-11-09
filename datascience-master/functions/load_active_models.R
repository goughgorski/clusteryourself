
load_active_models <- function(model_table, thresholds_table, model_grain_type, ensembles_table, threshold_FUN, environment) {
	model_locations <- model_table[model_table$active == TRUE & model_table$grain_type %in% model_grain_type , 'model_location']
	load_models_s3(s3_model_locations = model_locations, environment = environment)
	load_model_thresholds(thresholds_table = thresholds_table, ensembles_table = ensembles_table, FUN = threshold_FUN, environment = environment)
}
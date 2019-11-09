
load_model_thresholds <- function(thresholds_table, ensembles_table, FUN, na.rm = TRUE, environment) {
	thresh_table <- merge(thresholds_table, ensembles_table[, c('model_id', 'model_threshold_type')], by = 'model_id')
	thresh_table <- cbind(thresh_table, ldply(lapply(split(thresh_table, thresh_table[, 'model_id']), function(x) {
		data.frame(value = x[, which(colnames(x) == unique(x[, 'model_threshold_type']))])
		}), rbind))
	thresholds <- aggregate(thresh_table[,'value'], by = list(thresh_table$model_id), FUN, drop = TRUE, na.rm = na.rm)
	colnames(thresholds) <- c('model_id', 'value')
	thresholds <- merge(thresholds, ensembles_table[, c('model_id', 'model_threshold_type')], by = 'model_id')
	thresholds <- merge(thresholds, thresh_table[,c('model_id', 'model_threshold_type', 'value', 'threshold')], by = c('model_id', 'model_threshold_type', 'value'), sort = FALSE)
	thresholds <- aggregate(thresholds[,'threshold'], by = list(thresholds$model_id), min, drop = TRUE, na.rm = na.rm)
	colnames(thresholds) <- c('model_id', 'threshold')	
	thresholds <- merge(thresholds, thresh_table[,c('model_id', 'threshold', 'model_threshold_type', 'value', 'auc')], by = c('model_id', 'threshold'), sort = FALSE)	
	assign('model_thresholds', thresholds, envir = environment)
}
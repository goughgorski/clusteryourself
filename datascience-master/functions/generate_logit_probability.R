generate_logit_probability <- function(cluster, coef_table){
	tmp <- coef_table[, c('model_name', as.character(cluster))]
	tmp$score <- 1/(1 + exp(-coef_table[, as.character(cluster)]))
	tmp$cluster <- cluster
	return(tmp[, c('model_name', 'cluster', 'score')])
}
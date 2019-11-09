denormalize_data <- function(data, rowval, column_vector, value_vector, sort = TRUE) {
	formula <- reformulate(column_vector, response = value_vector)
	if (!sort) {column_order <- as.vector(data[!duplicated(data[,column_vector]), column_vector])}
	unstack <- unstack(data[,!(colnames(data) %in% rowval)], formula)
	denormalized_data <- cbind(data[!duplicated(data[,rowval]),rowval], unstack)
	colnames(denormalized_data) <- c(rowval, colnames(unstack))
	if (!sort) {denormalized_data <- denormalized_data[,c(rowval,column_order)]}
	return(denormalized_data)
}

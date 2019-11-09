#Normalize DTM for storage
normalize_data <- function(data, rowval) {
	stack <- stack(data[,!(colnames(data) %in% rowval)])	
	normalized_data <- cbind(data[rep(seq_len(nrow(as.data.frame(data[,rowval]))), sum(!colnames(data) %in% rowval)),rowval], stack[,c(2,1)]) 	
	colnames(normalized_data) <- c(rowval, 'column_name', 'value')
	return(normalized_data)
}
dump_blank_columns <- function(data) {

	data <- data[,!(apply(data,2,function(x)all(is.na(x))))]
	data <- data[,!(apply(data,2,function(x)all(x=='')))]
	return(data)
}
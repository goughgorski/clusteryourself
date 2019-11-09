strip_stm <- function(stm) {

	stm$settings$convergence <- c()
	stm$vocab <- c()
	stm$convergence <- c()
	stm$eta <- c()
	stm$invsigma <- c()
	stm$time <- c()
	stm$version <- c()

	return(stm)
}
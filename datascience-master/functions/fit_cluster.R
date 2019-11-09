fit_cluster <- function(data, scaling_table, kfit_centers){
	tmp <- sapply(seq_along(colnames(data)), function(x) {
		(data[, colnames(data)[x]] - scaling_table['mean', colnames(data)[x]])/scaling_table['sd', colnames(data)[x]]
		})
	if (nrow(data) == 1) {tmp <- as.data.frame(t(as.data.frame(tmp)))} else {tmp <- as.data.frame(tmp)}
	colnames(tmp) <- colnames(data)
	
	apply(tmp, 1, closest_cluster, kfit_centers[,colnames(data)])
}

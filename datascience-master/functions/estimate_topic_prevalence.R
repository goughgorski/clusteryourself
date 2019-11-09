estimate_topic_prevalence <- function(dtm_list, stm_list, dtm_names){
	for (i in 1:length(dtm_list)){
		tmp <- fitNewDocuments(stm_list[[i]], dtm_list[[i]])
		tmp <- as.data.frame(tmp$theta)
		colnames(tmp) <- paste('T', 1:length(colnames(tmp)), '_',
 		paste(dtm_names[[i]], '_k', length(colnames(tmp)), sep = ''), sep = '')
 		if (i == 1) {est_tp <- tmp} else {est_tp <- cbind(est_tp, tmp)}
	}
	est_tp
}
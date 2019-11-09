align_corpus <- function(dtm, vocab) {
	aligned <- alignCorpus(list(documents = readCorpus(dtm, type = 'Matrix')$documents, vocab = colnames(dtm)), vocab)
	names(aligned$documents) <- c(1:nrow(dtm))[!c(1:nrow(dtm)) %in% aligned$docs.removed]
	lapply(seq_along(c(1:nrow(dtm))), function (x) {
		if (any(aligned$docs.removed %in% x)) {
			matrix(nrow = 2, ncol = 0)} else {
			aligned$documents[[which(as.numeric(names(aligned$documents)) == x)]]}
		})
}
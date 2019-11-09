#Function for creating tokenized DTMs from a corpus using a dictionary
	tokenize_corpus <- function(ng, corpus, weighting, dictionary) {
		dict_wc <- nchar(dictionary) - nchar(gsub(" ",'',dictionary)) + 1
		dtm_list <- list()
		colname_list <- list()
		for (i in 1:length(ng)) {
			tokenizer <-  function(x) {unlist(lapply(ngrams(words(x), ng[i]), paste, collapse = " "), use.names = FALSE)}
			dtm <- as.matrix(DocumentTermMatrix(corpus, 
			                          control = list(tokenize = tokenizer, weighting = weighting,
			                                        dictionary = dictionary)))	
			colnames <- colnames(dtm)[dict_wc == ng[i]]
			dtm <- t(as.matrix(dtm[dict_wc == ng[i]]))
			colname_list[[i]] <- colnames
			dtm_list[[i]] <- dtm
		}
		dtm <- unlist(dtm_list)
		colnames <- unlist(colname_list)
		dtm <- t(dtm)
		colnames(dtm) <- colnames
		return(dtm)
	}
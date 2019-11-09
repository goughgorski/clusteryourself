pre_process_and_tokenize <- function(data, text_col, stopword_lang = 'english', non_stopwords = NULL, dictionary, token_lower_bound = 0, ngram_iter) {
	corpus <- pre_process_corpus(data = data, text = text_col, stopword_lang = stopword_lang, non_stopwords = non_stopwords)
	lower_bound <- round(token_lower_bound * length(corpus))
	ng <- nchar(dictionary) - nchar(gsub(" ",'',dictionary)) + 1
	for (i in 1:ngram_iter) {
		tokenizer <- function(x) {unlist(lapply(ngrams(sapply(words(x), depluralize), i), paste, collapse = " "), use.names = FALSE)}
		tmp <- as.matrix(DocumentTermMatrix(corpus, control = list(tokenize = tokenizer, weighting = weightTf, dictionary = dictionary[ng == i],
			bounds = list(global = c(lower_bound, Inf)))))
		if (i == 1) {dtm <- tmp} else {dtm <- cbind(dtm, tmp)}
	}
	dtm
}
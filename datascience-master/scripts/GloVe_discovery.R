#Global Vectors (GloVe)

# pre-process without VCorpus conversion
glove_tokens <- function(text, depluralize = FALSE) {

	print(noquote("Removing non-printable characters"))
	text <- gsub('^[:print:]','',text)

	print(noquote("Standardizing casing"))
	text <- tolower(text) # switched to after removing non-printable characters
	
	print(noquote("Removing non-ascii characters"))
	text <- gsub("[^\001-\177]",'', text, perl = TRUE)

	print(noquote("Removing whitespace, stopwords, letter-words, punctuation, and numbers"))
	text <- sapply(text, function(x) {stripWhitespace(x)})
	text <- sapply(text, function(x) {removeWords(x,stopwords('english'))})
	text <- sapply(text, function(x) {removeWords(x,letters)})
	text <- sapply(text, function(x) {removePunctuation(x)})
	text <- sapply(text, function(x) {removeNumbers(x)})

	if (depluralize == TRUE) {
		print(noquote("Depluralizing and tokenizing"))
		tokenizer <- function(x) {unlist(ngrams(sapply(words(x), depluralize), 1))}
	}
	
	sapply(text, tokenizer)
}

tokens <- glove_tokens(messages[messages$set == 'train', 'body_text'])

it <- itoken(tokens, n_chunks = 10) #n_chunks?
vocab <- create_vocabulary(it)
term_count_min <- round(.0075* nrow(messages[messages$set == 'train', ]))
vocab <- prune_vocabulary(vocab, term_count_min = term_count_min)
vectorizer <- vocab_vectorizer(vocab)
tcm <- create_tcm(it, vectorizer, skip_grams_window = 10)

glove = GlobalVectors$new(word_vectors_size = 100, vocabulary = vocab, x_max = 10)
fit <- glove$fit_transform(tcm, n_iter = 20)

cos_sim = sim2(x = as.matrix(fit), y = t(as.matrix(fit['counter',])), method = "cosine", norm = "l2")
head(sort(cos_sim[,1], decreasing = TRUE), 5)

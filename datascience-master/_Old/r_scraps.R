ngram_dictionary <- function(text,n, parallel = FALSE) {
  unlist(lapply(ngrams(words(text), n), paste, collapse = " "), use.names = FALSE)
}

corpus_ngram_dictionary <- function(corpus, n, parallel = FALSE) {
  dict <- unlist(lapply(corpus$content,function(x) {ngram_dictionary(x,n)}))
  dict <- sort(dict[!duplicated(dict)])
}

#Add code for parallelization
corpus_dtm <- function(corpus, n = 1, p_thresh = 0, dict = NULL, parallel = FALSE) {

	compare_dict <- TRUE
	dictionary <- corpus_ngram_dictionary(corpus, n, parallel = parallel)

	if (is.null(dict)) {
		compare_dict <- FALSE
		dict <- dictionary
		dict_diff_stats <- NULL		
	}

	dict_stats <- data.frame(matrix(ncol = 3, nrow = length(dict)))
	colnames(dict_stats) <- c('term', 'tdf', 'tdf_p')

	dtm <- data.frame(matrix(ncol = length(dict), nrow = length(corpus)))
	colnames(dtm) <- dict
	
	term_pass_thresh <- logical(length(dict))
	
	for (i in 1:length(dict)) {
		cat('\r',paste("Term", i, "of", length(dict), paste("(", round((i/length(dict))*100,0), "%)", ":", sep =''), substr(dict[i],1,2), sep = ' '))		
		
		#Match term and collect document statistics
    	term_match <- lapply(corpus$content, function(x) {gregexpr(dict[i],x)})
		term_count <- unlist(lapply(term_match,function(x)  {lapply(x,function(z){sum(z>0)})}))
		doc_freq <- sum(term_count>0, na.rm = T)
		doc_count <- sum(!is.na(term_count))
		
		dict_stats[i,] <- c(dict[i], doc_freq, (doc_freq/doc_count))

		if ((doc_freq/doc_count) > p_thresh) {
			term_pass_thresh[i] <- TRUE
			idf <- log(doc_count/doc_freq)
			tfidf <- term_count * idf
			#Add some catch for term position?
			dtm[,i] <- tfidf
		} 

	}

	if (compare_dict) {

		print(noquote("Collecing data on terms not in dictionary."))
		dict_diff <- intersect(setdiff(dictionary, dict),dictionary)
		dict_diff_stats <- data.frame(matrix(ncol = 3, nrow = length(dict_diff)))

		for (i in 1:length(dict_diff)) {
			cat('\r',paste("Term", i, "of", length(dict_diff), paste("(", round((i/length(dict_diff))*100,0), "%)", ":", sep =''), substr(dict[i],1,2), sep = ' '))		
			
			#Match term and collect document statistics
    		term_match <- lapply(corpus$content, function(x) {gregexpr(dict_diff[i],x)})
			term_count <- unlist(lapply(term_match,function(x) {lapply(x,function(z){sum(z>0)})}))
			doc_freq <- sum(term_count>0, na.rm = T)
			doc_count <- sum(!is.na(term_count))
		
			dict_diff_stats <- c(dict_diff[i], doc_freq, (doc_freq/doc_count))

		}

	}
	
	otuput <- list('dtm' = dtm[,term_pass_thresh], 'dictionary' = dict_stats, 'new_terms' = dict_diff_stats)
}


check <- corpus_dtm(test, n = 1, p_thresh = 0, parallel = FALSE)




uni_dict <- corpus_ngram_dictionary(test,1)




i <- sapply(corpus_document_names_tdm,function(x){x>=1})
corpus_document_names_tdm_ind <- corpus_document_names_tdm
corpus_document_names_tdm_ind[i] <- 1


assc <- list()

for(i in 1:nrow(corpus_document_names_tdm_ind)) {
  print(i)
  term_one <- corpus_document_names_tdm_ind[i,1]
  for (j in 1:nrow(corpus_document_names_tdm_ind)) {
    term_two <- corpus_document_names_tdm_ind[j,1]
    assc[noquote(paste(term_one,term_two, sep = ','))] <- corpus_document_names_tdm_ind[i,2:ncol(corpus_document_names_tdm_ind)] * corpus_document_names_tdm_ind[j,2:ncol(corpus_document_names_tdm_ind)]
  }
}

nc_bigrams <- ldply(assc,rbind)


corpus_document_names_tdm_ind[1,] * corpus_document_names_tdm_ind[2,]

rhs <- rep(corpus_document_names_tdm_ind,nrow(corpus_document_names_tdm_ind))
lhs <- order(rhs)



rownames(corpus_document_names_tdm)

document_names_bigrams <- tm_map(corpus_document_names, function(x){ngrams(words(x), 2)})



bigram_tokenizer <- function(x) {unlist(lapply(ngrams(words(x), 2), paste, collapse = " "), use.names = TRUE)}

corpus_document_names_tdm_bigram <- TermDocumentMatrix(corpus_document_names, control = list(tokenize = bigram_tokenizer))
corpus_document_names_tdm_bigram <- as.matrix(corpus_document_names_tdm_bigram)
#Confirmed TDM
confirmed_document_names_tdm_bigram <- corpus_document_names_tdm_bigram[,colnames(corpus_document_names_tdm) %in% confirmed]
termsum_confirmed_bigram <- rowSums(confirmed_document_names_tdm_bigram)
termsum_confirmed_bigram_ind <- rowSums(ifelse(confirmed_document_names_tdm_bigram>=1,1,confirmed_document_names_tdm_bigram))
terms_bigram <- names(termsum_confirmed_bigram)
terms_confirmed_bigram <- cbind(terms_bigram, termsum_confirmed_bigram, termsum_confirmed_bigram_ind)

#Rejected TDM
rejected_document_names_tdm_bigram <- corpus_document_names_tdm_bigram[,colnames(corpus_document_names_tdm) %in% rejected]
termsum_rejected_bigram <- rowSums(rejected_document_names_tdm_bigram)
termsum_rejected_bigram_ind <- rowSums(ifelse(rejected_document_names_tdm_bigram>=1,1,rejected_document_names_tdm_bigram))
terms_bigram <- names(termsum_rejected_bigram)
terms_rejected_bigram <- cbind(terms_bigram, termsum_rejected_bigram, termsum_rejected_bigram_ind)

#Merge Confirmed and Rejected Summaries
term_rates_bigram <- merge(terms_confirmed_bigram, terms_rejected_bigram, by = 'terms_bigram', all = T)
term_rates_bigram$jitter <- runif(nrow(term_rates_bigram))
dbWriteTable(local_connection, c('datasci_projects','term_rates_bigram'), value = as.data.frame(term_rates_bigram), overwrite = T, append = F)



corpus_email_bodies_tdm_bigram <- TermDocumentMatrix(corpus_email_bodies, control = list(tokenize = bigram_tokenizer))
corpus_email_bodies_tdm_bigram <- as.matrix(corpus_email_bodies_tdm_bigram)

#Confirmed TDM
confirmed_email_bodies_tdm <- corpus_email_bodies_tdm[,colnames(corpus_email_bodies_tdm) %in% confirmed]
termsum_confirmed <- rowSums(confirmed_email_bodies_tdm)
termsum_confirmed_ind <- rowSums(ifelse(confirmed_email_bodies_tdm>=1,1,confirmed_email_bodies_tdm))
terms <- names(termsum_confirmed)
terms_confirmed <- cbind(terms, termsum_confirmed, termsum_confirmed_ind)

#Rejected TDM
rejected_email_bodies_tdm <- corpus_email_bodies_tdm[,colnames(corpus_email_bodies_tdm) %in% rejected]
termsum_rejected <- rowSums(rejected_email_bodies_tdm)
termsum_rejected_ind <- rowSums(ifelse(rejected_email_bodies_tdm>=1,1,rejected_email_bodies_tdm))
terms <- names(termsum_rejected)
terms_rejected <- cbind(terms, termsum_rejected, termsum_rejected_ind)

#Merge Confirmed and Rejected Summaries
term_rates <- merge(terms_confirmed, terms_rejected, by = 'terms', all = T)
term_rates$jitter <- runif(nrow(term_rates))
dbWriteTable(local_connection, c('datasci_projects','term_rates'), value = as.data.frame(term_rates), overwrite = T, append = F)






data_msgs$lagg_fitler <- grepl('listing agreement', data_msgs$body_text, ignore.case = T) 

filter_sum <- rowSums(data_msgs[,filters])
table(filter_sum)





	con <- dbConnect(drv, dbname = 'development',
                 host = '1.tcp.ngrok.io', port = '23135',
                 user = 'luke')


t_counts <- apply(doctxt_dtm3,2,function(x){sum(x>0,na.rm=T)})


ngram_dictionary <- function(text,n) {
  unlist(lapply(ngrams(words(text), n), paste, collapse = " "), use.names = FALSE)
}

corpus_ngram_dictionary <- function(corpus, n) {
  dict <- unlist(lapply(corpus$content,function(x) {ngram_dictionary(x,n)}))
  dict <- sort(dict[!duplicated(dict)])
}

#Add code for parallelization
corpus_dtm <- function(corpus, n = 1, p_thresh = 0, dict = NULL) {

	compare_dict <- TRUE
	dictionary <- corpus_ngram_dictionary(corpus, n)

	if (is.null(dict)) {
		compare_dict <- FALSE
		dict <- dictionary
		dict_diff_stats <- NULL		
	}

	dict_stats <- data.frame(matrix(ncol = 3, nrow = length(dict)))
	colnames(dict_stats) <- c('term', 'tdf', 'tdf_p')

	dtm <- data.frame(matrix(ncol = length(dict), nrow = length(corpus)))
	colnames(dtm) <- dict
	
	term_pass_thresh <- logical(length(dict))
	
	t0 <- Sys.time()
	for (i in 1:length(dict)) {
		cat('\r',paste("~ Time Left:", (i/as.numeric(difftime(Sys.time(), t0, units = 'sec'))) * (length(dict) - i), "sec -", "Term", i, "of", length(dict), paste("(", round((i/length(dict))*100,0), "%)", ":", sep =''), substr(dict[i],1,2), sep = ' '))		
		
		#Match term and collect document statistics
    	term_match <- lapply(corpus$content, function(x) {gregexpr(paste(' ', dict[i], ' ', sept = ''),x)})
		term_count <- unlist(lapply(term_match,function(x)  {lapply(x,function(z){sum(z>0)})}))
		doc_freq <- sum(term_count>0, na.rm = T)
		doc_count <- sum(!is.na(term_count))
		
		dict_stats[i,] <- c(dict[i], doc_freq, (doc_freq/doc_count))

		if ((doc_freq/doc_count) > p_thresh) {
			term_pass_thresh[i] <- TRUE
			idf <- log(doc_count/doc_freq)
			tfidf <- term_count * idf
			#Add some catch for term position?
			dtm[,i] <- tfidf
		} 

	}

	if (compare_dict) {

		print(noquote("Collecing data on terms not in dictionary."))
		dict_diff <- intersect(setdiff(dictionary, dict),dictionary)
		dict_diff_stats <- data.frame(matrix(ncol = 3, nrow = length(dict_diff)))

		for (i in 1:length(dict_diff)) {
			cat('\r',paste("Term", i, "of", length(dict_diff), paste("(", round((i/length(dict_diff))*100,0), "%)", ":", sep =''), substr(dict[i],1,2), sep = ' '))		
			
			#Match term and collect document statistics
    		term_match <- lapply(corpus$content, function(x) {gregexpr(dict_diff[i],x)})
			term_count <- unlist(lapply(term_match,function(x) {lapply(x,function(z){sum(z>0)})}))
			doc_freq <- sum(term_count>0, na.rm = T)
			doc_count <- sum(!is.na(term_count))
		
			dict_diff_stats <- c(dict_diff[i], doc_freq, (doc_freq/doc_count))

		}

	}
	
	otuput <- list('dtm' = dtm[,term_pass_thresh], 'dictionary' = dict_stats, 'new_terms' = dict_diff_stats)
}


















ngram_dictionary <- function(text, n) {
  lapply(text, function(x) {ngrams(words(x), n), use.names = FALSE)})
}

par_corpus_ngram_dictionary <- function(corpus, n, parallel = 1) {
  
	if (parallel > 1) {
		if (parallel > detectCores()) {
			print(noquote("Call suggested using more cores than are available."))
			print(noquote(paste("Setting use to maximum of", detectCores(), "cores.")))
			parallel <- detectCores()
		} else { 
			print(noquote(paste("Starting parallel processing using", parallel, "of", detectCores(), "cores.")))
		}
	
	}

	cluster <- makeCluster(parallel, type = "FORK", outfile = "")
	dict <- unlist(parLapply(cluster, corpus$content,function(x) {ngram_dictionary(x, n)}))
	dict <- sort(dict[!duplicated(dict)])
	stopCluster(cluster) 
	return(dict) 
}

#Add code for parallelization
par_corpus_dtm <- function(corpus, n = 1, p_thresh = 0, dict = NULL, parallel = 1) {

	if (parallel > 1) {
		if (parallel > detectCores()) {
			print(noquote("Call suggested using more cores than are available."))
			print(noquote(paste("Setting use to maximum of", detectCores(), "cores.")))
			parallel <- detectCores()
		} else { 
			print(noquote(paste("Starting parallel processing using", parallel, "of", detectCores(), "cores.")))
		}
	
	}
	
	compare_dict <- TRUE
	dictionary <- par_corpus_ngram_dictionary(corpus, n, parallel = parallel)

	if (is.null(dict)) {
		compare_dict <- FALSE
		dict <- dictionary
		dict_diff_stats <- NULL		
	}

	dict_stats <- data.frame(matrix(ncol = 3, nrow = length(dict)))
	colnames(dict_stats) <- c('term', 'tdf', 'tdf_p')

	dtm <- data.frame(matrix(ncol = length(dict), nrow = length(corpus)))
	colnames(dtm) <- dict
	
	term_pass_thresh <- logical(length(dict))
	  
	cluster <- makeCluster(parallel, type = "FORK", outfile = "")

	for (i in 1:length(dict)) {
		cat('\r',paste("Term", i, "of", length(dict), paste("(", round((i/length(dict))*100,0), "%)", ":", sep =''), substr(dict[i],1,2), sep = ' '))		
		
		#Match term and collect document statistics
    	term_match <- parLapply(cluster, corpus$content, function(x) {gregexpr(dict[i],x)})
		term_count <- unlist(lapply(term_match, function(x)  {parLapply(cluster, x, function(z){sum(z>0)})}))
		doc_freq <- sum(term_count>0, na.rm = T)
		doc_count <- sum(!is.na(term_count))
		
		dict_stats[i,] <- c(dict[i], doc_freq, (doc_freq/doc_count))

		if ((doc_freq/doc_count) > p_thresh) {
			term_pass_thresh[i] <- TRUE
			idf <- log(doc_count/doc_freq)
			tfidf <- term_count * idf
			#Add some catch for term position?
			dtm[,i] <- tfidf
		} 

	}

	if (compare_dict) {

		print(noquote("Collecing data on terms not in dictionary."))
		dict_diff <- intersect(setdiff(dictionary, dict),dictionary)
		dict_diff_stats <- data.frame(matrix(ncol = 3, nrow = length(dict_diff)))

		for (i in 1:length(dict_diff)) {
			cat('\r',paste("Term", i, "of", length(dict_diff), paste("(", round((i/length(dict_diff))*100,0), "%)", ":", sep =''), substr(dict[i],1,2), sep = ' '))		
			
			#Match term and collect document statistics
    		term_match <- parLapply(cluster, corpus$content, function(x) {gregexpr(dict_diff[i],x)})
			term_count <- unlist(lapply(term_match,function(x) {parLapply(cluster, x, function(z){sum(z>0)})}))
			doc_freq <- sum(term_count>0, na.rm = T)
			doc_count <- sum(!is.na(term_count))
		
			dict_diff_stats <- c(dict_diff[i], doc_freq, (doc_freq/doc_count))

		}
	
	stopCluster(cluster)

	}
	
	otuput <- list('dtm' = dtm[,term_pass_thresh], 'dictionary' = dict_stats, 'new_terms' = dict_diff_stats)
}


tm_ngram_tokenizer <- function(x, n = 1) {
  unlist(lapply(x, function(i) {paste(ngrams(words(i), n), collapse = " ")}), use.names = FALSE)
}



















> nrow(doctxt_dtm3)
[1] 116322
> print(noquote(paste(ncol(doctxt_dtm3), 'unique terms found.')))                 [1] 28140 unique terms found.
> nrow(doctxt_dtm2)                                                               [1] 116322
> print(noquote(paste(ncol(doctxt_dtm2), 'unique terms found.')))
[1] 29738 unique terms found.
> print(noquote(paste(ncol(doctxt_dtm1), 'unique terms found.')))
Error in ncol(doctxt_dtm1) : object 'doctxt_dtm1' not found
> print(noquote(paste(ncol(doctxt_dtm), 'unique terms found.')))
[1] 6525 unique terms found.



	


connection <- local_db_conect('development')

	data_purchase_offers <- dbGetQuery(connection,"SELECT * FROM datasci_projects.vw_offer_extraction
	                                       WHERE state = 'FL'")

bkup <- data_purchase_offers

	print(noquote("Cleaning OCR debris, and multiple newlines"))
	data_purchase_offers$document_content_clean <- gsub("__|--", "", data_purchase_offers$document_content)
	data_purchase_offers$document_content_clean <- gsub("\n+", "\n", data_purchase_offers$document_content_clean)



. 52 \n

Page 2

purchase_order_data <- regexpr("(?Uis)(?=at the time established by the Closing Agent)(.*)?(?<=[\\n| |\\.]57[\\n| ])", data_purchase_offers$document_content_clean, perl=TRUE) 
purchase_order_data_sub <- substr(data_purchase_offers$document_content_clean,purchase_order_data,purchase_order_data + attr(purchase_order_data,"match.length"))
doc_split <- strsplit(purchase_order_data_sub,'\n', perl = TRUE)
	doc_split2 <- lapply(doc_split, function(x) {x<-trimws(gsub('\\s+', ' ', x, perl = TRUE))})
	doc_split3 <- lapply(doc_split2, function(x) {x<-x[x!="" & x!=" " & x!="-"]})
	doc_split4 <- lapply(doc_split3, function(x) {t(as.data.frame(x))})
	doc_split5 <- ldply(doc_split4, rbind.fill)

fnum <- regexpr("(?Uis)(?=[[:alnum:][:punct:]+])ASIS[[:alnum:][:punct:]+]", data_purchase_offers$document_content_clean, perl=TRUE) 
fnum_sub <- substr(data_purchase_offers$document_content_clean,fnum,fnum + attr(fnum,"match.length"))

check_second <- lapply(doc_split, function(x) {grepl('[[:alpha:].*]',x[[1]][2], perl = TRUE)})


check <- lapply(doc_split,function(x){length(x)})
check2 <- ldply(check, rbind)

doc_split5 <- doc_split4[check2<200]
doc_split6 <- ldply(doc_split5, rbind.fill)


money_check <- sapply(doc_split6, function(x) {grepl('^[[:digit:],.]*$', as.character(x), perl = TRUE) & grepl('[[:digit:]]{3,}', as.character(x), perl = TRUE)})
name_check <- sapply(doc_split6, function(x) {grepl('^[[:alpha:]]{3,}', as.character(x), perl = TRUE)})
parcel_id_check <- sapply(doc_split6, function(x) {grepl(' [[:alnum:]-]{4,}$', as.character(x), perl = TRUE)})


grepl(' [[:digit:]-]{4,}$', 'abc,, 03-41-29-066-0960-12-00-0-000-0-0-0')




parcel_id_anchor <- apply(parcel_id_check,1,function(x) {match(2,cumsum(x))})

parcel_id <- c()
for (i in 1:length(parcel_id_anchor)) {
	parcel_id[i] <- ifelse(!is.na(parcel_id_anchor[i]), as.character(doc_split6[i,parcel_id_anchor[i]]),NA)
}
doc_split5














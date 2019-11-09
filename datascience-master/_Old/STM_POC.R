#Clear Environment
rm(list=ls())

#Load libraries
source('~/workspace/datascience/functions/load_libraries.R')
load_libraries('~/workspace/datascience/functions/', include_bundles = c('utility'))
library(stm)
library(tm)
library(Matrix)
library(LDAvis)
library(tsne)
library(servr)
#library(pluralize) # to load: library(remotes), install_github('hrbrmstr/pluralize')

#Set Seed
set.seed(123456789)

#Connect to Local PSQL Database AND Pull Data
connection <- db_connect()

# pull the training corpus of emails along with client status
emails <- dbGetQuery(connection, "SELECT dspbim.*, client FROM datasci_projects.broad_inbox_messages dspbim
					LEFT JOIN (
						SELECT external_account_id, client_emails
						, CASE 
							WHEN event_type = 'ProspectFolderRejectedEvent' 
							THEN 0 ELSE 1 
						END AS client 
						FROM (
						SELECT external_account_id, client_emails, dsedf.event_type
						, ROW_NUMBER() OVER (PARTITION BY (external_account_id, client_emails))
							FROM datasci.event_data_flatten dsedf
							LEFT JOIN datasci.event_array_data_values_flatten dseadvf ON dseadvf.event_id = dsedf.event_id
							WHERE dsedf.event_type IN ('ProspectFolderConfirmedEvent', 'ProspectFolderManuallyCreatedEvent',
							'ProspectFolderRejectedEvent', 'ProspectFolderDeletedEvent')
							AND external_account_id::INT IN 
							(SELECT DISTINCT external_account_id FROM datasci_projects.broad_inbox_messages)
							AND client_emails IS NOT NULL) a
						WHERE a.row_number =1) A
					ON a.external_account_id::INT = dspbim.external_account_id and a.client_emails = dspbim.from
					WHERE dspbim.missing_reason = ''
					AND dspbim.created_at < '2019-02-01'")

# pull the email address associated with the external account ids in order to remove non-inbox messages
acct_addresses <- dbGetQuery(connection, "SELECT external_account_id, email FROM datasci.users
										WHERE external_account_id IN (
											SELECT distinct external_account_id FROM datasci_projects.broad_inbox_messages
											WHERE created_at < '2019-02-10')")

# merge in account email addresses to remove non-inbox messages
emails <- merge(emails, acct_addresses, by = 'external_account_id')
emails$inbox <- apply(emails, 1, function(x) {ifelse(x['email'] == x['to'] | grepl(x['email'], x['cc']), TRUE, FALSE)})
emails$inbox[is.na(emails$inbox)] <- FALSE

#clients <- dbGetQuery(connection, "SELECT external_account_id, client_emails, dsedf.event_type FROM datasci.event_data_flatten dsedf
#									LEFT JOIN datasci.event_array_data_values_flatten dseadvf ON dseadvf.event_id = dsedf.event_id
#									WHERE dsedf.event_type IN ('ProspectFolderConfirmedEvent', 'ProspectFolderManuallyCreatedEvent',
#									'ProspectFolderRejectedEvent')
#									AND external_account_id::INT IN 
#											(SELECT DISTINCT external_account_id FROM datasci_projects.broad_inbox_messages

# pull property transactions and rejection reasons
prop_tx <- dbGetQuery(connection, "SELECT external_account_id, originating_message_id, event_type, rejection_reason
									FROM datasci.event_data_flatten
									WHERE event_type IN ('PropertyTransactionConfirmedEvent', 'PropertyTransactionRejectedEvent')
									AND external_account_id::INT IN 
										(SELECT DISTINCT external_account_id FROM datasci_projects.broad_inbox_messages)
									AND originating_message_id IN 
										(SELECT DISTINCT message_id FROM datasci_projects.broad_inbox_messages)")

# removing prop tx with confirmed and rejected events
prop_tx <- ldply(lapply(split(prop_tx, prop_tx[, c('external_account_id', 'originating_message_id')], drop = T), function(x){
	if (any(x[, 'event_type'] %in% c('PropertyTransactionRejectedRejectedEvent')) &
		!any(x[, 'event_type'] %in% 'PropertyTransactionConfirmedEvent')) {client <- 0
		} else if (any(x[, 'event_type'] %in% 'PropertyTransactionConfirmedEvent') &
		!any(x[, 'event_type'] %in% c('PropertyTransactionRejectedEvent'))) {
			client <- 1} else {client <- NA}
			data.frame(external_account_id = unique(x[, 'external_account_id']),
						message_id = unique(x[, 'originating_message_id']),
						rejection_reason = paste(x[!is.na(x[, 'rejection_reason']) , 'rejection_reason'], collapse = ', '),
						 client = client)
	}), rbind)

# pull rejection reasons for client events
rej_reasons <- dbGetQuery(connection, "SELECT dsedf.external_account_id, client_emails, dsedf.event_type, rejection_reason
									FROM datasci.event_data_flatten dsedf
									LEFT JOIN datasci.event_array_data_values_flatten dseadvf ON dseadvf.event_id = dsedf.event_id
									WHERE dsedf.event_type IN ('ProspectFolderConfirmedEvent', 'ProspectFolderRejectedEvent')
									AND external_account_id::INT IN 
										(SELECT DISTINCT external_account_id FROM datasci_projects.broad_inbox_messages)
									AND client_emails IN 
										(SELECT DISTINCT dsp.from FROM datasci_projects.broad_inbox_messages dsp)")

# removing contacts with confirmed and rejected events
rej_reasons <- ldply(lapply(split(rej_reasons, rej_reasons[, c('external_account_id', 'client_emails')], drop = T), function(x){
	if (any(x[, 'event_type'] %in% c('ProspectFolderRejectedEvent')) &
		!any(x[, 'event_type'] %in% 'ProspectFolderConfirmedEvent')) {client <- 0
		} else if (any(x[, 'event_type'] %in% 'ProspectFolderConfirmedEvent') &
		!any(x[, 'event_type'] %in% c('ProspectFolderRejectedEvent'))) {
			client <- 1} else {client <- NA}
			data.frame(external_account_id = unique(x[, 'external_account_id']),
						client_emails = unique(x[, 'client_emails']),
						rejection_reason = paste(x[!is.na(x[, 'rejection_reason']) , 'rejection_reason'], collapse = ', '),
						 client = client)
	}), rbind)

# modified pre_process_corpus function ** need to save to functions library **
pre_process_corpus <- function(data, text, stopword_lang = 'english', extra_stopwords = NULL, non_stopwords = NULL, remove_letters = FALSE, stem = FALSE, stem_dict = NULL) {
  
  #Casing
  print(noquote("Standardizing casing"))
  text <- tolower(data[,text])
  
  #Remove unprintable Characters
  print(noquote("Removing non-printable characters"))  
  text <- gsub('^[:print:]','',text)
  
  #Remove non-ascii characters
  print(noquote("Removing non-ascii characters"))
  text <- gsub("[^\001-\177]",'', text, perl = TRUE)
  
  #Convert to volatile corpus
  print(noquote("Converting to VCorpus"))  
  text <- VCorpus(VectorSource(text))
  
  #Set and Remove Stopwords
  print(noquote("Removing stopwords"))  
  
  stopwords <- c(stopwords(stopword_lang),extra_stopwords)
  stopwords <- stopwords[which(!stopwords %in% non_stopwords)]
  
  text <- tm_map(text, function(x) {removeWords(x,stopwords)})
  
  #Add Entity extraction here ?
  
  #Remove punctuation, Strip Whitespace, Remove Numbers
  print(noquote("Removing punctuation, whitespace, numbers, and letter-words"))        
  text <- tm_map(text, function(x) {removePunctuation(x)})
  text <- tm_map(text, function(x) {stripWhitespace(x)})
  text <- tm_map(text, function(x) {removeNumbers(x)})
  
  if (remove_letters) {
    text <- tm_map(text, function(x) {removeWords(x,letters)})
  }
  
  if (stem == TRUE) {
    print(noquote("Stemming words"))        
    text <- tm_map(text, function(x) {stemDocument(x)})
    if (!is.null(stem_dict)) {
      text <- tm_map(text, function(x) {stemCompletion(x, dictionary = stem_dict)})
    } else { print(noquote("No stem completion dictionary found. Leaving stems as is."))}
  }
  
  return(text)
}

# coarse singularization function for use in tokenization ** need to save to functions library **
depluralize <- function(term){
  #if (class(x) != 'character') {stop('depluralize only works on character values')}
  if (is.na(term)){NA
  } else if (term == "" | term == " ") {""
  } else if (class(term) != 'character') {term
  } else if (last(strsplit(term, '')[[1]]) == 's' & length(strsplit(term, '')[[1]]) > 2
             && strsplit(term, '')[[1]][length(strsplit(term, '')[[1]]) - 1] != 's'){
    char <- strsplit(term, '')[[1]]
    t_length <- length(char)
    if (char[t_length - 1] == 'e') {
      if (char[t_length - 2] == 'i') {
        paste(paste(char[1:(t_length - 3)], collapse = ''), 'y', sep = '')
      } else if (char[t_length - 2] %in% c('h', 'x', 'o') |
                 char[t_length - 2] == 's' & !char[t_length -3] %in% c('a', 'e', 'i', 'o', 'u')) {
        paste(char[1:(t_length - 2)], collapse = '')
      } else {paste(char[1:(t_length - 1)], collapse = '')}
    } else {paste(char[1:(t_length - 1)], collapse = '')}
  } else {term}
  
}

## email body text ##
# pre-process corpus of email body text ** turn pre-process + tokenize into a function w n grams as as argument **
corp_1 <- pre_process_corpus(data = emails[emails$inbox == TRUE,], text = 'body_text', stopword_lang = 'english', non_stopwords = c('you', 'i'))
corp_1_p <- round(.0075* length(corp_1))

tokenizer <- function(x) {unlist(lapply(ngrams(sapply(words(x), depluralize), 1), paste, collapse = " "), use.names = FALSE)}
corp_1_dtm1 <- as.matrix(DocumentTermMatrix(corp_1, 
                          control = list(tokenize = tokenizer, weighting = weightTf,
                                        bounds = list(global = c(corp_1_p, Inf)))))
tokenizer <- function(x) {unlist(lapply(ngrams(sapply(words(x), depluralize), 2), paste, collapse = " "), use.names = FALSE)}
corp_1_dtm2 <- as.matrix(DocumentTermMatrix(corp_1, 
                          control = list(tokenize = tokenizer, weighting = weightTf,
                                        bounds = list(global = c(corp_1_p, Inf)))))
corp_body_dtm <- cbind(corp_1_dtm1, corp_1_dtm2)
tokenizer <- function(x) {unlist(lapply(ngrams(sapply(words(x), depluralize), 3), paste, collapse = " "), use.names = FALSE)}
corp_1_dtm3 <- as.matrix(DocumentTermMatrix(corp_1, 
                          control = list(tokenize = tokenizer, weighting = weightTf,
                                        bounds = list(global = c(corp_1_p, Inf)))))
corp_body_dtm <- cbind(corp_body_dtm, corp_1_dtm3)

# convert matrix into document-level list of vocab counts
docs_body <- apply(corp_body_dtm, 1, function(x){
	tmp <- as.data.frame(x)
	tmp$vocab <- 1:nrow(tmp)
	tmp <- tmp[tmp[,1] >0,]
	tmp <- as.matrix.data.frame(t(tmp[, c(2,1)]))
	return(tmp)
	})

sparse_corp_body <- Matrix(corp_body_dtm, sparse = TRUE)

stm_body_k5 <- stm(sparse_corp_body, K = 5)

#stm_body_k10 <- stm(sparse_corp_body, K = 10)

## email subject ##
# pre-process corpus of email subject - fix de-pluralization and remove corpus proportion limit
corp_1 <- pre_process_corpus(data = emails, text = 'subject', stopword_lang = 'english', non_stopwords = c('you', 'i'))
corp_1_p <- round(.0075* length(corp_1))

tokenizer <- function(x) {unlist(lapply(ngrams(sapply(words(x), depluralize), 1), paste, collapse = " "), use.names = FALSE)}
corp_1_dtm1 <- as.matrix(DocumentTermMatrix(corp_1, 
                          control = list(tokenize = tokenizer, weighting = weightTf,
                                        bounds = list(global = c(corp_1_p, Inf)))))
tokenizer <- function(x) {unlist(lapply(ngrams(sapply(words(x), depluralize), 2), paste, collapse = " "), use.names = FALSE)}
corp_1_dtm2 <- as.matrix(DocumentTermMatrix(corp_1, 
                          control = list(tokenize = tokenizer, weighting = weightTf,
                                        bounds = list(global = c(corp_1_p, Inf)))))
corp_subject_dtm <- cbind(corp_1_dtm1, corp_1_dtm2)

# convert matrix into document-level list of vocab counts
docs_subject <- apply(corp_subject_dtm, 1, function(x){
	tmp <- as.data.frame(x)
	tmp$vocab <- 1:nrow(tmp)
	tmp <- tmp[tmp[,1] >0,]
	tmp <- as.matrix.data.frame(t(tmp[, c(2,1)]))
	return(tmp)
	})

sparse_corp_subject <- Matrix(corp_subject_dtm, sparse = TRUE)

stm_subject_k5 <- stm(sparse_corp_subject, K = 5)

#stm_subject_k10 <- stm(sparse_corp_subject, K = 10)

# converting documents for dashboard use

stm_dash_prep <- function(stm_out, model, docs) {
	# term-level probabilities
	tmp <- as.data.frame(t(exp(stm_out$beta$logbeta[[1]])))
	tmp$t_index <- row.names(tmp)
	tmp <- melt(tmp, id = 't_index')
	colnames(tmp)[colnames(tmp) %in% c('variable', 'value')] <- c('topic', 'probability')
	tmp$topic <- as.integer(tmp$topic)
	tmp$model <- model
	tmp$k <- stm_out$settings$dim$K
	tmp$term <- stm_out$vocab
	# estimated term count by topic
	term_ct <- as.data.frame(stm_out$theta)
	term_ct$doc_index <- row.names(term_ct)
	term_ct <- melt(term_ct, id = 'doc_index')
	colnames(term_ct)[colnames(term_ct) %in% c('variable', 'value')] <- c('topic', 'probability')
	term_ct$topic <- as.integer(term_ct$topic)
	doc_length <- as.integer(sapply(docs, function(x) sum(x[2,])))
	doc_length <- data.frame(doc_index = 1:length(doc_length), n_terms = doc_length)
	term_ct <- merge(term_ct, doc_length, by = 'doc_index')
	term_ct <- ldply(lapply(split(term_ct, term_ct[, 'topic']), function(x){sum(x[,'probability']*x[, 'n_terms'])}), rbind)
	colnames(term_ct) <- c('topic', 'est_term_ct')
	tmp <- merge(tmp, term_ct, by = 'topic')
	tmp$est_term_ct <- tmp$probability * tmp$est_term_ct
	# pca
	pca_calc <- prcomp(exp(stm_out$beta$logbeta[[1]]), center = T, scale. = T)
	pca <- as.data.frame(pca_calc$x)
	pca$topic <- 1:stm_out$settings$dim$K
	pca <- pca[, c('topic', 'PC1', 'PC2')]
	pca$k <- stm_out$settings$dim$K
	pca$PC1_var_proportion <- summary(pca_calc)$importance[2,1]
	pca$PC2_var_proportion <- summary(pca_calc)$importance[2,2]
	merge(tmp, pca, by = c('topic', 'k'))
}

#models <- list(stm_subject_k5, stm_subject_k10)
#corpora <- rep('subject', length.out = length(models))
#docs <- list(docs_subject, docs_subject)

#models <- list(stm_body_k5, stm_body_k10)
#corpora <- rep('body', length.out = length(models))
#docs <- list(docs_body, docs_body)

stm_models <- list(stm_body_k5, stm_subject_k5)
stm_model_name <- c('body', 'subject')
docs <- list(docs_body, docs_subject)


stm_dash <- data.frame()
for(i in 1:length(stm_models)){
    tmp <- stm_dash_prep(stm_models[[i]], stm_model_name[[i]], docs[[i]])
    stm_dash <- rbind(tmp, stm_dash)}

setwd('~/Public')
write.csv(stm_dash, 'stm_dash.csv', row.names = FALSE)

# comparing topic prevalence to confirmed/created clients
#data <- emails
data <- emails[emails$inbox,]
data$n_terms_subject <- as.integer(sapply(docs_subject, function(x) sum(x[2,])))
data$n_terms_body <- as.integer(sapply(docs_body, function(x) sum(x[2,])))


topic_prev_prep <- function(stm_out, model) {
	topic_prevalence <- as.data.frame(stm_out$theta)
	colnames(topic_prevalence) <- paste('T', 1:length(colnames(topic_prevalence)), '_',
 		paste(model, '_k', length(colnames(topic_prevalence)), sep = ''), sep = '')
 	topic_prevalence
}

for(i in 1:length(stm_models)){
    tmp <- topic_prev_prep(stm_models[[i]], stm_model_name[[i]])
    data <- cbind(data, tmp)}

write.csv(data, 'topic_prevalence.csv', row.names = FALSE)

# import rejection reasons and adjust
#rej_full <- merge(data, prop_tx, by.x = c('external_account_id', 'message_id'), by.y = c('external_account_id', 'originating_message_id'))
#colnames(rej_full)[which(colnames(rej_full) == 'from')] <- 'client_emails'
#rej_full <- rbind(rej_full[, c('external_account_id', 'client_emails', 'event_type', 'rejection_reason')], rej_reasons)

## create models
# user-contact-level models
data <- merge(data, rej_reasons,by.x = c('external_account_id', 'from'), by.y = c('external_account_id', 'client_emails'), all.x = T)
data$client <- ifelse(is.na(data$client.y) & !is.na(data$client.x), data$client.x, data$client.y)

kmeans_vars <- c('T1_body_k5', 'T2_body_k5', 'T3_body_k5', 'T4_body_k5', 'T5_body_k5',
	'T1_subject_k5', 'T2_subject_k5', 'T3_subject_k5', 'T4_subject_k5', 'T5_subject_k5')

tp_scaling_table <- t(cbind(as.data.frame(apply(data[, kmeans_vars], 2, mean)), as.data.frame(apply(data[, kmeans_vars], 2, sd))))
row.names(tp_scaling_table) <- c('mean', 'sd')

compare <- data[, c('external_account_id', 'message_id', 'client', 'rejection_reason', kmeans_vars)]

compare[, kmeans_vars] <- scale(compare[, kmeans_vars])

# determine best k
wss <- (nrow(compare)-1)*sum(apply(compare[, kmeans_vars],2,var))
for (i in 2:20) wss[i] <- sum(kmeans(compare[, kmeans_vars], centers=i)$withinss)

kfit <- kmeans(compare[, kmeans_vars], 8)
compare$cluster <- kfit$cluster

compare <- compare[!is.na(compare$client), ]

logit_stm_contact <- glm(client ~ T1_body_k5 + T2_body_k5 + T3_body_k5 + T4_body_k5 + T5_body_k5
	+ T1_subject_k5 + T2_subject_k5 + T3_subject_k5 + T4_subject_k5 + T5_subject_k5 - 1, 
	data = compare, family = binomial(link = 'logit'))

logit_stm_contact_cluster <- glm(client ~ factor(cluster) - 1, 
	data = compare, family = binomial(link = 'logit'))

non_rejection <- c('Already added this client to Folio', 'already-added-to-folio',
 'Lost lead or client', 'lost-client', 'removed_client_manually',
 'already-closed', 'Closed/Closing Soon', 'offer-not-accepted', 'not-client-yet', 'Not a client yet')

compare$client <- apply(compare, 1, function(x){ifelse(grepl(paste(non_rejection, collapse = '|'), x['rejection_reason']), 1, x['client'])})
compare$client <- as.numeric(compare$client)

logit_stm_contact_rej_adj <- glm(client ~ T1_body_k5 + T2_body_k5 + T3_body_k5 + T4_body_k5 + T5_body_k5
	+ T1_subject_k5 + T2_subject_k5 + T3_subject_k5 + T4_subject_k5 + T5_subject_k5 - 1, 
	data = compare, family = binomial(link = 'logit'))

logit_stm_contact_cluster_rej_adj <- glm(client ~ factor(cluster) - 1, 
	data = compare, family = binomial(link = 'logit'))

# email-level models
comp2 <- data[, c('external_account_id', 'message_id', 'from', 'rejection_reason', kmeans_vars)]
comp2 <- merge(comp2, prop_tx, by = c('external_account_id', 'message_id'), all.x = T)
comp2$rejection_reason <- ifelse(is.na(comp2$rejection_reason.y) & !is.na(comp2$rejection_reason.x), comp2$rejection_reason.x, comp2$rejection_reason.y)
comp2[, c('rejection_reason.x', 'rejection_reason.y')] <- NULL
comp2 <- merge(comp2, email_status[!is.na(email_status$client), ], by = 'message_id', all.x = T)
comp2$client <- ifelse(is.na(comp2$client.y) & !is.na(comp2$client.x), comp2$client.x, comp2$client.y)
comp2$cluster <- kfit$cluster

comp2 <- comp2[!is.na(comp2$client), ]

logit_stm_email <- glm(client ~ T1_body_k5 + T2_body_k5 + T3_body_k5 + T4_body_k5 + T5_body_k5
	+ T1_subject_k5 + T2_subject_k5 + T3_subject_k5 + T4_subject_k5 + T5_subject_k5 - 1, 
	data = comp2, family = binomial(link = 'logit'))

logit_stm_email_cluster <- glm(client ~ factor(cluster) - 1, 
	data = comp2, family = binomial(link = 'logit'))

comp2$client <- apply(comp2, 1, function(x){ifelse(grepl(paste(non_rejection, collapse = '|'), x['rejection_reason']), 1, x['client'])})
comp2$client <- as.numeric(comp2$client)

logit_stm_email_rej_adj <- glm(client ~ T1_body_k5 + T2_body_k5 + T3_body_k5 + T4_body_k5 + T5_body_k5
	+ T1_subject_k5 + T2_subject_k5 + T3_subject_k5 + T4_subject_k5 + T5_subject_k5 - 1, 
	data = comp2, family = binomial(link = 'logit'))

logit_stm_email_cluster_rej_adj <- glm(client ~ factor(cluster) - 1, 
	data = comp2, family = binomial(link = 'logit'))

# creating ensemble of models
logit_models <- list(logit_stm_contact = logit_stm_contact, logit_stm_contact_rej_adj = logit_stm_contact_rej_adj,
					logit_stm_email = logit_stm_email, logit_stm_email_rej_adj = logit_stm_email_rej_adj)
logit_models_cluster <- list(logit_stm_contact_cluster = logit_stm_contact_cluster, logit_stm_contact_cluster_rej_adj = logit_stm_contact_cluster_rej_adj,
					logit_stm_email_cluster = logit_stm_email_cluster, logit_stm_email_cluster_rej_adj = logit_stm_email_cluster_rej_adj)

coef_table <- ldply(lapply(logit_models, function(x) {
	t(as.data.frame(x$coefficients))}), rbind)

coef_table_cluster <- ldply(lapply(logit_models_cluster, function(x) {
	t(as.data.frame(x$coefficients))}), rbind)

colnames(coef_table_cluster) <- c('model_name', c(1:(ncol(coef_table_cluster) - 1)))

#objects to save
save <- list(stm_body_k5, stm_subject_k5, data)
save(save)

# validate on second corpus of emails
vset <- dbGetQuery(connection, "SELECT dspbim.*, client FROM datasci_projects.broad_inbox_messages dspbim
					LEFT JOIN (
						SELECT external_account_id, client_emails
						, CASE 
							WHEN event_type IN ('ProspectFolderRejectedEvent', 'ProspectFolderDeletedEvent') 
							THEN 0 ELSE 1 
						END AS client 
						FROM (
						SELECT external_account_id, client_emails, dsedf.event_type
						, ROW_NUMBER() OVER (PARTITION BY (external_account_id, client_emails))
							/*, CASE WHEN dsedf.event_type = 'ProspectFolderRejectedEvent' THEN 0 ELSE 1 END AS client */
							FROM datasci.event_data_flatten dsedf
							LEFT JOIN datasci.event_array_data_values_flatten dseadvf ON dseadvf.event_id = dsedf.event_id
							WHERE dsedf.event_type IN ('ProspectFolderConfirmedEvent', 'ProspectFolderManuallyCreatedEvent',
							'ProspectFolderRejectedEvent', 'ProspectFolderDeletedEvent')
							AND external_account_id::INT IN 
							(SELECT distinct external_account_id FROM datasci_projects.broad_inbox_messages)
							AND client_emails IS NOT NULL) a
						WHERE a.row_number =1) a
					ON a.external_account_id::int = dspbim.external_account_id and a.client_emails = dspbim.from
					WHERE dspbim.missing_reason = ''
					AND dspbim.created_at > '2019-02-01'
					AND dspbim.n_attachments IS NULL")

vset_full <- vset

tset <- dbGetQuery(connection, "SELECT dspbim.*, event_type, rejection_reason 
					FROM datasci_projects.broad_inbox_messages dspbim
					INNER JOIN (SELECT dsedf.prospect_folder_id, dsedf.originating_message_id, dsedf2.event_type, dsedf2.rejection_reason
								FROM datasci.event_data_flatten dsedf
								INNER JOIN datasci.event_data_flatten dsedf2 ON dsedf.prospect_folder_id = dsedf2.prospect_folder_id
								WHERE dsedf.originating_message_id IN (
									SELECT message_id FROM datasci_projects.broad_inbox_messages WHERE n_attachments IS NOT NULL)
								AND dsedf.event_type = 'ProspectFolderAutomaticallyCreatedEvent'
								AND dsedf2.event_type IN ('ProspectFolderConfirmedEvent', 'ProspectFolderRejectedEvent')) a
					ON a.originating_message_id = dspbim.message_id
					WHERE dspbim.missing_reason = ''
					AND dspbim.n_attachments IS NOT NULL")

# removing emails with confirmed and rejected events
dedupe <- ldply(lapply(split(tset, tset[, 'message_id'], drop = T), function(x){
	if (any(x[, 'event_type'] %in% c('ProspectFolderRejectedEvent')) &
		!any(x[, 'event_type'] %in% 'ProspectFolderConfirmedEvent')) {client <- 0
		} else if (any(x[, 'event_type'] %in% 'ProspectFolderConfirmedEvent') &
		!any(x[, 'event_type'] %in% c('ProspectFolderRejectedEvent'))) {
			client <- 1} else {client <- NA}
			data.frame(message_id = unique(x[, 'message_id']),
						rejection_reason = paste(x[!is.na(x[, 'rejection_reason']) , 'rejection_reason'], collapse = ', '),
						 client = client)}), rbind)

tset <- merge(tset[!duplicated(tset$message_id),], dedupe, by = 'message_id')
tset$rejection_reason <- tset$rejection_reason.y
tset <- tset[!is.na(tset$client),]

email_status <- dbGetQuery(connection, "SELECT dsedf.prospect_folder_id as object_id, dsedf.originating_message_id
										, dsedf2.event_type, dsedf2.rejection_reason, 'client' as business_object
										, ROW_NUMBER () OVER (PARTITION BY (dsedf.prospect_folder_id))
										FROM datasci.event_data_flatten dsedf
										LEFT JOIN datasci.event_data_flatten dsedf2 ON dsedf.prospect_folder_id = dsedf2.prospect_folder_id
										WHERE dsedf.originating_message_id IN (SELECT message_id FROM datasci_projects.broad_inbox_messages)
										AND dsedf.event_type = 'ProspectFolderAutomaticallyCreatedEvent'
										AND dsedf2.event_type IN ('ProspectFolderConfirmedEvent', 'ProspectFolderRejectedEvent')
										UNION
										SELECT dsedf.property_transaction_id as object_id, dsedf.originating_message_id
										, dsedf2.event_type, dsedf2.rejection_reason, 'prop_tx' as business_object
										, ROW_NUMBER () OVER (PARTITION BY (dsedf.property_transaction_id))
										FROM datasci.event_data_flatten dsedf
										LEFT JOIN datasci.event_data_flatten dsedf2 ON dsedf.property_transaction_id = dsedf2.property_transaction_id
										WHERE dsedf.originating_message_id IN (SELECT message_id FROM datasci_projects.broad_inbox_messages)
										AND dsedf.event_type = 'PropertyTransactionAutomaticallyCreatedEvent'
										AND dsedf2.event_type IN ('PropertyTransactionConfirmedEvent', 'PropertyTransactionRejectedEvent')")

# collapse on message_id to determine email-level client status
email_status <- ldply(lapply(split(email_status, email_status[, 'originating_message_id']), function(x){
	if (any(x[, 'event_type'] %in% c('ProspectFolderRejectedEvent', 'PropertyTransactionRejectedEvent')) &
		!any(x[, 'event_type'] %in% c('ProspectFolderConfirmedEvent', 'PropertyTransactionConfirmedEvent'))) {client <- 0
		} else if (any(x[, 'event_type'] %in% c('ProspectFolderConfirmedEvent', 'PropertyTransactionConfirmedEvent')) &
		!any(x[, 'event_type'] %in% c('ProspectFolderRejectedEvent', 'PropertyTransactionRejectedEvent'))) {
			client <- 1} else {client <- NA}
			data.frame(message_id = unique(x[, 'originating_message_id']), client = client
				, rejection_reason = paste(x[!is.na(x[, 'rejection_reason']) , 'rejection_reason'], collapse = ', ')
				, business_object = unique(x[, 'business_object']))
	}))

vset <- merge(vset_full, email_status, by = 'message_id')
vset$client <- vset$client.y
vset <- vset[vset$business_object == 'client',]

# Step 1: pre-process and tokenize body text and subject
vcorp_body <- pre_process_corpus(data = vset, text = 'body_text', stopword_lang = 'english', non_stopwords = c('you', 'i'))
#vcorp_body_p <- round(.0075* length(vcorp_body))
vcorp_body_p <- 0

tokenizer <- function(x) {unlist(lapply(ngrams(sapply(words(x), depluralize), 1), paste, collapse = " "), use.names = FALSE)}
vcorp_body_dtm1 <- as.matrix(DocumentTermMatrix(vcorp_body, 
                          control = list(tokenize = tokenizer, weighting = weightTf,
                                        bounds = list(global = c(vcorp_body_p, Inf)))))
tokenizer <-  function(x) {unlist(lapply(ngrams(sapply(words(x), depluralize), 2), paste, collapse = " "), use.names = FALSE)}
vcorp_body_dtm2 <- as.matrix(DocumentTermMatrix(vcorp_body, 
                          control = list(tokenize = tokenizer, weighting = weightTf,
                                        bounds = list(global = c(vcorp_body_p, Inf)))))
vcorp_body_dtm <- cbind(vcorp_body_dtm1, vcorp_body_dtm2)
tokenizer <-  function(x) {unlist(lapply(ngrams(sapply(words(x), depluralize), 3), paste, collapse = " "), use.names = FALSE)}
vcorp_body_dtm3 <- as.matrix(DocumentTermMatrix(vcorp_body, 
                          control = list(tokenize = tokenizer, weighting = weightTf,
                                        bounds = list(global = c(vcorp_body_p, Inf)))))
vcorp_body_dtm <- cbind(vcorp_body_dtm, vcorp_body_dtm3)

vdocs_body <- apply(vcorp_body_dtm, 1, function(x){
	tmp <- as.data.frame(x)
	tmp$vocab <- 1:nrow(tmp)
	tmp <- tmp[tmp[,1] >0,]
	tmp <- as.matrix.data.frame(t(tmp[, c(2,1)]))
	return(tmp)
	})

vcorp_subject <- pre_process_corpus(data = vset, text = 'subject', stopword_lang = 'english', non_stopwords = c('you', 'i'))
#vcorp_subject_p <- round(.0075* length(vcorp_subject))
vcorp_subject_p <- 0

tokenizer <- function(x) {unlist(lapply(ngrams(sapply(words(x), depluralize), 1), paste, collapse = " "), use.names = FALSE)}
vcorp_subject_dtm1 <- as.matrix(DocumentTermMatrix(vcorp_subject, 
                          control = list(tokenize = tokenizer, weighting = weightTf,
                                        bounds = list(global = c(vcorp_subject_p, Inf)))))
tokenizer <-  function(x) {unlist(lapply(ngrams(sapply(words(x), depluralize), 2), paste, collapse = " "), use.names = FALSE)}
vcorp_subject_dtm2 <- as.matrix(DocumentTermMatrix(vcorp_subject, 
                          control = list(tokenize = tokenizer, weighting = weightTf,
                                        bounds = list(global = c(vcorp_subject_p, Inf)))))
vcorp_subject_dtm <- cbind(vcorp_subject_dtm1, vcorp_subject_dtm2)

vdocs_subject <- apply(vcorp_subject_dtm, 1, function(x){
	tmp <- as.data.frame(x)
	tmp$vocab <- 1:nrow(tmp)
	tmp <- tmp[tmp[,1] >0,]
	tmp <- as.matrix.data.frame(t(tmp[, c(2,1)]))
	return(tmp)
	})

# Step 2: align processed texts to modeled vocab (replace removed docs with empty matrices)
vcorp_body_aligned <- alignCorpus(readCorpus(vcorp_body_dtm, type = 'dtm'), stm_body_k5$vocab)
names(vcorp_body_aligned$documents) <- c(1:nrow(vset))[!c(1:nrow(vset)) %in% vcorp_body_aligned$docs.removed]
vcorp_body_aligned <- lapply(seq_along(c(1:nrow(vset))), function (x) {
	if (any(vcorp_body_aligned$docs.removed %in% x)) {
		matrix(nrow = 2, ncol = 0)} else {
		vcorp_body_aligned$documents[[which(as.numeric(names(vcorp_body_aligned$documents)) == x)]]}
	})

vcorp_subject_aligned <- alignCorpus(readCorpus(vcorp_subject_dtm, type = 'dtm'), stm_subject_k5$vocab)
names(vcorp_subject_aligned$documents) <- c(1:nrow(vset))[!c(1:nrow(vset)) %in% vcorp_subject_aligned$docs.removed]
vcorp_subject_aligned <- lapply(seq_along(c(1:nrow(vset))), function (x) {
	if (any(vcorp_subject_aligned$docs.removed %in% x)) {
		matrix(nrow = 2, ncol = 0)} else {
		vcorp_subject_aligned$documents[[which(as.numeric(names(vcorp_subject_aligned$documents)) == x)]]}
	})

# Step 3: estimate topic prevalences
vcorp_body_fit <- fitNewDocuments(stm_body_k5, vcorp_body_aligned)

vcorp_subject_fit <- fitNewDocuments(stm_subject_k5, vcorp_subject_aligned)

topic_fit_prep <- function(vcorp_fit, model) {
	topic_prevalence <- as.data.frame(vcorp_fit$theta)
	colnames(topic_prevalence) <- paste('T', 1:length(colnames(topic_prevalence)), '_',
 		paste(model, '_k', length(colnames(topic_prevalence)), sep = ''), sep = '')
 	topic_prevalence
}

vcorp <- list(vcorp_body_fit, vcorp_subject_fit)
model_name <- c('body', 'subject')

for(i in 1:length(vcorp)){
    tmp <- topic_fit_prep(vcorp[[i]], model_name[[i]])
    vset <- cbind(vset, tmp)}

# Step 4: assign cluster
vset$n_terms_subject <- as.integer(sapply(vdocs_subject, function(x) sum(x[2,])))
vset$n_terms_body <- as.integer(sapply(vdocs_body, function(x) sum(x[2,])))

tmp <- sapply(seq_along(kmeans_vars), function(x) {
	(vset[, kmeans_vars[x]] - tp_scaling_table['mean', kmeans_vars[x]])/tp_scaling_table['sd', kmeans_vars[x]]
	})

vset[, kmeans_vars] <- scale(vset[, kmeans_vars])

closest.cluster <- function(x) {
  cluster.dist <- apply(kfit$centers[, kmeans_vars], 1, function(y) sqrt(sum((x-y)^2)))
  return(which.min(cluster.dist)[1])
}

vset$fit_cluster <- apply(vset[, kmeans_vars], 1, closest.cluster)

#### Email Intake Process for Prediction Assignment ####

message <- emails[1,]
stm_models <- list(stm_body_k5, stm_subject_k5)
corpus_names <- c('body', 'subject')
kfit <- kfit

pre_process_and_tokenize <- function(data, text_col, stopword_lang = 'english', non_stopwords = NULL, token_lower_bound = 0, ngram_iter) {
	corpus <- pre_process_corpus(data = data, text = text_col, stopword_lang = stopword_lang, non_stopwords = non_stopwords)
	lower_bound <- round(token_lower_bound * length(corpus))
	for (i in 1:ngram_iter) {
		tokenizer <- function(x) {unlist(lapply(ngrams(sapply(words(x), depluralize), i), paste, collapse = " "), use.names = FALSE)}
		tmp <- as.matrix(DocumentTermMatrix(corpus, control = list(tokenize = tokenizer, weighting = weightTf,
			bounds = list(global = c(lower_bound, Inf)))))
		if (i == 1) {dtm <- tmp} else {dtm <- cbind(dtm, tmp)}
	}
	dtm
}

dtm_body <- pre_process_and_tokenize(message, 'body_text', non_stopwords = c('you', 'i'), ngram_iter = 3)
dtm_subject <- pre_process_and_tokenize(message, 'subject', non_stopwords = c('you', 'i'), ngram_iter = 2)

align_corpus <- function(dtm, vocab) {
	aligned <- alignCorpus(list(documents = readCorpus(dtm, type = 'Matrix')$documents, vocab = colnames(dtm)), vocab)
	names(aligned$documents) <- c(1:nrow(dtm))[!c(1:nrow(dtm)) %in% aligned$docs.removed]
	lapply(seq_along(c(1:nrow(dtm))), function (x) {
		if (any(aligned$docs.removed %in% x)) {
			matrix(nrow = 2, ncol = 0)} else {
			aligned$documents[[which(as.numeric(names(aligned$documents)) == x)]]}
		})
}

aligned_dtm_body <- align_corpus(dtm_body, stm_models[[1]]$vocab)
aligned_dtm_subject <- align_corpus(dtm_subject, stm_models[[2]]$vocab)

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

estimated_topic_prevalences <- estimate_topic_prevalence(list(aligned_dtm_body, aligned_dtm_subject),stm_models, corpus_names)

fit_cluster <- function(var_cols, kfit_clusters){
	tmp <- scale(var_cols)

	closest.cluster <- function(x) {
	  cluster.dist <- apply(kfit_clusters$centers[, colnames(var_cols)], 1, function(y) sqrt(sum((x-y)^2)))
	  return(which.min(cluster.dist)[1])
	}
	
	apply(tmp, 1, closest.cluster)
}

cluster_assignment <- fit_cluster(estimated_topic_prevalences, kfit)

####
vset$client_rej_adj <- apply(vset, 1, function(x){ifelse(grepl(paste(non_rejection, collapse = '|'), x['rejection_reason']), 1, x['client'])})

fitteds_table <- ldply(apply(vset, 1, function(x) {
	col <- which(colnames(coef_table_cluster) == gsub(' ', '', x['fit_cluster']))
	predicts <- data.frame(NA)
	for (i in 1:nrow(coef_table_cluster)){
		prediction <- 1/(1 + exp(-coef_table_cluster[i, col]))
		tmp <- data.frame(prediction)
		colnames(tmp) <- coef_table_cluster[i, 'model_name']
		predicts <- cbind(predicts, tmp)}
	cbind(t(data.frame(x[c('message_id', 'client', 'client_rej_adj', 'rejection_reason', 'fit_cluster')])),
	 predicts[, c(2:ncol(predicts))])
	}), rbind)

wt_thresh_table <- data.frame()
for (i in 1:length(seq(0.5, 0.6, 0.01))) {
	l1 <- data.frame()
	for (j in 1:length(seq(0.5, 0.6, 0.01))) {
		tmp <- data.frame(seq(0,10,1), seq(0,10,1)[order(seq(0,1,.1), decreasing = T)],
			seq(0,10,1)[j], seq(0.5, 0.6, 0.01)[i])
		l1 <- rbind(l1, tmp)
	}
	wt_thresh_table <- rbind(wt_thresh_table, l1)
}
colnames(wt_thresh_table) <- c('contact_wt', 'contact_rej_adj_wt', 'email_wt' ,'threshold')

wt_thresh_table[, c('conf_rate', 'perc_suggested', 'chisq_p', 'rej_adj_conf_rate', 'chisq_p_rej_adj')] <- 
	ldply(apply(wt_thresh_table, 1, function(x){
	wts_total <- sum(x[c('contact_wt', 'contact_rej_adj_wt', 'email_wt')])
	ensemble <- (fitteds_table$logit_stm_contact_cluster * (x['contact_wt']/wts_total)) +
				(fitteds_table$logit_stm_contact_cluster_rej_adj * (x['contact_rej_adj_wt']/wts_total)) +
							(fitteds_table$logit_stm_email_cluster * (x['email_wt']/wts_total))
	predicted_status <- ifelse(ensemble > x['threshold'], 1, 0)
	data.frame(table(fitteds_table$client[predicted_status == 1])[2]/
	sum(table(fitteds_table$client[predicted_status == 1])),
	length(fitteds_table$client[predicted_status == 1])/length(fitteds_table$client),
	ifelse(length(unique(predicted_status)) > 1, chisq.test(fitteds_table$client, predicted_status)$p.value, NA),
	table(fitteds_table$client_rej_adj[predicted_status == 1])[2]/
	sum(table(fitteds_table$client_rej_adj[predicted_status == 1])),
	ifelse(length(unique(predicted_status)) > 1, chisq.test(fitteds_table$client_rej_adj, predicted_status)$p.value, NA))
	}), rbind)

wt_thresh_table[order(wt_thresh_table$conf_rate, decreasing = T),][1:50,]

contact_wt <- (6/13)
contact_rej_adj_wt <- (4/13)
email_wt <- (3/13)
email_rej_adj_wt <- 0

fitteds_table$ensemble <- (fitteds_table$logit_stm_contact_cluster * contact_wt) +
						(fitteds_table$logit_stm_contact_cluster_rej_adj * contact_rej_adj_wt) +
						(fitteds_table$logit_stm_email_cluster * email_wt) +
						(fitteds_table$logit_stm_email_cluster_rej_adj * email_rej_adj_wt)

fitteds_table$predicted_status <- ifelse(fitteds_table$ensemble > 0.58, 1, 0)

# performance on validation set
train_table <- rbind(round(cbind(t(table(compare$cluster)), total = nrow(compare))),
 round(cbind(table(compare$client, compare$cluster)/rbind(table(compare$cluster),
  table(compare$cluster)), total =  table(compare$client)/nrow(compare)), digits = 3))

v_table <- rbind(round(cbind(t(table(vset$fit_cluster)), total = nrow(vset))),
 round(cbind(table(vset$client, vset$fit_cluster)/rbind(table(vset$fit_cluster),
  table(vset$fit_cluster)), total =  table(vset$client)/nrow(vset)), digits = 3))

cli_clusters <- which(train_table['1',] > .5 & colnames(train_table) != 'total')

vset_narrow <- merge(vset, vset_email_ids, by = 'message_id')

############################

data <- dbGetQuery(connection, "SELECT * FROM datasci_projects.transaction_messages")

# pre-process corpus of email body text
corp_1 <- pre_process_corpus(data = data, text = 'body_text', stopword_lang = 'english')
corp_1_p <- round(.0075* length(corp_1))

tokenizer <-  function(x) {unlist(lapply(ngrams(words(x), 1), paste, collapse = " "), use.names = FALSE)}
corp_1_dtm1 <- as.matrix(DocumentTermMatrix(corp_1, 
                          control = list(tokenize = tokenizer, weighting = weightTf,
                                        bounds = list(global = c(corp_1_p, Inf)))))
print(noquote(paste(ncol(corp_1_dtm1), 'unique terms found.')))

# convert matrix into document-level list of vocab counts
docs <- apply(corp_1_dtm1, 1, function(x){
	tmp <- as.data.frame(x)
	tmp$vocab <- 1:nrow(tmp)
	tmp <- tmp[tmp[,1] >0,]
	tmp <- as.matrix.data.frame(t(tmp[, c(2,1)]))
	return(tmp)
	})

#### need process here to determine optimal k value
ksearch <- searchK(documents = docs, vocab = colnames(sparse_corp_1), K = c(7,10))
k <- 7

# cluster of topics
kmeans_iter <- function(data, maxcluster) {
  wss <- list()
  if (maxcluster >= nrow(data)) {print(noquote("Max cluster greater than or equal to N. Setting to N-1."))
    maxcluster <- nrow(data) - 1
    }
  for (i in 2:maxcluster) {
    wss[i] <- sum(kmeans(data,i)$withinss)
  }
  wss
}

maxcluster <- 7

tmp <- kmeans_iter(corp_1_dtm1, maxcluster)

sparse_corp_1 <- Matrix(corp_1_dtm1, sparse = TRUE)

stm_corp_1 <- stm(sparse_corp_1, K = k)

labelTopics(stm_corp_1)
topicCorr(stm_corp_1)

topdocs <- findThoughts(stm_corp_1, text =  data$body_text, n = 5)

# calculate FREX
w <- 0.5
topcon <- as.data.frame(t(exp(stm_corp_1$beta$logbeta[[1]])))
freq <- apply(topcon, 2, function(x) {ecdf(x)(x)})
excl <- t(apply(topcon, 1, function(x) {x/sum(x)}))
frex <- as.data.frame((w/freq + (1-w)/excl)^-1)

# calculate score (1 is highest)
topcon <- as.data.frame(stm_corp_1$beta$logbeta[[1]])
score <- as.data.frame(calcscore(topcon))

# calculate lift
topcon <- as.data.frame(t(exp(stm_corp_1$beta$logbeta[[1]])))
lift <- as.data.frame(t(apply(topcon, 1, function(x) {x/sum(x)})))

# comparing term metrics
frex$t_index <- row.names(frex)
frex <- melt(frex, id = 't_index')
frex$metric <- 'FREX'

freq <- as.data.frame(freq)
freq$t_index <- row.names(freq)
freq <- melt(freq, id = 't_index')
freq$metric <- 'freq'

excl <- as.data.frame(excl)
excl$t_index <- row.names(excl)
excl <- melt(excl, id = 't_index')
excl$metric <- 'excl'

score$value <- row.names(score)
score <- melt(score, id = 'value', value.name = 't_index')
score$metric <- 'score'

lift$t_index <- row.names(lift)
lift <- melt(lift, id = 't_index')
lift$metric <- 'lift'

topcon$t_index <- row.names(topcon)
topcon <- melt(topcon, id = 't_index')
topcon$metric <- 'probability'

term_metrics <- rbind(frex, score)
term_metrics <- rbind(term_metrics, lift)
term_metrics <- rbind(term_metrics, topcon)
term_metrics <- rbind(term_metrics, freq)
term_metrics <- rbind(term_metrics, excl)
 	
term_metrics <- merge(term_metrics, vocab, by = 't_index')
term_metrics <- dcast(term_metrics, term + variable ~ metric, value.var = 'value')

write.csv(term_metrics, 'term_metrics.csv', row.names = F)

# model comp stats
# semantic coherence - measures within topic coherence
M <- 10
beta <- stm_corp_1$beta$logbeta[[1]]
top.words <- apply(beta, 1, order, decreasing = TRUE)[1:M, ]
indices <- unlist(lapply(docs, "[", 1, ))
counts <- lapply(docs, "[", 2, )
VsubD <- unlist(lapply(counts, length))
rowsums <- unlist(lapply(counts, sum))
docids <- rep(1:length(docs), times = VsubD)
counts <- unlist(counts)
triplet <- list(i = as.integer(docids), j = as.integer(indices), 
    v = as.integer(counts), rowsums = as.integer(rowsums))
# count of each term in each (zero counts excluded)
mat <- slam::simple_triplet_matrix(triplet$i, triplet$j, 
            triplet$v, ncol = stm_corp_1$settings$dim$V)
wordlist <- unique(as.vector(top.words))
# pare down to only those terms which are in the top M highest beta in one or more topics
mat <- mat[, wordlist]
mat$v <- ifelse(mat$v > 1, 1, mat$v)
# occurrence/co-occurrence matrix
cross <- slam::tcrossprod_simple_triplet_matrix(t(mat))
temp <- match(as.vector(top.words), wordlist)
labels <- split(temp, rep(1:nrow(beta), each = M))
sem <- function(ml, cross) {
        m <- ml[1]
        l <- ml[2]
        log(0.01 + cross[m, l]) - log(cross[l, l] + 0.01)}
result <- vector(length = nrow(beta))
# for each of the topics, calculate the log difference between term occurrence and co-occurrence for
# the top.words, then sum the differences
for (k in 1:nrow(beta)) {
    grid <- expand.grid(labels[[k]], labels[[k]])
    colnames(grid) <- c("m", "l")
    grid <- grid[grid$m > grid$l, ]
    calc <- apply(grid, 1, sem, cross)
    result[k] <- sum(calc)}

# build LDAvis
phi <- exp(stm_corp_1$beta$logbeta[[1]])
theta <- stm_corp_1$theta
doc.length <- as.integer(sapply(docs, function(x) sum(x[2, ])))
vocab <- stm_corp_1$vocab
term.frequency <- as.integer(apply(corp_1_dtm1, 2, sum))

svd_tsne <- function(x) tsne(svd(x)$u)
json <- createJSON(
  phi = phi, 
  theta = theta, 
  doc.length = doc.length, 
  vocab = vocab, 
  term.frequency = term.frequency,
  mds.method = svd_tsne
)
serVis(json, out.dir = 'vis', 
       open.browser = FALSE)

# move to local machine and run 'python -m SimpleHTTPServer' in destination folder

# write to /Public for dashboard
setwd('~/Public')

vocab <- data.frame(t_index = 1:length(stm_corp_1$vocab), term = stm_corp_1$vocab)

topic_prevalence <- as.data.frame(stm_corp_1$theta)
topic_prevalence$doc_index <- row.names(topic_prevalence)
topic_prevalence <- melt(topic_prevalence, id = 'doc_index')
colnames(topic_prevalence)[colnames(topic_prevalence) %in% c('variable', 'value')] <- c('topic', 'probability')
topic_prevalence$topic <- as.integer(topic_prevalence$topic)

topic_content <- as.data.frame(t(exp(stm_corp_1$beta$logbeta[[1]])))
topic_content$t_index <- row.names(topic_content)
topic_content <- melt(topic_content, id = 't_index')
colnames(topic_content)[colnames(topic_content) %in% c('variable', 'value')] <- c('topic', 'probability')
topic_content$topic <- as.integer(topic_content$topic)

doc_length <- as.integer(sapply(docs, function(x) sum(x[2, ])))
doc_length <- data.frame(doc_index = 1:length(doc_length), n_terms = doc_length)

term_frequency <- as.integer(apply(corp_1_dtm1, 2, sum))
term_frequency <- data.frame(t_index = vocab$t_index, t_count = term_frequency)

pca_full <- prcomp(exp(stm_corp_1$beta$logbeta[[1]]), center = T, scale. = T)
pca <- as.data.frame(pca_full$x)
pca$topic <- 1:k
pca <- melt(pca, id = 'topic')
colnames(pca)[colnames(pca) == 'variable'] <- 'PC'

pca_stats <- as.data.frame(t(summary(pca_full)$importance))
pca_stats$PC <- row.names(pca_stats)

write.csv(vocab, 'stm_1_vocab.csv', row.names = F)
write.csv(topic_prevalence, 'stm_1_topic_prevalence.csv', row.names = F)
write.csv(topic_content, 'stm_1_topic_content.csv', row.names = F)
write.csv(doc_length, 'stm_1_doc_length.csv', row.names = F)
write.csv(term_frequency, 'stm_1_term_freq.csv', row.names = F)
write.csv(pca, 'stm_1_pca.csv', row.names = F)
write.csv(pca_stats, 'stm_1_pca_stats.csv', row.names = F)
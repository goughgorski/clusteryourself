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
library(caret)
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


## set near zero thresholds
nzv_dim_thresh <- 1000
nzv_fr_thresh <- 995/5
nzv_pctunq_thresh <- 1

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

# compute Idf
ln_idf <- log(sum(!is.na(emails$body_text))/apply(corp_body_dtm, 2, function(x) {sum(x>0)}))

# compute TfIdf
body_rhs <- corp_body_dtm * ln_idf
rownames(body_rhs) <- emails$message_id

#Near Zero Variance
nz_body_rhs <- nearZeroVar(body_rhs, freqCut = nzv_fr_thresh, uniqueCut = nzv_pctunq_thresh, saveMetrics = TRUE)

body_dict <- cbind(rep('body_stm',length(ln_idf)),colnames(corp_body_dtm),ln_idf)
colnames(body_dict) <- c('component','term', 'ln_idf')
nz_body_rhs <- cbind(term = rownames(nz_body_rhs), nz_body_rhs)
body_dict <- merge(body_dict, nz_body_rhs, by = 'term', all = TRUE)

nzv_used <- FALSE
if (ncol(body_rhs) > nzv_dim_thresh) {
  body_rhs <- body_rhs[,!(nz_body_rhs$nzv)]
  nzv_used  <- TRUE
}
body_dict$nzv_used <- nzv_used

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


body_dict <- merge(data.frame(term = stm_body_k5$vocab), body_dict, by = 'term', sort = FALSE)
dbWriteTable(connection, c('datasci_modeling','term_dictionaries'), value = as.data.frame(body_dict), overwrite = FALSE, append = TRUE, row.names = FALSE)

stm_body_k5_stripped <- strip_stm(stm_body_k5)

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

# compute Idf
ln_idf <- log(sum(!is.na(data$subject))/apply(corp_subject_dtm, 2, function(x) {sum(x>0)}))

# compute TfIdf
subject_rhs <- corp_subject_dtm * ln_idf
rownames(subject_rhs) <- data$message_id

#Near Zero Variance
nz_subject_rhs <- nearZeroVar(subject_rhs, freqCut = nzv_fr_thresh, uniqueCut = nzv_pctunq_thresh, saveMetrics = TRUE)

subject_dict <- cbind(rep('subject_stm',length(ln_idf)),colnames(corp_subject_dtm),ln_idf)
colnames(subject_dict) <- c('component','term', 'ln_idf')
nz_subject_rhs <- cbind(term = rownames(nz_subject_rhs), nz_subject_rhs)
subject_dict <- merge(subject_dict, nz_subject_rhs, by = 'term', all = TRUE)

nzv_used <- FALSE
if (ncol(subject_rhs) > nzv_dim_thresh) {
  subject_rhs <- subject_rhs[,!(nz_subject_rhs$nzv)]
  nzv_used  <- TRUE
}
subject_dict$nzv_used <- nzv_used 


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

subject_dict <- merge(data.frame(term = stm_subject_k5$vocab), subject_dict, by = 'term', sort = FALSE)
dbWriteTable(connection, c('datasci_modeling','term_dictionaries'), value = as.data.frame(subject_dict), overwrite = FALSE, append = TRUE, row.names = FALSE)

stm_subject_k5_stripped <- strip_stm(stm_subject_k5)

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
#data$n_terms_subject <- as.integer(sapply(docs_subject, function(x) sum(x[2,])))
#data$n_terms_body <- as.integer(sapply(docs_body, function(x) sum(x[2,])))


topic_prev_prep <- function(stm_out, model) {
	topic_prevalence <- as.data.frame(stm_out$theta)
	colnames(topic_prevalence) <- paste('T', 1:length(colnames(topic_prevalence)), '_',
 		paste(model, '_k', length(colnames(topic_prevalence)), sep = ''), sep = '')
 	topic_prevalence
}

for(i in 1:length(stm_models)){
    tmp <- topic_prev_prep(stm_models[[i]], stm_model_name[[i]])
    data <- cbind(data, tmp)}

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

kfit <- kmeans(compare[, kmeans_vars], 10)
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

# train on second corpus of emails
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

# Step 1: pre-process and tokenize body text and subject
tcorp_body <- pre_process_corpus(data = tset, text = 'body_text', stopword_lang = 'english', non_stopwords = c('you', 'i'))
#vcorp_body_p <- round(.0075* length(vcorp_body))
tcorp_body_p <- 0

ng <- nchar(stm_models[[1]]$vocab) - nchar(gsub(" ",'',stm_models[[1]]$vocab)) + 1

tokenizer <- function(x) {unlist(lapply(ngrams(sapply(words(x), depluralize), 1), paste, collapse = " "), use.names = FALSE)}
tcorp_body_dtm1 <- as.matrix(DocumentTermMatrix(tcorp_body, 
                          control = list(tokenize = tokenizer, weighting = weightTf,
                                        bounds = list(global = c(tcorp_body_p, Inf)), dictionary = stm_models[[1]]$vocab[ng == 1])))
tokenizer <-  function(x) {unlist(lapply(ngrams(sapply(words(x), depluralize), 2), paste, collapse = " "), use.names = FALSE)}
tcorp_body_dtm2 <- as.matrix(DocumentTermMatrix(tcorp_body, 
                          control = list(tokenize = tokenizer, weighting = weightTf,
                                        bounds = list(global = c(tcorp_body_p, Inf)), dictionary = stm_models[[1]]$vocab[ng == 2])))
tcorp_body_dtm <- cbind(tcorp_body_dtm1, tcorp_body_dtm2)
tokenizer <-  function(x) {unlist(lapply(ngrams(sapply(words(x), depluralize), 3), paste, collapse = " "), use.names = FALSE)}
tcorp_body_dtm3 <- as.matrix(DocumentTermMatrix(tcorp_body, 
                          control = list(tokenize = tokenizer, weighting = weightTf,
                                        bounds = list(global = c(tcorp_body_p, Inf)), dictionary = stm_models[[1]]$vocab[ng == 3])))
tcorp_body_dtm <- cbind(tcorp_body_dtm, tcorp_body_dtm3)

tdocs_body <- apply(tcorp_body_dtm, 1, function(x){
	tmp <- as.data.frame(x)
	tmp$vocab <- 1:nrow(tmp)
	tmp <- tmp[tmp[,1] >0,]
	tmp <- as.matrix.data.frame(t(tmp[, c(2,1)]))
	return(tmp)
	})

tcorp_body_dtm <- tcorp_body_dtm * as.numeric(as.character(body_dict[body_dict$nzv == F,'ln_idf'][colnames(tcorp_body_dtm)]))

tcorp_subject <- pre_process_corpus(data = tset, text = 'subject', stopword_lang = 'english', non_stopwords = c('you', 'i'))
#tcorp_subject_p <- round(.0075* length(vcorp_subject))
tcorp_subject_p <- 0

ng <- nchar(stm_models[[2]]$vocab) - nchar(gsub(" ",'',stm_models[[2]]$vocab)) + 1

tokenizer <- function(x) {unlist(lapply(ngrams(sapply(words(x), depluralize), 1), paste, collapse = " "), use.names = FALSE)}
tcorp_subject_dtm1 <- as.matrix(DocumentTermMatrix(tcorp_subject, 
                          control = list(tokenize = tokenizer, weighting = weightTf,
                                        bounds = list(global = c(tcorp_subject_p, Inf)), dictionary = stm_models[[2]]$vocab[ng == 1])))
tokenizer <-  function(x) {unlist(lapply(ngrams(sapply(words(x), depluralize), 2), paste, collapse = " "), use.names = FALSE)}
tcorp_subject_dtm2 <- as.matrix(DocumentTermMatrix(tcorp_subject, 
                          control = list(tokenize = tokenizer, weighting = weightTf,
                                        bounds = list(global = c(tcorp_subject_p, Inf)), dictionary = stm_models[[2]]$vocab[ng == 2])))
tcorp_subject_dtm <- cbind(tcorp_subject_dtm1, tcorp_subject_dtm2)

tdocs_subject <- apply(tcorp_subject_dtm, 1, function(x){
	tmp <- as.data.frame(x)
	tmp$vocab <- 1:nrow(tmp)
	tmp <- tmp[tmp[,1] >0,]
	tmp <- as.matrix.data.frame(t(tmp[, c(2,1)]))
	return(tmp)
	})

tcorp_subject_dtm <- tcorp_subject_dtm * as.numeric(as.character(subject_dict[,'ln_idf'][colnames(tcorp_subject_dtm)])) ### NEED TO ADD nzv == F ###

# Step 2: align processed texts to modeled vocab (replace removed docs with empty matrices)
tcorp_body_aligned <- alignCorpus(readCorpus(tcorp_body_dtm, type = 'dtm'), stm_models[[1]]$vocab)
names(tcorp_body_aligned$documents) <- c(1:nrow(tset))[!c(1:nrow(tset)) %in% tcorp_body_aligned$docs.removed]
tcorp_body_aligned <- lapply(seq_along(c(1:nrow(tset))), function (x) {
	if (any(tcorp_body_aligned$docs.removed %in% x)) {
		matrix(nrow = 2, ncol = 0)} else {
		tcorp_body_aligned$documents[[which(as.numeric(names(tcorp_body_aligned$documents)) == x)]]}
	})

tcorp_subject_aligned <- alignCorpus(readCorpus(tcorp_subject_dtm, type = 'dtm'), stm_models[[2]]$vocab)
names(tcorp_subject_aligned$documents) <- c(1:nrow(tset))[!c(1:nrow(tset)) %in% tcorp_subject_aligned$docs.removed]
tcorp_subject_aligned <- lapply(seq_along(c(1:nrow(tset))), function (x) {
	if (any(tcorp_subject_aligned$docs.removed %in% x)) {
		matrix(nrow = 2, ncol = 0)} else {
		tcorp_subject_aligned$documents[[which(as.numeric(names(tcorp_subject_aligned$documents)) == x)]]}
	})

# Step 3: estimate topic prevalences
tcorp_body_fit <- fitNewDocuments(stm_models[[1]], tcorp_body_aligned)

tcorp_subject_fit <- fitNewDocuments(stm_models[[2]], tcorp_subject_aligned)

topic_fit_prep <- function(tcorp_fit, model) {
	topic_prevalence <- as.data.frame(tcorp_fit$theta)
	colnames(topic_prevalence) <- paste('T', 1:length(colnames(topic_prevalence)), '_',
 		paste(model, '_k', length(colnames(topic_prevalence)), sep = ''), sep = '')
 	topic_prevalence
}

tcorp <- list(tcorp_body_fit, tcorp_subject_fit)
model_name <- c('body', 'subject')

for(i in 1:length(tcorp)){
    tmp <- topic_fit_prep(tcorp[[i]], model_name[[i]])
    tset <- cbind(tset, tmp)}

# Step 4: assign cluster
tset[, kmeans_vars] <- as.data.frame(sapply(seq_along(kmeans_vars), function(x) {
	(tset[, kmeans_vars[x]] - tp_scaling_table['mean', kmeans_vars[x]])/tp_scaling_table['sd', kmeans_vars[x]]
	}))

closest.cluster <- function(x) {
  cluster.dist <- apply(kfit$centers[, kmeans_vars], 1, function(y) sqrt(sum((x-y)^2)))
  return(which.min(cluster.dist)[1])
}

tset$fit_cluster <- apply(tset[, kmeans_vars], 1, closest.cluster)

tset$client_rej_adj <- apply(tset, 1, function(x){ifelse(grepl(paste(non_rejection, collapse = '|'), x['rejection_reason']), 1, x['client'])})

fitteds_table <- ldply(apply(tset, 1, function(x) {
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

fitteds_table$client <- as.numeric(as.character(fitteds_table$client))
thresh_iter_contact_rhs <- thresh_iter(0, 1, .01, fitteds_table, 'logit_stm_contact_cluster', 'client')
thresh_iter_contact_rej_adj_rhs <- thresh_iter(0, 1, .01, fitteds_table, 'logit_stm_contact_cluster_rej_adj', 'client')
thresh_iter_email_rhs <- thresh_iter(0, 1, .01, fitteds_table, 'logit_stm_email_cluster', 'client')
thresh_iter_rhs <- ldply(lapply(list(thresh_iter_contact_rhs, thresh_iter_email_rhs), function(x){
	x[which.max(x$inverse_distance),]}), rbind)
thresh_iter_rhs$model_name <- c('logit_stm_contact_cluster', 'logit_stm_email_cluster')
thresh_iter_rhs$model_weight <- thresh_iter_rhs$auc/sum(thresh_iter_rhs$auc)

fitteds_table$pred_rhs <- apply(fitteds_table, 1, function(x) {
	ensemble <- data.frame()
	for (i in 1:nrow(thresh_iter_rhs)){
		model_vote <- x[thresh_iter_rhs$model_name[i]] > thresh_iter_rhs$threshold[i]
		ensemble <- rbind(ensemble, model_vote)}
		ensemble$model_weight <- thresh_iter_rhs$model_weight
		vote_share <- sum(ensemble[,1] * ensemble[,2])
		vote_share >= .5
	})

# store STM models - model_storage function BROKEN: saves empty file to s3
model_storage(model_obj = stm_body_k5, location_folder = "stm_client_detection", model_script = "STM_client_detection.R",
strip_model_fn = strip_stm, model_grain = "document", model_response = "Proportional", model_outcome = "proportional",
 model_type = "Structured Topic Modeling")
s3save(stm_body_k5_stripped, bucket = "amitree-datascience/models/messages/stm_client_detection", object = "stm_body_k5.rda")
model_storage(model_obj = stm_subject_k5, location_folder = "stm_client_detection", model_script = "STM_client_detection.R",
strip_model_fn = strip_stm, model_grain = "document", model_response = "Proportional", model_outcome = "proportional",
 model_type = "Structured Topic Modeling")
s3save(stm_subject_k5_stripped, bucket = "amitree-datascience/models/messages/stm_client_detection", object = "stm_subject_k5.rda")

# store persistent logit models and threshold iterations
model_storage(model = logit_stm_contact_cluster, location_folder = "stm_client_detection", model_script = "STM_client_detection.R",
strip_model_fn = strip_model, model_grain = "message", model_response = "Binary", model_outcome = "binary", model_type = "Logistic Regression",
model_test = fitteds_table, model_test_cols = c('logit_stm_contact_cluster', 'client'))
s3save(logit_stm_contact_cluster, bucket = "amitree-datascience/models/messages/stm_client_detection", object = "logit_stm_contact_cluster.rda")

model_storage(model = logit_stm_email_cluster, location_folder = "stm_client_detection", model_script = "STM_client_detection.R",
strip_model_fn = strip_model, model_grain = "message", model_response = "Binary", model_outcome = "binary", model_type = "Logistic Regression",
model_test = fitteds_table, model_test_cols = c('logit_stm_email_cluster', 'client'))
s3save(logit_stm_email_cluster, bucket = "amitree-datascience/models/messages/stm_client_detection", object = "logit_stm_email_cluster.rda")

# store model transforms
tp_scaling_stats <- cbind(transform_type = 'Topic Prevalence Scaling', transform_data = toJSON(as.data.frame(tp_scaling_table)))
dbWriteTable(connection, c('datasci_modeling','model_transforms'), value = as.data.frame(tp_scaling_stats), overwrite = FALSE, append = TRUE, row.names = FALSE)

kfit_centroids <- cbind(transform_type = 'Kfit Centroids', transform_data = toJSON(as.data.frame(kfit$centers)))
dbWriteTable(connection, c('datasci_modeling','model_transforms'), value = as.data.frame(kfit_centroids), overwrite = FALSE, append = TRUE, row.names = FALSE)

model_data <- dbGetQuery(connection, "SELECT * from datasci_modeling.models")

#Store Ensemble Information
ensemble <- rep('STM Logit Ensemble 1', nrow(thresh_iter_rhs))
model_id <- model_data$id[model_data$model_name %in% paste(thresh_iter_rhs$model_name, '_stripped', sep = '')]
model_weight <- thresh_iter_rhs$model_weight
model_weight_type <- rep('AUC', nrow(thresh_iter_rhs))
active <- rep(TRUE, nrow(thresh_iter_rhs))
ensemble_data <- cbind(ensemble, model_id, model_weight, model_weight_type, active)

dbWriteTable(connection, c('datasci_modeling','ensembles'), value = as.data.frame(ensemble_data), overwrite = FALSE, append = TRUE, row.names = FALSE)


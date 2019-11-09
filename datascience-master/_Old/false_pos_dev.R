rm(list=ls())

#Load libraries
source('~/workspace/datascience/functions/load_libraries.R')
load_libraries('~/workspace/datascience/functions/')
options(stringsAsFactors = FALSE)
set.seed(123456789)
connection <- local_db_conect('development')

#Query to get data for Proposed Transactions
data <- dbGetQuery(connection, "SELECT *
                                FROM datasci_projects.vw_tx_modeling dspvtxm")

data$document_count <- ifelse(is.na(data$document_count),0,data$document_count)

table(!is.na(data$timeline_update), data$rejection_reason, data$event_type)
nrow(data)

#Clean Data
data <- data[!duplicated(data$message_id), ]
nrow(data)
data <- data[!is.na(data$created_at) & !is.na(data$tx_created_at) & data$daysfromtxcreate >= 0, ]
nrow(data)

###Outcomes

#Drop dirty outcome data
data <- data[!(data$event_type == 'PropertyTransactionConfirmedEvent' & data$representation == 'unknown'),]
data <- data[!(data$event_type == 'PropertyTransactionRejectedEvent' & !is.na(data$timeline_update)),]

#Outcomes
data$tx_binary <- ifelse(data$event_type == 'PropertyTransactionConfirmedEvent', 1, 0)
data$tx_binary_not_tx <- ifelse(data$event_type == 'PropertyTransactionConfirmedEvent', 1, ifelse(data$rejection_reason == 'not-a-transaction',0,NA))
data$tx_4cat <- ifelse(data$event_type == 'PropertyTransactionConfirmedEvent' & data$representation == 'buyer', 1,
                      ifelse(data$event_type == 'PropertyTransactionConfirmedEvent' & data$representation == 'seller', 2,
                      ifelse(data$event_type == 'PropertyTransactionConfirmedEvent' & data$representation == 'both', 3, 0)))

data$tx_5cat <- ifelse(data$event_type == 'PropertyTransactionConfirmedEvent' & data$representation == 'buyer' & !is.na(data$timeline_updates), 1,
                      ifelse(data$event_type == 'PropertyTransactionConfirmedEvent' & data$representation == 'buyer' & is.na(data$timeline_updates), 2,
                      ifelse(data$event_type == 'PropertyTransactionConfirmedEvent' & data$representation != 'buyer' & !is.na(data$timeline_updates), 3,
                      ifelse(data$event_type == 'PropertyTransactionConfirmedEvent' & data$representation != 'buyer' & is.na(data$timeline_updates), 4, 0))))

data$tx_7cat <- ifelse(data$event_type == 'PropertyTransactionConfirmedEvent' & data$representation == 'buyer' & !is.na(data$timeline_updates), 1,
                      ifelse(data$event_type == 'PropertyTransactionConfirmedEvent' & data$representation == 'buyer' & is.na(data$timeline_updates), 2,
                      ifelse(data$event_type == 'PropertyTransactionConfirmedEvent' & data$representation != 'buyer' & !is.na(data$timeline_updates), 3,
                      ifelse(data$event_type == 'PropertyTransactionConfirmedEvent' & data$representation != 'buyer' & is.na(data$timeline_updates), 4, 
                      ifelse(data$event_type == 'PropertyTransactionRejectedEvent' & data$rejection_reason %in% c('already-closed', 'fell-through'), -1,
                      ifelse(data$event_type == 'PropertyTransactionRejectedEvent' & data$rejection_reason %in% c('', 'other', 'rejected '), -2, 0))))))

#Message Timing
data$daysfromtx_con_or_rej_win <- mad_winsor(data$daysfromtx_con_or_rej, multiple = 10, na.rm = TRUE)
data$daysfromtxcreate_win <- mad_winsor(data$daysfromtxcreate, multiple = 10, na.rm = TRUE)

#Domain Separation
email_domain <- strsplit(data$from,'@')
data$email_domain <- ldply(email_domain,rbind)[,2]
data$freq <- 1
domain_freq <- aggregate(freq ~ email_domain, data = data, FUN = sum)
data <- merge(data, domain_freq, by = 'email_domain', all.x = T)
names(data)[names(data) == "freq.y"] <- "domain_freq"    
delta2 <- diff(diff(cumsum(table(domain_freq$freq)),lag = 1),lag = 1)
n_thresh <- as.numeric(names(delta2)[delta2==0][1])
print(noquote(paste("Sender email domain count threshold set at:", n_thresh, sep = ' ')))
email_domain_table <- as.data.frame(table(data$email_domain))
colnames(email_domain_table) <- c("email_domain","freq")
email_domain_table <- email_domain_table[order(email_domain_table$freq, decreasing=TRUE),]
email_domain_top50 <- data$email_domain %in% email_domain_table[1:50,1]

#data$email_domain_recat <- ifelse(data$domain_freq <= n_thresh | is.na(data$email_domain), 'small_n_domain', data$email_domain)
#data$email_domain_recat <- as.factor(data$email_domain_recat)

data$email_domain_recat <- ifelse(!email_domain_top50 | is.na(data$email_domain), 'small_n_domain', data$email_domain)
data$email_domain_recat <- as.factor(data$email_domain_recat)

#Check for message components
data$has_subject <- ifelse(!is.na(data$subject),1,0)
data$has_body <- ifelse(!is.na(data$body_text),1,0)
data$has_attachment <- ifelse(!is.na(data$attachment_names),1,0)

#Word Counts
data$body_characters <- nchar(data$body_text)
data$body_characters_win <- mad_winsor(data$body_characters, multiple = 5, na.rm = TRUE)

data$doc_characters <- nchar(data$document_content)
data$doc_characters_win <- mad_winsor(data$doc_characters, multiple = 5, na.rm = TRUE)

#Recode Missing Counts to Zero
data$body_characters_winr <- ifelse(is.na(data$body_characters_win),0,data$body_characters_win)
data$doc_characters_winr <- ifelse(is.na(data$doc_characters_win),0,data$doc_characters_win)

#Combine Base Data Features
base_numeric <- as.matrix(data[, c('daysfromtxcreate_win', 'daysfromtx_con_or_rej_win', 'body_characters_winr', 'doc_characters_winr')])
base_factor <-  as.data.frame(data[,'email_domain_recat'])
base_binary <- as.matrix(data[, c('has_subject', 'has_body', 'has_attachment')])
base_rhs <- cbind(base_numeric, base_factor, base_binary)
colnames(base_rhs) <- c('daysfromtxcreate_win', 'daysfromtx_con_or_rej_win', 'body_characters_winr', 'doc_characters_winr', 'email_domain_recat', 'has_subject', 'has_body', 'has_attachment')

#Segment Data
incremental <- data$tx_scan_type == 'incremental_scan'
initial <- data$tx_scan_type == 'initial_scan'

#Set Indicators
rand <- runif(nrow(data))
train <- rand<.7
test <- rand >=.7 & rand <.9
validation <- rand >=.9

#Set PCA thresholds
mar_var_inc_thresh <- .01
cum_var_thresh <- .90
nzv_dim_thresh <- 1000
nzv_fr_thresh <- 995/5
nzv_pctunq_thresh <- 1

####Create Corpora

###Subject Corpora
subject_corpus <- pre_process_corpus(data = data, text = 'subject', stopword_lang = 'english')
subject_corpus_p <- round(.01* length(subject_corpus))

tokenizer <-  function(x) {unlist(lapply(ngrams(words(x), 1), paste, collapse = " "), use.names = FALSE)}
subject_dtm <- as.matrix(DocumentTermMatrix(subject_corpus, 
                          control = list(tokenize = tokenizer, weighting = weightTf,
                                        bounds = list(global = c(subject_corpus_p, Inf)))))
print(noquote(paste(ncol(subject_dtm), 'unique terms found.')))

tokenizer <-  function(x) {unlist(lapply(ngrams(words(x), 2), paste, collapse = " "), use.names = FALSE)}
subject_dtm2 <- as.matrix(DocumentTermMatrix(subject_corpus, 
                          control = list(tokenize = tokenizer, weighting = weightTf,
                                        bounds = list(global = c(subject_corpus_p, Inf)))))
print(noquote(paste(ncol(subject_dtm2), 'unique terms found.')))

tokenizer <-  function(x) {unlist(lapply(ngrams(words(x), 3), paste, collapse = " "), use.names = FALSE)}
subject_dtm3 <- as.matrix(DocumentTermMatrix(subject_corpus, 
                          control = list(tokenize = tokenizer, weighting = weightTf,
                                        bounds = list(global = c(subject_corpus_p, Inf)))))
print(noquote(paste(ncol(subject_dtm3), 'unique terms found.')))

##Compute Idf
subject_rhs <- cbind(subject_dtm, subject_dtm2, subject_dtm3)
ln_idf <- log(sum(!is.na(data$subject))/apply(subject_rhs, 2, function(x) {sum(x>0)}))
subject_dict <- cbind(rep('subject',length(ln_idf)),c(colnames(subject_dtm),colnames(subject_dtm2),colnames(subject_dtm3)),ln_idf)
colnames(subject_dict) <- c('component','term', 'ln_idf')
print(noquote(paste(ncol(subject_rhs), 'unique terms will be used.')))

#Store Dictionary
dbWriteTable(connection, c('datasci_projects','term_dictionaries'), value = as.data.frame(subject_dict), overwrite = TRUE, append = FALSE, row.names = FALSE)

##Compute TfIdf
subject_rhs <- subject_rhs * ln_idf
rownames(subject_rhs) <- data$message_id

#Normalize DTM for storage
subject_rhs_ls <- list()
for (i in 1:ncol(subject_rhs)) {
  filter <- subject_rhs[,i]>0
  doc_id <- data$message_id[filter]
  email_part <- rep('subject', sum(filter))
  term <- rep(colnames(subject_rhs)[i], sum(filter))
  values <- subject_rhs[filter,i]
  subject_rhs_ls[[i]] <- cbind(doc_id, email_part, term, values)
}

subject_rhs_ls <- ldply(subject_rhs_ls, rbind)

#Store Normalized DTM
dbWriteTable(connection, c('datasci_projects','doc_term_matrices'), value = as.data.frame(subject_rhs_ls), overwrite = TRUE, append = FALSE, row.names = FALSE)

#Near Zero Variance and PCA
if (ncol(subject_rhs) > nzv_dim_thresh) {
  nz_subject_rhs <- nearZeroVar(subject_rhs, freqCut = nzv_fr_thresh, uniqueCut = nzv_pctunq_thresh, saveMetrics = TRUE)
  subject_rhs <- subject_rhs[,!(nz_subject_rhs$nzv)]
}

subject_pca <- prcomp(subject_rhs[train,], scale = TRUE)
cum_var <- cumsum(subject_pca$sdev/sum(subject_pca$sdev))
delta <- diff(cum_var,lag = 1)
cut <- c(TRUE,delta < mar_var_inc_thresh) & cum_var > cum_var_thresh
ncomp <- length(cum_var) - sum(cut)
subject_rhs_reduced <- subject_pca$x[,1:ncomp]
subject_rhs_pca <- predict(subject_pca,subject_rhs)
subject_rhs_pca <- subject_rhs_pca[,1:ncomp]

save.image('FalsePositives.Rdata')

###Attachment Name Corpora
attname_corpus <- pre_process_corpus(data = data, text = 'attachment_names', stopword_lang = 'english')
attname_corpus_p <- round(.01* length(attname_corpus))

tokenizer <-  function(x) {unlist(lapply(ngrams(words(x), 1), paste, collapse = " "), use.names = FALSE)}
attname_dtm <- as.matrix(DocumentTermMatrix(attname_corpus, 
                          control = list(tokenize = tokenizer, weighting = weightTf,
                                        bounds = list(global = c(attname_corpus_p, Inf)))))
print(noquote(paste(ncol(attname_dtm), 'unique terms found.')))

tokenizer <-  function(x) {unlist(lapply(ngrams(words(x), 2), paste, collapse = " "), use.names = FALSE)}
attname_dtm2 <- as.matrix(DocumentTermMatrix(attname_corpus, 
                          control = list(tokenize = tokenizer, weighting = weightTf,
                                        bounds = list(global = c(attname_corpus_p, Inf)))))
print(noquote(paste(ncol(attname_dtm2), 'unique terms found.')))

tokenizer <-  function(x) {unlist(lapply(ngrams(words(x), 3), paste, collapse = " "), use.names = FALSE)}
attname_dtm3 <- as.matrix(DocumentTermMatrix(attname_corpus, 
                          control = list(tokenize = tokenizer, weighting = weightTf,
                                        bounds = list(global = c(attname_corpus_p, Inf)))))
print(noquote(paste(ncol(attname_dtm3), 'unique terms found.')))

##Compute Idf
attname_rhs <- cbind(attname_dtm, attname_dtm2, attname_dtm3)
ln_idf <- log(sum(!is.na(data$attachment_names))/apply(attname_rhs, 2, function(x) {sum(x>0)}))
attname_dict <- cbind(rep('attachment_names',length(ln_idf)),c(colnames(attname_dtm),colnames(attname_dtm2),colnames(attname_dtm3)),ln_idf)
colnames(attname_dict) <- c('component','term', 'ln_idf')
print(noquote(paste(ncol(attname_rhs), 'unique terms will be used.')))

#Store Dictionary
dbWriteTable(connection, c('datasci_projects','term_dictionaries'), value = as.data.frame(attname_dict), overwrite = FALSE, append = TRUE, row.names = FALSE)

##Compute TfIdf
attname_rhs <- attname_rhs * ln_idf

#Normalize DTM for storage
attname_rhs_ls <- list()
for (i in 1:ncol(attname_rhs)) {
  filter <- attname_rhs[,i]>0
  doc_id <- data$message_id[filter]
  email_part <- rep('attname', sum(filter))
  term <- rep(colnames(attname_rhs)[i], sum(filter))
  values <- attname_rhs[filter,i]
  attname_rhs_ls[[i]] <- cbind(doc_id, email_part, term, values)
}

attname_rhs_ls <- ldply(attname_rhs_ls, rbind)

#Store Normalized DTM
dbWriteTable(connection, c('datasci_projects','doc_term_matrices'), value = as.data.frame(attname_rhs_ls), overwrite = FALSE, append = TRUE, row.names = FALSE)

#Near Zero Variance and PCA
if (ncol(attname_rhs) > nzv_dim_thresh) {
  nz_attname_rhs <- nearZeroVar(attname_rhs, freqCut = nzv_fr_thresh, uniqueCut = nzv_pctunq_thresh, saveMetrics = TRUE)
  attname_rhs <- attname_rhs[,!(nz_attname_rhs$nzv)]
}

attname_pca <- prcomp(attname_rhs[train,], scale = TRUE)
cum_var <- cumsum(attname_pca$sdev/sum(attname_pca$sdev))
delta <- diff(cum_var,lag = 1)
cut <- c(TRUE,delta < mar_var_inc_thresh) & cum_var > cum_var_thresh
ncomp <- length(cum_var) - sum(cut)
attname_rhs_reduced <- attname_pca$x[,1:ncomp]
attname_rhs_pca <- predict(attname_pca,attname_rhs)
attname_rhs_pca <- attname_rhs_pca[,1:ncomp]

save.image('FalsePositives.Rdata')

###Email Txt Corpora
emailtxt_corpus <- pre_process_corpus(data = data, text = 'body_text', stopword_lang = 'english')
emailtxt_corpus_p <- round(.01* length(emailtxt_corpus))

tokenizer <-  function(x) {unlist(lapply(ngrams(words(x), 1), paste, collapse = " "), use.names = FALSE)}
emailtxt_dtm <- as.matrix(DocumentTermMatrix(emailtxt_corpus, 
                          control = list(tokenize = tokenizer, weighting = weightTf,
                                        bounds = list(global = c(emailtxt_corpus_p, Inf)))))
print(noquote(paste(ncol(emailtxt_dtm), 'unique terms found.')))

tokenizer <-  function(x) {unlist(lapply(ngrams(words(x), 2), paste, collapse = " "), use.names = FALSE)}
emailtxt_dtm2 <- as.matrix(DocumentTermMatrix(emailtxt_corpus, 
                          control = list(tokenize = tokenizer, weighting = weightTf,
                                        bounds = list(global = c(emailtxt_corpus_p, Inf)))))
print(noquote(paste(ncol(emailtxt_dtm2), 'unique terms found.')))

tokenizer <-  function(x) {unlist(lapply(ngrams(words(x), 3), paste, collapse = " "), use.names = FALSE)}
emailtxt_dtm3 <- as.matrix(DocumentTermMatrix(emailtxt_corpus, 
                          control = list(tokenize = tokenizer, weighting = weightTf,
                                        bounds = list(global = c(emailtxt_corpus_p, Inf)))))
print(noquote(paste(ncol(emailtxt_dtm3), 'unique terms found.')))

##Compute Idf
emailtxt_rhs <- cbind(emailtxt_dtm, emailtxt_dtm2, emailtxt_dtm3)
ln_idf <- log(sum(!is.na(data$body_text))/apply(emailtxt_rhs, 2, function(x) {sum(x>0)}))
emailtxt_dict <- cbind(rep('body_text',length(ln_idf)),c(colnames(emailtxt_dtm),colnames(emailtxt_dtm2),colnames(emailtxt_dtm3)),ln_idf)
colnames(emailtxt_dict) <- c('component','term', 'ln_idf')
print(noquote(paste(ncol(emailtxt_rhs), 'unique terms will be used.')))

#Store Dictionary
dbWriteTable(connection, c('datasci_projects','term_dictionaries'), value = as.data.frame(emailtxt_dict), overwrite = FALSE, append = TRUE, row.names = FALSE)

##Compute TfIdf
emailtxt_rhs <- emailtxt_rhs * ln_idf

emailtxt_rhs_ls <- list()
for (i in 1:ncol(emailtxt_rhs)) {
  filter <- emailtxt_rhs[,i]>0
  doc_id <- data$message_id[filter]
  email_part <- rep('emailtxt', sum(filter))
  term <- rep(colnames(emailtxt_rhs)[i], sum(filter))
  values <- emailtxt_rhs[filter,i]
  emailtxt_rhs_ls[[i]] <- cbind(doc_id, email_part, term, values)
}

emailtxt_rhs_ls <- ldply(emailtxt_rhs_ls, rbind)

#Store Normalized DTM
dbWriteTable(connection, c('datasci_projects','doc_term_matrices'), value = as.data.frame(emailtxt_rhs_ls), overwrite = FALSE, append = TRUE, row.names = FALSE)

#Near Zero Variance and PCA
if (ncol(emailtxt_rhs) > nzv_dim_thresh) {
  nz_emailtxt_rhs <- nearZeroVar(emailtxt_rhs, freqCut = nzv_fr_thresh, uniqueCut = nzv_pctunq_thresh, saveMetrics = TRUE)
  emailtxt_rhs <- emailtxt_rhs[,!(nz_emailtxt_rhs$nzv)]
}

emailtxt_pca <- prcomp(emailtxt_rhs[train,], scale = TRUE)
cum_var <- cumsum(emailtxt_pca$sdev/sum(emailtxt_pca$sdev))
delta <- diff(cum_var,lag = 1)
cut <- c(TRUE,delta < mar_var_inc_thresh) & cum_var > cum_var_thresh
ncomp <- length(cum_var) - sum(cut)
print(cum_var[ncomp])
emailtxt_rhs_reduced <- emailtxt_pca$x[,1:ncomp]
emailtxt_rhs_pca <- predict(emailtxt_pca,emailtxt_rhs)
emailtxt_rhs_pca <- emailtxt_rhs_pca[,1:ncomp]

save.image('FalsePositives.Rdata')

#Document Txt Corpora
t0 <- Sys.time()
doctxt_corpus <- pre_process_corpus(data = data, text = 'document_content', stopword_lang = 'english', remove_letters = TRUE)
print(difftime(Sys.time(), t0, units = 'min'))
doctxt_corpus_p <- round(.01* length(doctxt_corpus))

tokenizer <-  function(x) {unlist(lapply(ngrams(words(x), 1), paste, collapse = " "), use.names = FALSE)}
doctxt_dtm <- as.matrix(DocumentTermMatrix(doctxt_corpus, 
                          control = list(tokenize = tokenizer, weighting = weightTf,
                                        bounds = list(global = c(doctxt_corpus_p, Inf)))))
print(noquote(paste(ncol(doctxt_dtm), 'unique terms found.')))

tokenizer <-  function(x) {unlist(lapply(ngrams(words(x), 2), paste, collapse = " "), use.names = FALSE)}
doctxt_dtm2 <- as.matrix(DocumentTermMatrix(doctxt_corpus, 
                          control = list(tokenize = tokenizer, weighting = weightTf,
                                        bounds = list(global = c(doctxt_corpus_p, Inf)))))
print(noquote(paste(ncol(doctxt_dtm2), 'unique terms found.')))

tokenizer <-  function(x) {unlist(lapply(ngrams(words(x), 3), paste, collapse = " "), use.names = FALSE)}
doctxt_dtm3 <- as.matrix(DocumentTermMatrix(doctxt_corpus, 
                          control = list(tokenize = tokenizer, weighting = weightTf,
                                        bounds = list(global = c(doctxt_corpus_p, Inf)))))
print(noquote(paste(ncol(doctxt_dtm3), 'unique terms found.')))

##Compute Idf
doctxt_rhs <- cbind(doctxt_dtm, doctxt_dtm2, doctxt_dtm3)
ln_idf <- log(sum(!is.na(data$document_content))/apply(doctxt_rhs, 2, function(x) {sum(x>0)}))
doctxt_dict <- cbind(rep('document_content',length(ln_idf)),c(colnames(doctxt_dtm),colnames(doctxt_dtm2),colnames(doctxt_dtm3)),ln_idf)
colnames(doctxt_dict) <- c('component','term', 'ln_idf')
print(noquote(paste(ncol(doctxt_rhs), 'unique subject terms will be used.')))

#Store Dictionary
dbWriteTable(connection, c('datasci_projects','term_dictionaries'), value = as.data.frame(doctxt_dict), overwrite = FALSE, append = TRUE, row.names = FALSE)

##Compute TfIdf
doctxt_rhs <- doctxt_rhs * ln_idf

#Store Normalized DTMs for all Componets
doctxt_rhs_ls <- list()
for (i in 1:ncol(doctxt_rhs)) {
  filter <- doctxt_rhs[,i]>0
  doc_id <- data$message_id[filter]
  email_part <- rep('doctxt', sum(filter))
  term <- rep(colnames(doctxt_rhs)[i], sum(filter))
  values <- doctxt_rhs[filter,i]
  doctxt_rhs_ls[[i]] <- cbind(doc_id, email_part, term, values)
}

doctxt_rhs_ls <- ldply(doctxt_rhs_ls, rbind)

#Store DTMs
dbWriteTable(connection, c('datasci_projects','doc_term_matrices'), value = as.data.frame(doctxt_rhs_ls), overwrite = FALSE, append = TRUE, row.names = FALSE)

#Near Zero Variance and PCA
if (ncol(doctxt_rhs) > nzv_dim_thresh) {
  nz_doctxt_rhs <- nearZeroVar(doctxt_rhs, saveMetrics = TRUE)
  doctxt_rhs <- doctxt_rhs[,!(nz_doctxt_rhs$nzv)]
}

doctxt_pca <- prcomp(doctxt_rhs[train,], scale = TRUE)
cum_var <- cumsum(doctxt_pca$sdev/sum(doctxt_pca$sdev))
delta <- diff(cum_var,lag = 1)
cut <- c(TRUE,delta < mar_var_inc_thresh) & cum_var > cum_var_thresh
ncomp <- length(cum_var) - sum(cut)
print(cum_var[ncomp])
doctxt_rhs_reduced <- doctxt_pca$x[,1:ncomp]
doctxt_rhs_pca <- predict(doctxt_pca,doctxt_rhs)
doctxt_rhs_pca <- doctxt_rhs_pca[,1:ncomp]

#Clean Up
rm(filter, doc_id, email_part, term, values, ln_idf, subject_rhs_ls, attname_rhs_ls, emailtxt_rhs_ls, doctxt_rhs_ls)
gc()

#Save Image
save.image('FalsePositives.Rdata')

####Modeling 
##Rf Settings
max_mtry = 30

#Initial Scan Model

#Random Forest Models - Incremental Scan - Basic
t0 <- Sys.time()
rft_base_2cat <- tuneRF(y = as.factor(data[train & incremental, 'tx_binary']), x = cbind(base_rhs[train & incremental,]), trace = TRUE, improve=0.0005, mtryStart = min(round(sqrt(ncol(base_rhs[train & incremental,]))), max_mtry))
rft_base_4cat <- tuneRF(y = as.factor(data[train & incremental, 'tx_4cat']), x = cbind(base_rhs[train & incremental,]), trace = TRUE, improve=0.0005, mtryStart = min(round(sqrt(ncol(base_rhs[train & incremental,]))), max_mtry))
rft_base_5cat <- tuneRF(y = as.factor(data[train & incremental, 'tx_5cat']), x = cbind(base_rhs[train & incremental,]), trace = TRUE, improve=0.0005, mtryStart = min(round(sqrt(ncol(base_rhs[train & incremental,]))), max_mtry))
rft_base_7cat <- tuneRF(y = as.factor(data[train & incremental, 'tx_7cat']), x = cbind(base_rhs[train & incremental,]), trace = TRUE, improve=0.0005, mtryStart = min(round(sqrt(ncol(base_rhs[train & incremental,]))), max_mtry))
#rft_base_9cat <- tuneRF(y = as.factor(data[train & incremental, 'tx_9cat']), x = cbind(base_rhs[train & incremental,]), trace = TRUE, improve=0.0005, mtryStart = min(round(sqrt(ncol(base_rhs[train & incremental,]))), max_mtry))
print(difftime(Sys.time(), t0, units = 'sec'))

t0 <- Sys.time()
rf_base_2cat <- randomForest(y = as.factor(data[train & incremental, 'tx_binary']),x = base_rhs[train & incremental,], do.trace = TRUE, ntree = 500, mtry = rft_base_2cat[rft_base_2cat[,2] == min(rft_base_2cat[,2]),1], nodesize = 1000)
rf_base_4cat <- randomForest(y = as.factor(data[train & incremental, 'tx_4cat']),x = base_rhs[train & incremental,], do.trace = TRUE, ntree = 500, mtry = rft_base_4cat[rft_base_4cat[,2] == min(rft_base_4cat[,2]),1], nodesize = 1000)
rf_base_5cat <- randomForest(y = as.factor(data[train & incremental, 'tx_5cat']),x = base_rhs[train & incremental,], do.trace = TRUE, ntree = 500, mtry = rft_base_5cat[rft_base_5cat[,2] == min(rft_base_5cat[,2]),1], nodesize = 1000)
rf_base_7cat <- randomForest(y = as.factor(data[train & incremental, 'tx_7cat']),x = base_rhs[train & incremental,], do.trace = TRUE, ntree = 500, mtry = rft_base_7cat[rft_base_7cat[,2] == min(rft_base_7cat[,2]),1], nodesize = 1000)
#rf_base_9cat <- randomForest(y = as.factor(data[train & incremental, 'tx_9cat']),x = base_rhs[train & incremental,], do.trace = TRUE, ntree = 500, mtry = rft_base_9cat[rft_base_9cat[,2] == min(rft_base_9cat[,2]),1], nodesize = 1000)
print(difftime(Sys.time(), t0, units = 'sec'))

rf_base_pred <- cbind.data.frame(data[train & incremental, 'tx_binary'], 
              data[train & incremental, 'tx_4cat'], 
              data[train & incremental, 'tx_5cat'],
              data[train & incremental, 'tx_7cat'],               
              rf_base_2cat$votes[,ncol(rf_base_2cat$votes)],
              rowSums(rf_base_4cat$votes[,c(2:ncol(rf_base_4cat$votes))]),
              rowSums(rf_base_5cat$votes[,c(2:ncol(rf_base_5cat$votes))]),
              rowSums(rf_base_7cat$votes[,c(4:ncol(rf_base_7cat$votes))])
              )

#              data[train & incremental, 'tx_9cat'],               
#             rowSums(rf_base_9cat$votes[,c(2:ncol(rf_base_9cat$votes))])

colnames(rf_base_pred) <- c('tx_binary', 'tx_4cat', 'tx_5cat', 'tx_7cat',  
                            'prob_con2', 'prob_con4', 'prob_con5', 'prob_con7')
#'tx_9cat', , 'prob_con9'

rf_base_pred_rocset_2cat <- thresh_iter(.0, 1, .01, rf_base_pred, 'prob_con2', 'tx_binary')
rf_base_pred_rocset_2cat[rf_base_pred_rocset_2cat$inv_distance==max(rf_base_pred_rocset_2cat$inv_distance),]
rf_base_pred_rocset_4cat <- thresh_iter(.0, 1, .01, rf_base_pred, 'prob_con4', 'tx_binary')
rf_base_pred_rocset_4cat[rf_base_pred_rocset_4cat$inv_distance==max(rf_base_pred_rocset_4cat$inv_distance),]
rf_base_pred_rocset_5cat <- thresh_iter(.0, 1, .01, rf_base_pred, 'prob_con5', 'tx_binary')
rf_base_pred_rocset_5cat[rf_base_pred_rocset_5cat$inv_distance==max(rf_base_pred_rocset_5cat$inv_distance),]
rf_base_pred_rocset_7cat <- thresh_iter(.0, 1, .01, rf_base_pred, 'prob_con7', 'tx_binary')
rf_base_pred_rocset_7cat[rf_base_pred_rocset_7cat$inv_distance==max(rf_base_pred_rocset_7cat$inv_distance),]
#rf_base_pred_rocset_9cat <- thresh_iter(.0, 1, .01, rf_base_pred, 'prob_con9', 'tx_binary')
#rf_base_pred_rocset_9cat[rf_base_pred_rocset_9cat$inv_distance==max(rf_base_pred_rocset_9cat$inv_distance),]

#Save Image
save.image('FalsePositives.Rdata')

#Random Forest Models - Incremental Scan - Subject
t0 <- Sys.time()
rft_sub_2cat <- tuneRF(y = as.factor(data[train & incremental, 'tx_binary']), x = cbind(subject_rhs[train & incremental,]), trace = TRUE, improve=0.0005, mtryStart = min(round(sqrt(ncol(subject_rhs[train & incremental,]))), max_mtry))
rft_sub_4cat <- tuneRF(y = as.factor(data[train & incremental, 'tx_4cat']), x = cbind(subject_rhs[train & incremental,]), trace = TRUE, improve=0.0005, mtryStart = min(round(sqrt(ncol(subject_rhs[train & incremental,]))), max_mtry))
rft_sub_5cat <- tuneRF(y = as.factor(data[train & incremental, 'tx_5cat']), x = cbind(subject_rhs[train & incremental,]), trace = TRUE, improve=0.0005, mtryStart = min(round(sqrt(ncol(subject_rhs[train & incremental,]))), max_mtry))
rft_sub_7cat <- tuneRF(y = as.factor(data[train & incremental, 'tx_7cat']), x = cbind(subject_rhs[train & incremental,]), trace = TRUE, improve=0.0005, mtryStart = min(round(sqrt(ncol(subject_rhs[train & incremental,]))), max_mtry))
#rft_sub_9cat <- tuneRF(y = as.factor(data[train & incremental, 'tx_9cat']), x = cbind(subject_rhs[train & incremental,]), trace = TRUE, improve=0.0005, mtryStart = min(round(sqrt(ncol(subject_rhs[train & incremental,]))), max_mtry))
print(difftime(Sys.time(), t0, units = 'sec'))

t0 <- Sys.time()
rf_sub_2cat <- randomForest(y = as.factor(data[train & incremental, 'tx_binary']),x = subject_rhs[train & incremental,], do.trace = TRUE, ntree = 500, mtry = rft_sub_2cat[rft_sub_2cat[,2] == min(rft_sub_2cat[,2]),1], nodesize = 1000)
rf_sub_4cat <- randomForest(y = as.factor(data[train & incremental, 'tx_4cat']),x = subject_rhs[train & incremental,], do.trace = TRUE, ntree = 500, mtry = rft_sub_4cat[rft_sub_4cat[,2] == min(rft_sub_4cat[,2]),1], nodesize = 1000)
rf_sub_5cat <- randomForest(y = as.factor(data[train & incremental, 'tx_5cat']),x = subject_rhs[train & incremental,], do.trace = TRUE, ntree = 500, mtry = rft_sub_5cat[rft_sub_5cat[,2] == min(rft_sub_5cat[,2]),1], nodesize = 1000)
rf_sub_7cat <- randomForest(y = as.factor(data[train & incremental, 'tx_7cat']),x = subject_rhs[train & incremental,], do.trace = TRUE, ntree = 500, mtry = rft_sub_7cat[rft_sub_7cat[,2] == min(rft_sub_7cat[,2]),1], nodesize = 1000)
#rf_sub_9cat <- randomForest(y = as.factor(data[train & incremental, 'tx_9cat']),x = subject_rhs[train & incremental,], do.trace = TRUE, ntree = 500, mtry = rft_sub_9cat[rft_sub_9cat[,2] == min(rft_sub_9cat[,2]),1], nodesize = 1000)
print(difftime(Sys.time(), t0, units = 'sec'))

rf_sub_pred <- cbind.data.frame(data[train & incremental, 'tx_binary'], 
              data[train & incremental, 'tx_4cat'], 
              data[train & incremental, 'tx_5cat'],
              data[train & incremental, 'tx_7cat'],               
              rf_sub_2cat$votes[,ncol(rf_sub_2cat$votes)],
              rowSums(rf_sub_4cat$votes[,c(2:ncol(rf_sub_4cat$votes))]),
              rowSums(rf_sub_5cat$votes[,c(2:ncol(rf_sub_5cat$votes))]),
              rowSums(rf_sub_7cat$votes[,c(4:ncol(rf_sub_7cat$votes))])              
              )
            
#            data[train & incremental, 'tx_9cat'],               
#            rowSums(rf_sub_9cat$votes[,c(2:ncol(rf_sub_9cat$votes))])
 
colnames(rf_sub_pred) <- c('tx_binary', 'tx_4cat', 'tx_5cat', 'tx_7cat', 
                            'prob_con2', 'prob_con4', 'prob_con5', 'prob_con7')
#'tx_9cat', , 'prob_con9'

rf_sub_pred_rocset_2cat <- thresh_iter(.0, 1, .01, rf_sub_pred, 'prob_con2', 'tx_binary')
rf_sub_pred_rocset_2cat[rf_sub_pred_rocset_2cat$inv_distance==max(rf_sub_pred_rocset_2cat$inv_distance),]
rf_sub_pred_rocset_4cat <- thresh_iter(.0, 1, .01, rf_sub_pred, 'prob_con4', 'tx_binary')
rf_sub_pred_rocset_4cat[rf_sub_pred_rocset_4cat$inv_distance==max(rf_sub_pred_rocset_4cat$inv_distance),]
rf_sub_pred_rocset_5cat <- thresh_iter(.0, 1, .01, rf_sub_pred, 'prob_con5', 'tx_binary')
rf_sub_pred_rocset_5cat[rf_sub_pred_rocset_5cat$inv_distance==max(rf_sub_pred_rocset_5cat$inv_distance),]
rf_sub_pred_rocset_7cat <- thresh_iter(.0, 1, .01, rf_sub_pred, 'prob_con7', 'tx_binary')
rf_sub_pred_rocset_7cat[rf_sub_pred_rocset_7cat$inv_distance==max(rf_sub_pred_rocset_7cat$inv_distance),]
#rf_sub_pred_rocset_9cat <- thresh_iter(.0, 1, .01, rf_sub_pred, 'prob_con9', 'tx_binary')
#rf_sub_pred_rocset_9cat[rf_sub_pred_rocset_9cat$inv_distance==max(rf_sub_pred_rocset_9cat$inv_distance),]

#Save Image
save.image('FalsePositives.Rdata')

#Random Forest Models - Incremental Scan - Attachment Names
t0 <- Sys.time()
subsamp <- train
rft_att_2cat <- tuneRF(y = as.factor(data[train & incremental, 'tx_binary']), x = cbind(attname_rhs[train & incremental,]), trace = TRUE, improve=0.0005, mtryStart = min(round(sqrt(ncol(attname_rhs[train & incremental,]))), max_mtry))
rft_att_4cat <- tuneRF(y = as.factor(data[train & incremental, 'tx_4cat']), x = cbind(attname_rhs[train & incremental,]), trace = TRUE, improve=0.0005, mtryStart = min(round(sqrt(ncol(attname_rhs[train & incremental,]))), max_mtry))
rft_att_5cat <- tuneRF(y = as.factor(data[train & incremental, 'tx_5cat']), x = cbind(attname_rhs[train & incremental,]), trace = TRUE, improve=0.0005, mtryStart = min(round(sqrt(ncol(attname_rhs[train & incremental,]))), max_mtry))
rft_att_7cat <- tuneRF(y = as.factor(data[train & incremental, 'tx_7cat']), x = cbind(attname_rhs[train & incremental,]), trace = TRUE, improve=0.0005, mtryStart = min(round(sqrt(ncol(attname_rhs[train & incremental,]))), max_mtry))
#rft_att_9cat <- tuneRF(y = as.factor(data[train & incremental, 'tx_9cat']), x = cbind(attname_rhs[train & incremental,]), trace = TRUE, improve=0.0005, mtryStart = min(round(sqrt(ncol(attname_rhs[train & incremental,]))), max_mtry))
print(difftime(Sys.time(), t0, units = 'sec'))

t0 <- Sys.time()
rf_att_2cat <- randomForest(y = as.factor(data[train & incremental, 'tx_binary']),x = attname_rhs[train & incremental,], do.trace = TRUE, ntree = 500, mtry = rft_att_2cat[rft_att_2cat[,2] == min(rft_att_2cat[,2]),1], nodesize = 1000)
rf_att_4cat <- randomForest(y = as.factor(data[train & incremental, 'tx_4cat']),x = attname_rhs[train & incremental,], do.trace = TRUE, ntree = 500, mtry = rft_att_4cat[rft_att_4cat[,2] == min(rft_att_4cat[,2]),1], nodesize = 1000)
rf_att_5cat <- randomForest(y = as.factor(data[train & incremental, 'tx_5cat']),x = attname_rhs[train & incremental,], do.trace = TRUE, ntree = 500, mtry = rft_att_5cat[rft_att_5cat[,2] == min(rft_att_5cat[,2]),1], nodesize = 1000)
rf_att_7cat <- randomForest(y = as.factor(data[train & incremental, 'tx_7cat']),x = attname_rhs[train & incremental,], do.trace = TRUE, ntree = 500, mtry = rft_att_7cat[rft_att_7cat[,2] == min(rft_att_7cat[,2]),1], nodesize = 1000)
#rf_att_9cat <- randomForest(y = as.factor(data[train & incremental, 'tx_9cat']),x = attname_rhs[train & incremental,], do.trace = TRUE, ntree = 500, mtry = rft_att_9cat[rft_att_9cat[,2] == min(rft_att_9cat[,2]),1], nodesize = 1000)
print(difftime(Sys.time(), t0, units = 'sec'))

rf_att_pred <- cbind.data.frame(data[train & incremental, 'tx_binary'], 
              data[train & incremental, 'tx_4cat'], 
              data[train & incremental, 'tx_5cat'],
              data[train & incremental, 'tx_7cat'],               
              rf_att_2cat$votes[,ncol(rf_att_2cat$votes)],
              rowSums(rf_att_4cat$votes[,c(2:ncol(rf_att_4cat$votes))]),
              rowSums(rf_att_5cat$votes[,c(2:ncol(rf_att_5cat$votes))]),
              rowSums(rf_att_7cat$votes[,c(4:ncol(rf_att_7cat$votes))])               
              )

#              data[train & incremental, 'tx_9cat'],               
#              rowSums(rf_att_9cat$votes[,c(2:ncol(rf_att_9cat$votes))])

colnames(rf_att_pred) <- c('tx_binary', 'tx_4cat', 'tx_5cat', 'tx_7cat',
                            'prob_con2', 'prob_con4', 'prob_con5', 'prob_con7')
#'tx_9cat', , 'prob_con9'

rf_att_pred_rocset_2cat <- thresh_iter(.0, 1, .01, rf_att_pred, 'prob_con2', 'tx_binary')
rf_att_pred_rocset_2cat[rf_att_pred_rocset_2cat$inv_distance==max(rf_att_pred_rocset_2cat$inv_distance),]
rf_att_pred_rocset_4cat <- thresh_iter(.0, 1, .01, rf_att_pred, 'prob_con4', 'tx_binary')
rf_att_pred_rocset_4cat[rf_att_pred_rocset_4cat$inv_distance==max(rf_att_pred_rocset_4cat$inv_distance),]
rf_att_pred_rocset_5cat <- thresh_iter(.0, 1, .01, rf_att_pred, 'prob_con5', 'tx_binary')
rf_att_pred_rocset_5cat[rf_att_pred_rocset_5cat$inv_distance==max(rf_att_pred_rocset_5cat$inv_distance),]
rf_att_pred_rocset_7cat <- thresh_iter(.0, 1, .01, rf_att_pred, 'prob_con7', 'tx_binary')
rf_att_pred_rocset_7cat[rf_att_pred_rocset_7cat$inv_distance==max(rf_att_pred_rocset_7cat$inv_distance),]
#rf_att_pred_rocset_9cat <- thresh_iter(.0, 1, .01, rf_att_pred, 'prob_con9', 'tx_binary')
#rf_att_pred_rocset_9cat[rf_att_pred_rocset_9cat$inv_distance==max(rf_att_pred_rocset_9cat$inv_distance),]

#Save Image
save.image('FalsePositives.Rdata')

#Random Forest Models - Incremental Scan - Email Text
#Subsample for tuning
train_subsamp <- rand<.2

t0 <- Sys.time()
rft_emtxt_2cat <- tuneRF(y = as.factor(data[train_subsamp & incremental, 'tx_binary']), x = cbind(emailtxt_rhs_pca[train_subsamp & incremental,]), trace = TRUE, improve=0.0005, mtryStart = min(round(sqrt(ncol(emailtxt_rhs_pca[train_subsamp & incremental,]))), max_mtry), nodesize = 1000)
#rft_emtxt_4cat <- tuneRF(y = as.factor(data[train_subsamp & incremental, 'tx_4cat']), x = cbind(emailtxt_rhs_pca[train_subsamp & incremental,]), trace = TRUE, improve=0.0005, mtryStart = min(round(sqrt(ncol(emailtxt_rhs_pca[train_subsamp & incremental,]))), max_mtry), nodesize = 1000, do.trace = TRUE)
#rft_emtxt_5cat <- tuneRF(y = as.factor(data[train_subsamp & incremental, 'tx_5cat']), x = cbind(emailtxt_rhs_pca[train_subsamp & incremental,]), trace = TRUE, improve=0.0005, mtryStart = min(round(sqrt(ncol(emailtxt_rhs_pca[train_subsamp & incremental,]))), max_mtry), nodesize = 1000, do.trace = TRUE)
rft_emtxt_7cat <- tuneRF(y = as.factor(data[train_subsamp & incremental, 'tx_7cat']), x = cbind(emailtxt_rhs_pca[train_subsamp & incremental,]), trace = TRUE, improve=0.0005, mtryStart = min(round(sqrt(ncol(emailtxt_rhs_pca[train_subsamp & incremental,]))), max_mtry), nodesize = 1000, do.trace = TRUE)
#rft_emtxt_9cat <- tuneRF(y = as.factor(data[train & incremental, 'tx_9cat']), x = cbind(emailtxt_rhs_pca[train & incremental,]), trace = TRUE, improve=0.0005, mtryStart = min(round(sqrt(ncol(emailtxt_rhs_pca[train & incremental,]))), max_mtry), nodesize = 1000)
print(difftime(Sys.time(), t0, units = 'sec'))

t0 <- Sys.time()
#rf_emtxt_2cat <- randomForest(y = as.factor(data[train & incremental, 'tx_binary']),x = emailtxt_rhs_pca[train & incremental,], do.trace = TRUE, ntree = 500, mtry = rft_emtxt_2cat[rft_emtxt_2cat[,2] == min(rft_emtxt_2cat[,2]),1], nodesize = 1000)
rf_emtxt_2cat <- randomForest(y = as.factor(data[train & incremental, 'tx_binary']),x = emailtxt_rhs_pca[train & incremental,], do.trace = TRUE, ntree = 250, mtry = rft_emtxt_2cat[rft_emtxt_2cat[,2] == min(rft_emtxt_2cat[,2]),1], nodesize = 1000)
#rf_emtxt_4cat <- randomForest(y = as.factor(data[train & incremental, 'tx_4cat']),x = emailtxt_rhs_pca[train & incremental,], do.trace = TRUE, ntree = 500, mtry = rft_emtxt_4cat[rft_emtxt_4cat[,2] == min(rft_emtxt_4cat[,2]),1], nodesize = 1000)
#rf_emtxt_5cat <- randomForest(y = as.factor(data[train & incremental, 'tx_5cat']),x = emailtxt_rhs_pca[train & incremental,], do.trace = TRUE, ntree = 500, mtry = rft_emtxt_5cat[rft_emtxt_5cat[,2] == min(rft_emtxt_5cat[,2]),1], nodesize = 1000)
#rf_emtxt_7cat <- randomForest(y = as.factor(data[train & incremental, 'tx_7cat']),x = emailtxt_rhs_pca[train & incremental,], do.trace = TRUE, ntree = 500, mtry = rft_emtxt_7cat[rft_emtxt_7cat[,2] == min(rft_emtxt_7cat[,2]),1], nodesize = 1000)
rf_emtxt_7cat <- randomForest(y = as.factor(data[train & incremental, 'tx_7cat']),x = emailtxt_rhs_pca[train & incremental,], do.trace = TRUE, ntree = 300, mtry = rft_emtxt_7cat[rft_emtxt_7cat[,2] == min(rft_emtxt_7cat[,2]),1], nodesize = 1000)
#rf_emtxt_9cat <- randomForest(y = as.factor(data[train & incremental, 'tx_9cat']),x = emailtxt_rhs_pca[train & incremental,], do.trace = TRUE, ntree = 500, mtry = rft_emtxt_9cat[rft_emtxt_9cat[,2] == min(rft_emtxt_9cat[,2]),1], nodesize = 1000)
print(difftime(Sys.time(), t0, units = 'sec'))

rf_emtxt_pred <- cbind.data.frame(data[train & incremental, 'tx_binary'], 
#              data[train & incremental, 'tx_4cat'], 
#              data[train & incremental, 'tx_5cat'],
              data[train & incremental, 'tx_7cat'],               
              rf_emtxt_2cat$votes[,ncol(rf_emtxt_2cat$votes)],
#              rowSums(rf_emtxt_4cat$votes[,c(2:ncol(rf_emtxt_4cat$votes))]),
#              rowSums(rf_emtxt_5cat$votes[,c(2:ncol(rf_emtxt_5cat$votes))]),
              rowSums(rf_emtxt_7cat$votes[,c(4:ncol(rf_emtxt_7cat$votes))])              
              )

#              data[train & incremental, 'tx_9cat'],               
#              rowSums(rf_emtxt_9cat$votes[,c(2:ncol(rf_emtxt_9cat$votes))])

colnames(rf_emtxt_pred) <- c('tx_binary', 'tx_7cat', 
                            'prob_con2', 'prob_con7')
# 'tx_9cat', , 'prob_con9' 'tx_4cat', 'tx_5cat', 'prob_con4', 'prob_con5', 

rf_emtxt_pred_rocset_2cat <- thresh_iter(.0, 1, .01, rf_emtxt_pred, 'prob_con2', 'tx_binary')
rf_emtxt_pred_rocset_2cat[rf_emtxt_pred_rocset_2cat$inv_distance==max(rf_emtxt_pred_rocset_2cat$inv_distance),]
#rf_emtxt_pred_rocset_4cat <- thresh_iter(.0, 1, .01, rf_emtxt_pred, 'prob_con4', 'tx_binary')
#rf_emtxt_pred_rocset_4cat[rf_emtxt_pred_rocset_4cat$inv_distance==max(rf_emtxt_pred_rocset_4cat$inv_distance),]
#rf_emtxt_pred_rocset_5cat <- thresh_iter(.0, 1, .01, rf_emtxt_pred, 'prob_con5', 'tx_binary')
#rf_emtxt_pred_rocset_5cat[rf_emtxt_pred_rocset_5cat$inv_distance==max(rf_emtxt_pred_rocset_5cat$inv_distance),]
rf_emtxt_pred_rocset_7cat <- thresh_iter(.0, 1, .01, rf_emtxt_pred, 'prob_con7', 'tx_binary')
rf_emtxt_pred_rocset_7cat[rf_emtxt_pred_rocset_7cat$inv_distance==max(rf_emtxt_pred_rocset_7cat$inv_distance),]
#rf_emtxt_pred_rocset_9cat <- thresh_iter(.0, 1, .01, rf_emtxt_pred, 'prob_con9', 'tx_binary')
#rf_emtxt_pred_rocset_9cat[rf_emtxt_pred_rocset_9cat$inv_distance==max(rf_emtxt_pred_rocset_9cat$inv_distance),]

#Save Image
save.image('FalsePositives.Rdata')

#Random Forest Models - Incremental Scan - Document Text
t0 <- Sys.time()
rft_doctxt_2cat <- tuneRF(y = as.factor(data[train_subsamp & incremental, 'tx_binary']), x = cbind(doctxt_rhs_pca[train_subsamp & incremental,]), trace = TRUE, improve=0.0005, mtryStart = min(round(sqrt(ncol(doctxt_rhs_pca[train_subsamp & incremental,]))), max_mtry), nodesize = 1000, do.trace = TRUE)
#rft_doctxt_4cat <- tuneRF(y = as.factor(data[train & incremental, 'tx_4cat']), x = cbind(doctxt_rhs_pca[train & incremental,]), trace = TRUE, improve=0.0005, mtryStart = min(round(sqrt(ncol(doctxt_rhs_pca[train & incremental,]))), max_mtry), nodesize = 1000)
#rft_doctxt_5cat <- tuneRF(y = as.factor(data[train & incremental, 'tx_5cat']), x = cbind(doctxt_rhs_pca[train & incremental,]), trace = TRUE, improve=0.0005, mtryStart = min(round(sqrt(ncol(doctxt_rhs_pca[train & incremental,]))), max_mtry), nodesize = 1000)
rft_doctxt_7cat <- tuneRF(y = as.factor(data[train_subsamp & incremental, 'tx_7cat']), x = cbind(doctxt_rhs_pca[train_subsamp & incremental,]), trace = TRUE, improve=0.0005, mtryStart = min(round(sqrt(ncol(doctxt_rhs_pca[train_subsamp & incremental,]))), max_mtry), nodesize = 1000, do.trace = TRUE)
#rft_doctxt_9cat <- tuneRF(y = as.factor(data[train & incremental, 'tx_9cat']), x = cbind(doctxt_rhs_pca[train & incremental,]), trace = TRUE, improve=0.0005, mtryStart = min(round(sqrt(ncol(doctxt_rhs_pca[train & incremental,]))), max_mtry), nodesize = 1000)
print(difftime(Sys.time(), t0, units = 'sec'))

t0 <- Sys.time()
rf_doctxt_2cat <- randomForest(y = as.factor(data[train & incremental, 'tx_binary']),x = doctxt_rhs_pca[train & incremental,], do.trace = TRUE, ntree = 150, mtry = rft_doctxt_2cat[rft_doctxt_2cat[,2] == min(rft_doctxt_2cat[,2]),1], nodesize = 1000)
#rf_doctxt_4cat <- randomForest(y = as.factor(data[train & incremental, 'tx_4cat']),x = doctxt_rhs_pca[train & incremental,], do.trace = TRUE, ntree = 500, mtry = rft_doctxt_4cat[rft_doctxt_4cat[,2] == min(rft_doctxt_4cat[,2]),1], nodesize = 1000)
#rf_doctxt_5cat <- randomForest(y = as.factor(data[train & incremental, 'tx_5cat']),x = doctxt_rhs_pca[train & incremental,], do.trace = TRUE, ntree = 500, mtry = rft_doctxt_5cat[rft_doctxt_5cat[,2] == min(rft_doctxt_5cat[,2]),1], nodesize = 1000)
rf_doctxt_7cat <- randomForest(y = as.factor(data[train & incremental, 'tx_7cat']),x = doctxt_rhs_pca[train & incremental,], do.trace = TRUE, ntree = 250, mtry = rft_doctxt_7cat[rft_doctxt_7cat[,2] == min(rft_doctxt_7cat[,2]),1], nodesize = 1000)
#rf_doctxt_9cat <- randomForest(y = as.factor(data[train & incremental, 'tx_9cat']),x = doctxt_rhs_pca[train & incremental,], do.trace = TRUE, ntree = 500, mtry = rft_doctxt_9cat[rft_doctxt_9cat[,2] == min(rft_doctxt_9cat[,2]),1], nodesize = 1000)
print(difftime(Sys.time(), t0, units = 'sec'))

rf_doctxt_pred <- cbind.data.frame(data[train & incremental, 'tx_binary'], 
#              data[train & incremental, 'tx_4cat'], 
#              data[train & incremental, 'tx_5cat'],
              data[train & incremental, 'tx_7cat'],               
              rf_doctxt_2cat$votes[,ncol(rf_doctxt_2cat$votes)],
#              rowSums(rf_doctxt_4cat$votes[,c(2:ncol(rf_doctxt_4cat$votes))]),
#              rowSums(rf_doctxt_5cat$votes[,c(2:ncol(rf_doctxt_5cat$votes))]),
              rowSums(rf_doctxt_7cat$votes[,c(4:ncol(rf_doctxt_7cat$votes))])               
              )

#              data[train & incremental, 'tx_9cat'],               
#              rowSums(rf_doctxt_9cat$votes[,c(2:ncol(rf_doctxt_9cat$votes))])

colnames(rf_doctxt_pred) <- c('tx_binary', 'tx_7cat', 
                            'prob_con2', 'prob_con7')
# 'tx_9cat', , 'prob_con9', 'tx_4cat', 'tx_5cat', 'prob_con4', 'prob_con5', 

rf_doctxt_pred_rocset_2cat <- thresh_iter(.0, 1, .01, rf_doctxt_pred, 'prob_con2', 'tx_binary')
rf_doctxt_pred_rocset_2cat[rf_doctxt_pred_rocset_2cat$inv_distance==max(rf_doctxt_pred_rocset_2cat$inv_distance),]
#rf_doctxt_pred_rocset_4cat <- thresh_iter(.0, 1, .01, rf_doctxt_pred, 'prob_con4', 'tx_binary')
#rf_doctxt_pred_rocset_4cat[rf_doctxt_pred_rocset_4cat$inv_distance==max(rf_doctxt_pred_rocset_4cat$inv_distance),]
#rf_doctxt_pred_rocset_5cat <- thresh_iter(.0, 1, .01, rf_doctxt_pred, 'prob_con5', 'tx_binary')
#rf_doctxt_pred_rocset_5cat[rf_doctxt_pred_rocset_5cat$inv_distance==max(rf_doctxt_pred_rocset_5cat$inv_distance),]
rf_doctxt_pred_rocset_7cat <- thresh_iter(.0, 1, .01, rf_doctxt_pred, 'prob_con7', 'tx_binary')
rf_doctxt_pred_rocset_7cat[rf_doctxt_pred_rocset_7cat$inv_distance==max(rf_doctxt_pred_rocset_7cat$inv_distance),]
#rf_emtxt_pred_rocset_9cat <- thresh_iter(.0, 1, .01, rf_emtxt_pred, 'prob_con9', 'tx_binary')
#rf_emtxt_pred_rocset_9cat[rf_emtxt_pred_rocset_9cat$inv_distance==max(rf_emtxt_pred_rocset_9cat$inv_distance),]

#Try Base + Subject, Base + Subject + Attname
rft_bsacombo_7cat <- tuneRF(y = as.factor(data[train & incremental,'tx_7cat']), x = cbind(base_rhs[train & incremental,], subject_rhs[train & incremental,], attname_rhs[train & incremental,]), trace = TRUE, improve=0.0005, mtryStart = 30, nodesize = 1000, do.trace = TRUE)
rf_bsacombo_7cat <- randomForest(y = as.factor(data[train & incremental,'tx_7cat']), x = cbind(base_rhs[train & incremental,], subject_rhs[train & incremental,], attname_rhs[train & incremental,], emailtxt_rhs_pca[train & incremental,]), do.trace = TRUE, ntree = 150, mtry = 30, nodesize = 1000)

rf_bsacombo_pred <- cbind.data.frame(data[train & incremental, 'tx_binary'], 
#              data[train & incremental, 'tx_4cat'], 
#              data[train & incremental, 'tx_5cat'],
              data[train & incremental, 'tx_7cat'],               
#              rowSums(rf_doctxt_4cat$votes[,c(2:ncol(rf_doctxt_4cat$votes))]),
#              rowSums(rf_doctxt_5cat$votes[,c(2:ncol(rf_doctxt_5cat$votes))]),
              rowSums(rf_bsacombo_7cat$votes[,c(4:ncol(rf_bsacombo_7cat$votes))])               
              )

colnames(rf_bsacombo_pred) <- c('tx_binary', 'tx_7cat', 'prob_con7')

rf_bsacombo_pred_rocset_7cat <- thresh_iter(.0, 1, .01, rf_bsacombo_pred, 'prob_con7', 'tx_binary')
rf_bsacombo_pred_rocset_7cat[rf_bsacombo_pred_rocset_7cat$inv_distance==max(rf_bsacombo_pred_rocset_7cat$inv_distance),]

rf_bsacombo2_7cat <- randomForest(y = as.factor(data[train & incremental,'tx_7cat']), x = cbind(base_rhs[train & incremental,], subject_rhs[train & incremental,], attname_rhs[train & incremental,], emailtxt_rhs_pca[train & incremental,], doctxt_rhs_pca[train & incremental,]), do.trace = TRUE, ntree = 500, mtry = 30, nodesize = 1000)
rf_bsacombo2_pred <- cbind.data.frame(data[train & incremental, 'tx_binary'], 
#              data[train & incremental, 'tx_4cat'], 
#              data[train & incremental, 'tx_5cat'],
              data[train & incremental, 'tx_7cat'],               
#              rowSums(rf_doctxt_4cat$votes[,c(2:ncol(rf_doctxt_4cat$votes))]),
#              rowSums(rf_doctxt_5cat$votes[,c(2:ncol(rf_doctxt_5cat$votes))]),
              rowSums(rf_bsacombo2_7cat$votes[,c(4:ncol(rf_bsacombo2_7cat$votes))])               
              )

colnames(rf_bsacombo2_pred) <- c('tx_binary', 'tx_7cat', 'prob_con7')

rf_bsacombo2_pred_rocset_7cat <- thresh_iter(.0, 1, .01, rf_bsacombo2_pred, 'prob_con7', 'tx_binary')
rf_bsacombo2_pred_rocset_7cat[rf_bsacombo2_pred_rocset_7cat$inv_distance==max(rf_bsacombo2_pred_rocset_7cat$inv_distance),]






tot_auc <- rf_base_pred_rocset_2cat[rf_base_pred_rocset_2cat$inv_distance==max(rf_base_pred_rocset_2cat$inv_distance),ncol(rf_base_pred_rocset_2cat)] + 
            rf_sub_pred_rocset_2cat[rf_sub_pred_rocset_2cat$inv_distance==max(rf_sub_pred_rocset_2cat$inv_distance),ncol(rf_sub_pred_rocset_2cat)] + 
#            rf_att_pred_rocset_2cat[rf_att_pred_rocset_2cat$inv_distance==max(rf_att_pred_rocset_2cat$inv_distance),ncol(rf_att_pred_rocset_2cat)] + 
            rf_emtxt_pred_rocset_2cat[rf_emtxt_pred_rocset_2cat$inv_distance==max(rf_emtxt_pred_rocset_2cat$inv_distance),ncol(rf_emtxt_pred_rocset_2cat)] + 
            rf_doctxt_pred_rocset_2cat[rf_doctxt_pred_rocset_2cat$inv_distance==max(rf_doctxt_pred_rocset_2cat$inv_distance),ncol(rf_doctxt_pred_rocset_2cat)] + 
            rf_base_pred_rocset_7cat[rf_base_pred_rocset_7cat$inv_distance==max(rf_base_pred_rocset_7cat$inv_distance),ncol(rf_base_pred_rocset_7cat)] + 
            rf_sub_pred_rocset_7cat[rf_sub_pred_rocset_7cat$inv_distance==max(rf_sub_pred_rocset_7cat$inv_distance),ncol(rf_sub_pred_rocset_7cat)] + 
            rf_att_pred_rocset_7cat[rf_att_pred_rocset_7cat$inv_distance==max(rf_att_pred_rocset_7cat$inv_distance),ncol(rf_att_pred_rocset_7cat)] + 
            rf_emtxt_pred_rocset_7cat[rf_emtxt_pred_rocset_7cat$inv_distance==max(rf_emtxt_pred_rocset_7cat$inv_distance),ncol(rf_emtxt_pred_rocset_7cat)] + 
            rf_doctxt_pred_rocset_7cat[rf_doctxt_pred_rocset_7cat$inv_distance==max(rf_doctxt_pred_rocset_7cat$inv_distance),ncol(rf_doctxt_pred_rocset_7cat)] + 
            rf_bsacombo_pred_rocset_7cat[rf_bsacombo_pred_rocset_7cat$inv_distance==max(rf_bsacombo_pred_rocset_7cat$inv_distance),ncol(rf_bsacombo_pred_rocset_7cat)] + 
            rf_bsacombo2_pred_rocset_7cat[rf_bsacombo2_pred_rocset_7cat$inv_distance==max(rf_bsacombo2_pred_rocset_7cat$inv_distance),ncol(rf_bsacombo2_pred_rocset_7cat)]

base_2cat_auc_wt <- (rf_base_pred_rocset_2cat[1, ncol(rf_base_pred_rocset_2cat)]/tot_auc)
sub_2cat_auc_wt <- (rf_sub_pred_rocset_2cat[1, ncol(rf_sub_pred_rocset_2cat)]/tot_auc)
emtxt_2cat_auc_wt <- (rf_emtxt_pred_rocset_2cat[1, ncol(rf_emtxt_pred_rocset_2cat)]/tot_auc)
doctxt_2cat_auc_wt <- (rf_doctxt_pred_rocset_2cat[1, ncol(rf_doctxt_pred_rocset_2cat)]/tot_auc)
base_7cat_auc_wt <- (rf_base_pred_rocset_7cat[1, ncol(rf_base_pred_rocset_7cat)]/tot_auc)
sub_7cat_auc_wt <- (rf_sub_pred_rocset_7cat[1, ncol(rf_sub_pred_rocset_7cat)]/tot_auc)
emtxt_7cat_auc_wt <- (rf_emtxt_pred_rocset_7cat[1, ncol(rf_emtxt_pred_rocset_7cat)]/tot_auc)
doctxt_7cat_auc_wt <- (rf_doctxt_pred_rocset_7cat[1, ncol(rf_doctxt_pred_rocset_7cat)]/tot_auc)
att_7cat_auc_wt <- (rf_att_pred_rocset_7cat[1, ncol(rf_att_pred_rocset_7cat)]/tot_auc)
bsacombo_7cat_auc_wt <- (rf_bsacombo_pred_rocset_7cat[1, ncol(rf_bsacombo_pred_rocset_7cat)]/tot_auc)
bsacombo2_7cat_auc_wt <- (rf_bsacombo2_pred_rocset_7cat[1, ncol(rf_bsacombo2_pred_rocset_7cat)]/tot_auc)

combined_prob_confirmed <- (rf_base_pred[,5] * base_2cat_auc_wt) + 
                            (rf_sub_pred[,5] * sub_2cat_auc_wt) +
                            (rf_emtxt_pred[,3] * emtxt_2cat_auc_wt) +
                            (rf_doctxt_pred[,3] * doctxt_2cat_auc_wt) + 
                            (rf_base_pred[,5] * base_7cat_auc_wt) + 
                            (rf_sub_pred[,5] * sub_7cat_auc_wt) +
                            (rf_att_pred[,5] * att_7cat_auc_wt) +
                            (rf_emtxt_pred[,4] * emtxt_7cat_auc_wt) +
                            (rf_doctxt_pred[,4] * doctxt_7cat_auc_wt) +                              
                            (rf_bsacombo_pred[,3] * bsacombo_7cat_auc_wt) +                              
                            (rf_bsacombo2_pred[,3] * bsacombo2_7cat_auc_wt) 

rf_combo <- cbind.data.frame(data[train & incremental,'tx_binary'], 
              combined_prob_confirmed)
colnames(rf_combo) <- c('tx_binary','prob_con')
rf_combo_rocset <- thresh_iter(.0, 1, .01, rf_combo, 'prob_con', 'tx_binary')
rf_combo_rocset[rf_combo_rocset$inv_distance==max(rf_combo_rocset$inv_distance),]




combined_full <- cbind(rf_base_pred[,4] , (rf_base_pred[,4] * base_auc_wt), 
                            rf_sub_pred[,4], (rf_sub_pred[,4] * sub_auc_wt) ,
                            rf_attname_pred[,4], (rf_attname_pred[,4] * attname_auc_wt) ,
                            rf_emailtxt_pred[,4], (rf_emailtxt_pred[,4] * emailtxt_auc_wt) ,
                            rf_doctxt_pred[,4], (rf_doctxt_pred[,4] * doctxt_auc_wt) )

combined_prob_confirmed <- (rf_base_pred[,4] * base_auc_wt) + 
                            (rf_sub_pred[,4] * sub_auc_wt) +
                            (rf_attname_pred[,4] * attname_auc_wt) +
                            (rf_emailtxt_pred[,4] * emailtxt_auc_wt) +
                            (rf_doctxt_pred[,4] * doctxt_auc_wt) 

rf_combo <- cbind.data.frame(data[train & incremental,'tx_binary'], 
              data[train & incremental,'tx_5cat'], 
              combined_prob_confirmed)
colnames(rf_combo) <- c('tx_binary','tx_5cat','prob_con')

two_class_stats(rf_combo, 'prob_con', 'tx_binary', .5)
rf_combo_rocset <- thresh_iter(.0, 1, .01, rf_combo_testpred, 'prob_con', 'tx_binary')
rf_combo_rocset[rf_combo_rocset$inv_distance==max(rf_combo_rocset$inv_distance),]









t0 <- Sys.time()
rf_tune_emailtxt <- tuneRF(y = as.factor(data[train & incremental,'tx_5cat']), x = cbind(emailtxt_rhs_pca[train & incremental,]), trace = TRUE, improve=0.005, mtryStart = 8)
print(difftime(Sys.time(), t0, units = 'sec'))

t0 <- Sys.time()
rf_emailtxt <- randomForest(y = as.factor(data[train & incremental,'tx_5cat']),x = cbind(emailtxt_rhs_pca[train & incremental,]), do.trace = TRUE, ntree = 500, mtry = 8)
print(difftime(Sys.time(), t0, units = 'sec'))

rf_emailtxt_pred <- cbind.data.frame(data[train & incremental,'tx_binary'], 
              data[train & incremental,'tx_5cat'], 
              rf_emailtxt$votes[,1], 
              rowSums(rf_emailtxt$votes[,c(2:ncol(rf_emailtxt$votes))]))
colnames(rf_emailtxt_pred) <- c('tx_binary','tx_5cat','prob_rej','prob_con')

two_class_stats(rf_emailtxt_pred, 'prob_con', 'tx_binary', .5)
rf_emailtxt_pred_rocset <- thresh_iter(.0, 1, .01, rf_emailtxt_pred, 'prob_con', 'tx_binary')
rf_emailtxt_pred_rocset[rf_emailtxt_pred_rocset$inv_distance==max(rf_emailtxt_pred_rocset$inv_distance),]

t0 <- Sys.time()
rf_tune_doctxt <- tuneRF(y = as.factor(data[train & incremental,'tx_5cat']), x = cbind(doctxt_rhs_pca[train & incremental,]), trace = TRUE, improve=0.005, mtryStart = 8)
print(difftime(Sys.time(), t0, units = 'sec'))

t0 <- Sys.time()
rf_doctxt <- randomForest(y = as.factor(data[train & incremental,'tx_5cat']),x = cbind(doctxt_rhs_pca[train & incremental,]), do.trace = TRUE, ntree = 1001, mtry = 64)
print(difftime(Sys.time(), t0, units = 'sec'))

rf_doctxt_pred <- cbind.data.frame(data[train & incremental,'tx_binary'], 
              data[train & incremental,'tx_5cat'], 
              rf_doctxt$votes[,1], 
              rowSums(rf_doctxt$votes[,c(2:ncol(rf_doctxt$votes))]))
colnames(rf_doctxt_pred) <- c('tx_binary','tx_5cat','prob_rej','prob_con')

two_class_stats(rf_doctxt_pred, 'prob_con', 'tx_binary', .5)
rf_doctxt_pred_rocset <- thresh_iter(.0, 1, .01, rf_doctxt_pred, 'prob_con', 'tx_binary')
rf_doctxt_pred_rocset[rf_doctxt_pred_rocset$inv_distance==max(rf_doctxt_pred_rocset$inv_distance),]

tot_auc <- rf_base_pred_rocset[rf_base_pred_rocset$inv_distance==max(rf_base_pred_rocset$inv_distance),ncol(rf_base_pred_rocset)] + 
            rf_sub_pred_rocset[rf_sub_pred_rocset$inv_distance==max(rf_sub_pred_rocset$inv_distance),ncol(rf_sub_pred_rocset)] + 
            rf_attname_pred_rocset[rf_attname_pred_rocset$inv_distance==max(rf_attname_pred_rocset$inv_distance),ncol(rf_attname_pred_rocset)] + 
            rf_emailtxt_pred_rocset[rf_emailtxt_pred_rocset$inv_distance==max(rf_emailtxt_pred_rocset$inv_distance),ncol(rf_emailtxt_pred_rocset)] + 
            rf_doctxt_pred_rocset[rf_doctxt_pred_rocset$inv_distance==max(rf_doctxt_pred_rocset$inv_distance),ncol(rf_doctxt_pred_rocset)]

base_auc_wt <- (rf_base_pred_rocset[1, ncol(rf_base_pred_rocset)]/tot_auc)
sub_auc_wt <- (rf_sub_pred_rocset[1, ncol(rf_sub_pred_rocset)]/tot_auc)
attname_auc_wt <- (rf_attname_pred_rocset[1, ncol(rf_attname_pred_rocset)]/tot_auc)
emailtxt_auc_wt <- (rf_emailtxt_pred_rocset[1, ncol(rf_emailtxt_pred_rocset)]/tot_auc)
doctxt_auc_wt <- (rf_doctxt_pred_rocset[1, ncol(rf_doctxt_pred_rocset)]/tot_auc)

combined_full <- cbind(rf_base_pred[,4] , (rf_base_pred[,4] * base_auc_wt), 
                            rf_sub_pred[,4], (rf_sub_pred[,4] * sub_auc_wt) ,
                            rf_attname_pred[,4], (rf_attname_pred[,4] * attname_auc_wt) ,
                            rf_emailtxt_pred[,4], (rf_emailtxt_pred[,4] * emailtxt_auc_wt) ,
                            rf_doctxt_pred[,4], (rf_doctxt_pred[,4] * doctxt_auc_wt) )

combined_prob_confirmed <- (rf_base_pred[,4] * base_auc_wt) + 
                            (rf_sub_pred[,4] * sub_auc_wt) +
                            (rf_attname_pred[,4] * attname_auc_wt) +
                            (rf_emailtxt_pred[,4] * emailtxt_auc_wt) +
                            (rf_doctxt_pred[,4] * doctxt_auc_wt) 

rf_combo <- cbind.data.frame(data[train & incremental,'tx_binary'], 
              data[train & incremental,'tx_5cat'], 
              combined_prob_confirmed)
colnames(rf_combo) <- c('tx_binary','tx_5cat','prob_con')

two_class_stats(rf_combo, 'prob_con', 'tx_binary', .5)
rf_combo_rocset <- thresh_iter(.0, 1, .01, rf_combo_testpred, 'prob_con', 'tx_binary')
rf_combo_rocset[rf_combo_rocset$inv_distance==max(rf_combo_rocset$inv_distance),]






rf_base_testpred <- cbind.data.frame(data[test & incremental,'tx_binary'], 
              data[test & incremental,'tx_5cat'],
              predict(rf_base, newdata = base_rhs[test & incremental,], type = 'prob')[,1],
              rowSums(predict(rf_base, newdata = base_rhs[test & incremental,], type = 'prob')[,2:4]))

colnames(rf_base_testpred) <- c('tx_binary','tx_4cat','prob_rej','prob_con')

two_class_stats(rf_base_testpred, 'prob_con', 'tx_binary', .5)
rf_base_testpred_rocset <- thresh_iter(.0, 1, .01, rf_base_testpred, 'prob_con', 'tx_binary')
rf_base_testpred_rocset[rf_base_testpred_rocset$inv_distance==max(rf_base_testpred_rocset$inv_distance),]

rf_sub_testpred <- cbind.data.frame(data[test & incremental,'tx_binary'], 
              data[test & incremental,'tx_4cat'],
              predict(rf_sub, newdata = subject_rhs_pca[test & incremental,], type = 'prob')[,1],
              rowSums(predict(rf_sub, newdata = subject_rhs_pca[test & incremental,], type = 'prob')[,2:4]))

colnames(rf_sub_testpred) <- c('tx_binary','tx_4cat','prob_rej','prob_con')

two_class_stats(rf_sub_testpred, 'prob_con', 'tx_binary', .5)
rf_sub_testpred_rocset <- thresh_iter(.0, 1, .01, rf_sub_testpred, 'prob_con', 'tx_binary')
rf_sub_testpred_rocset[rf_sub_testpred_rocset$inv_distance==max(rf_sub_testpred_rocset$inv_distance),]

rf_attname_testpred <- cbind.data.frame(data[test & incremental,'tx_binary'], 
              data[test & incremental,'tx_4cat'],
              predict(rf_attname, newdata = attname_rhs_pca[test & incremental,], type = 'prob')[,1],
              rowSums(predict(rf_attname, newdata = attname_rhs_pca[test & incremental,], type = 'prob')[,2:4]))

colnames(rf_attname_testpred) <- c('tx_binary','tx_4cat','prob_rej','prob_con')

two_class_stats(rf_attname_testpred, 'prob_con', 'tx_binary', .5)
rf_attname_testpred_rocset <- thresh_iter(.0, 1, .01, rf_attname_testpred, 'prob_con', 'tx_binary')
rf_attname_testpred_rocset[rf_attname_testpred_rocset$inv_distance==max(rf_attname_testpred_rocset$inv_distance),]

rf_emailtxt_testpred <- cbind.data.frame(data[test & incremental,'tx_binary'], 
              data[test & incremental,'tx_4cat'],
              predict(rf_emailtxt, newdata = emailtxt_rhs_pca[test & incremental,], type = 'prob')[,1],
              rowSums(predict(rf_emailtxt, newdata = emailtxt_rhs_pca[test & incremental,], type = 'prob')[,2:4]))

colnames(rf_emailtxt_testpred) <- c('tx_binary','tx_4cat','prob_rej','prob_con')

two_class_stats(rf_emailtxt_testpred, 'prob_con', 'tx_binary', .5)
rf_emailtxt_testpred_rocset <- thresh_iter(.0, 1, .01, rf_emailtxt_testpred, 'prob_con', 'tx_binary')
rf_emailtxt_testpred_rocset[rf_emailtxt_testpred_rocset$inv_distance==max(rf_emailtxt_testpred_rocset$inv_distance),]

rf_doctxt_testpred <- cbind.data.frame(data[test & incremental,'tx_binary'], 
              data[test & incremental,'tx_4cat'],
              predict(rf_doctxt, newdata = doctxt_rhs_pca[test & incremental,], type = 'prob')[,1],
              rowSums(predict(rf_doctxt, newdata = doctxt_rhs_pca[test & incremental,], type = 'prob')[,2:4]))

colnames(rf_doctxt_testpred) <- c('tx_binary','tx_4cat','prob_rej','prob_con')

two_class_stats(rf_doctxt_testpred, 'prob_con', 'tx_binary', .5)
rf_doctxt_testpred_rocset <- thresh_iter(.0, 1, .01, rf_doctxt_testpred, 'prob_con', 'tx_binary')
rf_doctxt_testpred_rocset[rf_doctxt_testpred_rocset$inv_distance==max(rf_doctxt_testpred_rocset$inv_distance),]

tot_auc_test <- rf_base_testpred_rocset[rf_base_testpred_rocset$inv_distance==max(rf_base_testpred_rocset$inv_distance),ncol(rf_base_testpred_rocset)] + 
            rf_sub_testpred_rocset[rf_sub_testpred_rocset$inv_distance==max(rf_sub_testpred_rocset$inv_distance),ncol(rf_sub_testpred_rocset)] + 
            rf_attname_testpred_rocset[rf_attname_testpred_rocset$inv_distance==max(rf_attname_testpred_rocset$inv_distance),ncol(rf_attname_testpred_rocset)] + 
            rf_emailtxt_testpred_rocset[rf_emailtxt_testpred_rocset$inv_distance==max(rf_emailtxt_testpred_rocset$inv_distance),ncol(rf_emailtxt_testpred_rocset)] + 
            rf_doctxt_testpred_rocset[rf_doctxt_testpred_rocset$inv_distance==max(rf_doctxt_testpred_rocset$inv_distance),ncol(rf_doctxt_testpred_rocset)]

base_auc_test_wt <- (rf_base_testpred_rocset[1, ncol(rf_base_testpred_rocset)]/tot_auc_test)
sub_auc_test_wt <- (rf_sub_testpred_rocset[1, ncol(rf_sub_testpred_rocset)]/tot_auc_test)
attname_auc_test_wt <- (rf_attname_testpred_rocset[1, ncol(rf_attname_testpred_rocset)]/tot_auc_test)
emailtxt_auc_test_wt <- (rf_emailtxt_testpred_rocset[1, ncol(rf_emailtxt_testpred_rocset)]/tot_auc_test)
doctxt_auc_test_wt <- (rf_doctxt_testpred_rocset[1, ncol(rf_doctxt_testpred_rocset)]/tot_auc_test)

combined_prob_testconfirmed <- (rf_base_testpred[,4] * base_auc_test_wt) + 
                            (rf_sub_testpred[,4] * sub_auc_test_wt) +
                            (rf_attname_testpred[,4] * attname_auc_test_wt) +
                            (rf_emailtxt_testpred[,4] * emailtxt_auc_test_wt) +
                            (rf_doctxt_testpred[,4] * doctxt_auc_test_wt) 

rf_combo_testpred <- cbind.data.frame(data[test & incremental,'tx_binary'], 
              data[test & incremental,'tx_4cat'], 
              combined_prob_testconfirmed)
colnames(rf_combo_testpred) <- c('tx_binary','tx_4cat','prob_con')

two_class_stats(rf_combo_testpred, 'prob_con', 'tx_binary', .5)
rf_combo_pred_rocset <- thresh_iter(.0, 1, .01, rf_combo_testpred, 'prob_con', 'tx_binary')
rf_combo_pred_rocset[rf_combo_pred_rocset$inv_distance==max(rf_combo_pred_rocset$inv_distance),]







tuneRF(y = as.factor(data[train & incremental,'tx_4cat']), x = cbind(emailtxt_rhs_reduced[train & incremental,]), trace = TRUE, improve=0.005, mtryStart = sqrt(ncol(emailtxt_rhs[train & incremental,])))

tuneRF(y = as.factor(data[train & incremental,'tx_4cat']), x = cbind(emailtxt_rhs_reduced[train & incremental,]), trace = TRUE, improve=0.005, mtryStart = 8)

t0 <- Sys.time()
rf4 <- randomForest(y = as.factor(data[train & incremental,'tx_4cat']),x = cbind(emailtxt_rhs_reduced[train & incremental,]), do.trace = TRUE, ntree = 751, mtry = 8)
print(difftime(Sys.time(), t0, units = 'sec'))

tuneRF(y = as.factor(data[train & incremental,'tx_4cat']), x = cbind(doctxt_rhs_reduced[train & incremental,]), trace = TRUE, improve=0.005, mtryStart = sqrt(ncol(doctxt_rhs[train & incremental,])))
t0 <- Sys.time()
rf5 <- randomForest(y = as.factor(data[train & incremental,'tx_4cat']),x = cbind(doctxt_rhs_reduced[train & incremental,]), do.trace = TRUE, ntree = 751, mtry = 8)
print(difftime(Sys.time(), t0, units = 'sec'))





rf1_x <- cbind.data.frame(data[train & incremental,'tx_binary'], 
              data[train & incremental,'tx_4cat'], 
              rf1$votes[,1], 
              rowSums(rf1$votes[,c(2:4)]))
colnames(rf1_x) <- c('tx_binary','tx_4cat','prob_rej','prob_con')

rf2_x <- cbind.data.frame(data[train & incremental,'tx_binary'], 
              data[train & incremental,'tx_4cat'], 
              rf2$votes)
colnames(rf2_x) <- c('tx_binary','tx_4cat','prob_rej','prob_con')

two_class_stats(rf1_x, 'prob_con', 'tx_binary', .5)
two_class_stats(rf2_x, 'prob_con', 'tx_binary', .5)
rf1_rocset <- thresh_iter(.0, 1, .01, rf1_x, 'prob_con', 'tx_binary')
rf2_rocset <- thresh_iter(.0, 1, .01, rf2_x, 'prob_con', 'tx_binary')




tuneRF(y = as.factor(data[train & incremental,'tx_4cat']), x = cbind(base_numeric[train & incremental,], base_factor[train & incremental,], base_binary[train & incremental,], subject_rhs[train & incremental,], attname_rhs[train & incremental,], emailtxt_rhs[train & incremental,], doctxt_rhs[train & incremental,]), trace = TRUE)


tuneRF(y = as.factor(data[train & incremental,'tx_binary']), x = cbind(base_numeric[train & incremental,], base_factor[train & incremental,], base_binary[train & incremental,], subject_rhs[train & incremental,], attname_rhs[train & incremental,], emailtxt_rhs[train & incremental,], doctxt_rhs[train & incremental,]), trace = TRUE)

t0 <- Sys.time()
rf1 <- randomForest(y = as.factor(data[train & incremental,'tx_4cat']), x = cbind(base_numeric[train & incremental,], base_factor[train & incremental,], base_binary[train & incremental,], subject_rhs[train & incremental,], attname_rhs[train & incremental,], emailtxt_rhs[train & incremental,], doctxt_rhs[train & incremental,]), do.trace = TRUE, ntree = 1001, mtry = 10)
print(difftime(Sys.time(), t0, units = 'sec'))

t0 <- Sys.time()
rf2 <- randomForest(y = as.factor(data[train & incremental,'tx_binary']), x = cbind(base_numeric[train & incremental,], base_factor[train & incremental,], base_binary[train & incremental,], subject_rhs[train & incremental,], attname_rhs[train & incremental,], emailtxt_rhs[train & incremental,], doctxt_rhs[train & incremental,]), do.trace = TRUE, ntree = 1001, mtry = 10,)
print(difftime(Sys.time(), t0, units = 'sec'))

rf1_x <- cbind.data.frame(data[train & incremental,'tx_binary'], 
              data[train & incremental,'tx_4cat'], 
              rf1$votes[,1], 
              rowSums(rf1$votes[,c(2:4)]))
colnames(rf1_x) <- c('tx_binary','tx_4cat','prob_rej','prob_con')

rf2_x <- cbind.data.frame(data[train & incremental,'tx_binary'], 
              data[train & incremental,'tx_4cat'], 
              rf2$votes)
colnames(rf2_x) <- c('tx_binary','tx_4cat','prob_rej','prob_con')

two_class_stats(rf1_x, 'prob_con', 'tx_binary', .5)
two_class_stats(rf2_x, 'prob_con', 'tx_binary', .5)
rf1_rocset <- thresh_iter(.0, 1, .01, rf1_x, 'prob_con', 'tx_binary')
rf2_rocset <- thresh_iter(.0, 1, .01, rf2_x, 'prob_con', 'tx_binary')

#Modeling 
t0 <- Sys.time()
simple_logit_initscan <- glm(data[train & initial,'tx_binary'] ~ data[train & initial, 'email_domain_recat'] + data[train & initial, 'daysfromtxcreate'] + subject_rhs[train & initial,] + attname_rhs[train & initial,] , 
  family = binomial, na.action = na.omit)
print(difftime(Sys.time(), t0, units = 'sec'))




pred <- as.data.frame(cbind(data[train & incremental,'tx_binary'],predict.glm(simple_logit_b, type = 'response')))
colnames(pred) <- c('tx_binary', 'pred')
two_class_stats(pred, 'pred', 'tx_binary', .5)

t0 <- Sys.time()
simple_logit_b <- glm(data[train & incremental,'tx_binary'] ~ data[train & incremental, 'email_domain_recat'] + data[train & incremental, 'daysfromtxcreate'] + subject_rhs[train & incremental,] + attname_rhs[train & incremental,] + emailtxt_rhs[train & incremental,], 
  family = binomial, na.action = na.omit)
print(difftime(Sys.time(), t0, units = 'sec'))

pred <- as.data.frame(cbind(data[train & incremental,'tx_binary'],predict.glm(simple_logit_b, type = 'response')))
colnames(pred) <- c('tx_binary', 'pred')
two_class_stats(pred, 'pred', 'tx_binary', .5)








t0 <- Sys.time()
mfp_a <- mfp(data[train & incremental,'tx_binary'] ~ 
  fp(base_numeric[train & incremental,'daysfromtxcreate_win'], df = 4, select = .05) + 
  fp(base_numeric[train & incremental,'daysfromtx_con_or_rej_win'], df = 4, select = .05) +
  fp(base_numeric[train & incremental,'body_characters_win'], df = 4, select = .05) + 
  fp(base_numeric[train & incremental,'doc_characters_win'], df = 4, select = .05) ,
  family = binomial, na.action = na.omit)
print(difftime(Sys.time(), t0, units = 'sec'))


simple_logit_minc_a <- multinom(data[train & incremental,'tx_4cat'] ~ base_numeric[train & incremental,] + subject_rhs[train & incremental,])
t0 <- Sys.time()






n_features <- ncol(cbind(base_numeric[train & incremental,], base_factor[train & incremental,], subject_rhs[train & incremental,], attname_rhs[train & incremental,]))
rf1c <- randomForest(y = as.factor(data[train & incremental,'tx_4cat']), x = cbind(base_numeric[train & incremental,], base_factor[train & incremental,], subject_rhs[train & incremental,], attname_rhs[train & incremental,]), do.trace = TRUE, ntree = 100, mtry = sqrt(n_features),)


n_features <- ncol(cbind(base_numeric[train & incremental,], base_factor[train & incremental,], subject_rhs[train & incremental,], attname_rhs[train & incremental,], emailtxt_rhs[train & incremental,]))
rf1c <- randomForest(y = as.factor(data[train & incremental,'tx_4cat']), x = cbind(base_numeric[train & incremental,], base_factor[train & incremental,], subject_rhs[train & incremental,], attname_rhs[train & incremental,], emailtxt_rhs[train & incremental,]), do.trace = TRUE, ntree = 100, mtry = sqrt(n_features),)


rf2 <- randomForest(y = as.factor(data[train & incremental,'tx_binary']), x = cbind(base_numeric[train & incremental,], subject_rhs[train & incremental,]), do.trace = TRUE, ntree = 100, mtry = 30,)

rf1x <- cbind.data.frame(data[train & incremental,'tx_binary'], 
              data[train & incremental,'tx_4cat'], 
              rf1$votes[,1], 
              rowSums(rf1$votes[,c(2:4)]))
colnames(rf1x) <- c('tx_binary','tx_4cat','prob_rej','prob_con')

rf2x <- cbind.data.frame(data[train & incremental,'tx_binary'], 
              data[train & incremental,'tx_4cat'], 
              rf2$votes)
colnames(rf2x) <- c('tx_binary','tx_4cat','prob_rej','prob_con')

two_class_stats(rf1x, 'prob_con', 'tx_binary', .5)
two_class_stats(rf2x, 'prob_con', 'tx_binary', .5)

print(difftime(Sys.time(), t0, units = 'sec'))
t <- rhs
rhs$pred_rf1 <- predict(rf1, type="prob", data = rhs)


pred <- as.data.frame(cbind(data[train & incremental,'tx_4cat'],
  predict.glm(simple_logit_minc_a, type = 'response')))
colnames(pred) <- c('tx_4cat', 'pred')

t0 <- Sys.time()
simple_logit_inc_a <- glm(data[train & incremental,'tx_binary'] ~ base_numeric[train & incremental,] + base_factor[train & incremental,] , 
  family = binomial, na.action = na.omit)
print(difftime(Sys.time(), t0, units = 'sec'))

pred <- as.data.frame(cbind(data[train & incremental,'tx_binary'],
  predict.glm(simple_logit_inc_a, type = 'response')))
colnames(pred) <- c('tx_binary', 'pred')
two_class_stats(pred, 'pred', 'tx_binary', .5)




t0 <- Sys.time()
simple_logit_inc_b <- glm(data[train & incremental,'tx_binary'] ~ base_numeric[train & incremental,] + base_factor[train & incremental,] + subject_rhs[train & incremental,], 
  family = binomial, na.action = na.omit)
print(difftime(Sys.time(), t0, units = 'sec'))

pred <- as.data.frame(cbind(data[train & incremental,'tx_binary'],predict.glm(simple_logit_inc_b, type = 'response')))
colnames(pred) <- c('tx_binary', 'pred')
two_class_stats(pred, 'pred', 'tx_binary', .5)



t0 <- Sys.time()
simple_logit_b <- glm(data[train & incremental,'tx_binary'] ~ data[train & incremental,'email_domain_recat'] + data[train & incremental,'daysfromtxcreate_win'] + data[train & incremental,'daysfromtx_con_or_rej_win'] + data[train & incremental,'body_characters_win'] + data[train & incremental,'doc_characters_win'], 
  family = binomial, na.action = na.omit)
print(difftime(Sys.time(), t0, units = 'sec'))

pred <- as.data.frame(cbind(data[train & incremental,'tx_binary'],predict.glm(simple_logit_b, type = 'response')))
colnames(pred) <- c('tx_binary', 'pred')
two_class_stats(pred, 'pred', 'tx_binary', .5)

t0 <- Sys.time()
simple_logit_b <- glm(data[train & incremental,'tx_binary'] ~ base_factor[train & incremental,] + base_numeric[train & incremental,1:2], 
  family = binomial, na.action = na.omit)
print(difftime(Sys.time(), t0, units = 'sec'))

pred <- as.data.frame(cbind(data[train & incremental,'tx_binary'],predict.glm(simple_logit_b, type = 'response')))
colnames(pred) <- c('tx_binary', 'pred')
two_class_stats(pred, 'pred', 'tx_binary', .5)







#









t0 <- Sys.time()
subject_dtm <- corpus_dtm(subject_corpus, n = 1, p_thresh = subject_corpus_p)
print(difftime(Sys.time(), t0, units = 'sec'))

t0 <- Sys.time()
subject_dtm2 <- par_corpus_dtm(subject_corpus, n = 1, p_thresh = subject_corpus_p, parallel = 4)
print(difftime(Sys.time(), t0, units = 'sec'))


subject_dtm <- as.matrix(DocumentTermMatrix(subject_corpus, 
                          control = list(weighting = weightTfIdf,
                                        bounds = list(global = c(subject_corpus_p, Inf)))))
colnames(subject_dtm)<- paste(colnames(subject_dtm),'sub', sep = '_')

subject_corpus <- pre_process_corpus(data = data_incremental, text = 'subject', stopword_lang = 'english')
subject_corpus_p <- round(.01* length(subject_corpus))
subject_dtm <- as.matrix(DocumentTermMatrix(subject_corpus, 
                          control = list(weighting = weightTfIdf,
                                        bounds = list(global = c(subject_corpus_p, Inf)))))
colnames(subject_dtm)<- paste(colnames(subject_dtm),'sub', sep = '_')

attachment_name_corpus <- pre_process_corpus(data = data_incremental, text = 'attachment_names', stopword_lang = 'english')
attachment_name_corpus_p <- round(.01* length(attachment_name_corpus))
attachment_name_dtm <- as.matrix(DocumentTermMatrix(attachment_name_corpus, 
                          control = list(weighting = weightTfIdf,
                                        bounds = list(global = c(attachment_name_corpus_p, Inf)))))

colnames(attachment_name_dtm)<- paste(colnames(attachment_name_dtm),'attn', sep = '_')

email_corpus <- pre_process_corpus(data = data_incremental, text = 'body_text', stopword_lang = 'english')
email_corpus_p <- round(.01* length(email_corpus))
email_corpus_dtm <- as.matrix(DocumentTermMatrix(email_corpus, 
                          control = list(weighting = weightTfIdf,
                                        bounds = list(global = c(email_corpus_p, Inf)))))

colnames(email_corpus_dtm)<- paste(colnames(email_corpus_dtm),'em', sep = '_')

document_corpus <- pre_process_corpus(data = data_incremental, text = 'document_content', stopword_lang = 'english')
document_corpus_p <- round(.01* length(document_corpus))
document_corpus_dtm <- as.matrix(DocumentTermMatrix(document_corpus, 
                          control = list(weighting = weightTfIdf,
                                        bounds = list(global = c(document_corpus_p, Inf)))))

colnames(document_corpus_dtm)<- paste(colnames(document_corpus_dtm),'doc', sep = '_')

rhs <- cbind(subject_dtm, attachment_name_dtm, email_corpus_dtm, data_incremental$email_domain_recat, data_incremental$daysfromtxcreate, data_incremental$daysfromtxcreate^2)
colnames(rhs) <- c(colnames(subject_dtm), colnames(attachment_name_dtm), colnames(email_corpus_dtm), 'email_domain_recat', 'daysfromtxcreate', 'daysfromtxcreate_sq')

t0 <- Sys.time()
simple_logit <- glm(tx_binary ~ email_domain_recat + daysfromtxcreate
  , data = data_insc[train_insc,], family = binomial, na.action = na.omit)
print(difftime(Sys.time(), t0, units = 'sec'))
data$pred[train] <- predict.glm(simple_logit)
two_class_stats(data[train,], 'pred', 'tx_binary', .5)









num_folds <- 4
glmnet_classifier2 <- cv.glmnet(x = subject_dtm[train_incremental,], y = data_incremental[train_incremental,'tx_binary'], 
                              family = 'binomial', 
                              # L1 penalty
                              alpha = 1,
                              # interested in the area under ROC curve
                              type.measure = "auc",
                              # 5-fold cross-validation
                              nfolds = num_folds,
                              # high value is less accurate, but has faster training
                              thresh = 1e-3,
                              # again lower number of iterations for faster training
                              maxit = 1e3)








t0 <- Sys.time()
simple_logit <- glm(tx_binary ~ email_domain_recat + daysfromtxcreate
  , data = data_insc[train_insc,], family = binomial, na.action = na.omit)
print(difftime(Sys.time(), t0, units = 'sec'))
data$pred[train] <- predict.glm(simple_logit)
two_class_stats(data[train,], 'pred', 'tx_binary', .5)

t0 <- Sys.time()
simple_logit <- glm(tx_binary ~ email_domain_recat + I(daysfromtxcreate_win) + I(daysfromtxcreate_win^2) + I(daysfromtxcreate_win^3) + I(daysfromtxcreate_win^4) + I(daysfromtxcreate_win^5) + I(daysfromtxcreate_win^6) + I(daysfromtxcreate_win^7) + I(daysfromtxcreate_win^8)
  , data = data[train,], family = binomial, na.action = na.omit)
print(difftime(Sys.time(), t0, units = 'sec'))
data$pred[train] <- predict.glm(simple_logit)
two_class_stats(data[train,], 'pred', 'tx_binary', .5)

t0 <- Sys.time()
simple_logit <- glm(tx_binary ~ email_domain_recat + I(daysfromtxcreate_win) + I(daysfromtxcreate_win^2) + I(daysfromtxcreate_win^3) + I(daysfromtxcreate_win^4) + I(daysfromtxcreate_win^5) + I(daysfromtxcreate_win^6) + I(daysfromtxcreate_win^7) + I(daysfromtxcreate_win^8)
  , data = data[train,], family = binomial, na.action = na.omit)
print(difftime(Sys.time(), t0, units = 'sec'))
data$pred[train] <- predict.glm(simple_logit)
two_class_stats(data[train,], 'pred', 'tx_binary', .5)
check <- thresh_iter(.0, 1, .01, data, 'pred', 'tx_binary')

t0 <- Sys.time()
simple_logit <- glm(tx_binary ~ email_domain_recat + daysfromtxcreate + has_subject, data = data[train,], na.action = na.omit)
print(difftime(Sys.time(), t0, units = 'sec'))
data$pred[train] <- predict.glm(simple_logit)
two_class_stats(data[train,], 'pred', 'tx_binary', .5)

simple_logit <- glm(tx_binary ~ email_domain_recat + daysfromtxcreate_win , data = data, na.action = na.omit)
print(difftime(Sys.time(), t0, units = 'sec'))
data$pred <- predict.glm(simple_logit)
two_class_stats(data, 'pred', 'tx_binary', .5)


num_folds <- 4
t0 <- Sys.time()
glmnet_classifier2 <- cv.glmnet(x = rhs[train_incremental,], y = data_incremental[train_incremental,'tx_binary'], 
                              family = 'binomial', 
                              # L1 penalty
                              alpha = 1,
                              # interested in the area under ROC curve
                              type.measure = "auc",
                              # 5-fold cross-validation
                              nfolds = num_folds,
                              # high value is less accurate, but has faster training
                              thresh = 1e-3,
                              # again lower number of iterations for faster training
                              maxit = 1e3)
print(difftime(Sys.time(), t0, units = 'sec'))
data_incremental$pred_glm2 <- predict(glmnet_classifier2, type="response", newx = rhs)
two_class_stats(data_incremental[test_incremental,], 'pred_glm2', 'tx_binary', .5)

t0 <- Sys.time()
glmnet_classifier3 <- cv.glmnet(x = rhs[train_incremental & !is.na(data_incremental[,'tx_binary_not_tx']),], y = data_incremental[train_incremental & !is.na(data_incremental[,'tx_binary_not_tx']),'tx_binary_not_tx'], 
                              family = 'binomial', 
                              # L1 penalty
                              alpha = 1,
                              # interested in the area under ROC curve
                              type.measure = "auc",
                              # 5-fold cross-validation
                              nfolds = num_folds,
                              # high value is less accurate, but has faster training
                              thresh = 1e-3,
                              # again lower number of iterations for faster training
                              maxit = 1e3)
print(difftime(Sys.time(), t0, units = 'sec'))
data_incremental$pred_glm3 <- predict(glmnet_classifier3, type="response", newx = rhs)
two_class_stats(data_incremental[test_incremental,], 'pred_glm3', 'tx_binary_not_tx', .5)


t0 <- Sys.time()
rf1 <- randomForest(y = as.factor(data_incremental[train_incremental,'tx_binary']), x = rhs[train_incremental,], do.trace = TRUE, ntree = 100, mtry = 30, )
print(difftime(Sys.time(), t0, units = 'sec'))
t <- rhs
rhs$pred_rf1 <- predict(rf1, type="prob", data = rhs)



email_corpus <- pre_process_corpus(data = data, text = 'body_text', stopword_lang = 'english')
attachment_name_corpus <- pre_process_corpus(data = data, text = 'attachment_names', stopword_lang = 'english')
document_corpus <- pre_process_corpus(data = data, text = 'document_content', stopword_lang = 'english')

#Create TDMs
email_corpus_p <- round(.02 * length(email_corpus))
email_corpus_tdm <- as.matrix(TermDocumentMatrix(email_corpus, 
                          control = list(weighting = weightTf,
                                        bounds = list(global = c(email_corpus_p, Inf)))))

subject_corpus_p <- round(.02 * length(subject_corpus))
subject_corpus_tdm <- as.matrix(TermDocumentMatrix(subject_corpus, 
                          control = list(weighting = weightTf,
                                        bounds = list(global = c(subject_corpus_p, Inf)))))

attachment_name_corpus_p <- round(.02 * length(attachment_name_corpus))
attachment_name_corpus_tdm <- as.matrix(TermDocumentMatrix(attachment_name_corpus, 
                          control = list(weighting = weightTf,
                                        bounds = list(global = c(attachment_name_corpus_p, Inf)))))

document_corpus_p <- round(.02 * length(document_corpus))
document_corpus_tdm <- as.matrix(TermDocumentMatrix(document_corpus, 
                          control = list(weighting = weightTf,
                                        bounds = list(global = c(document_corpus_p, Inf)))))
#Add Message ID to column names
colnames(email_corpus_tdm) <- data$message_id
colnames(subject_corpus_tdm) <- data$message_id
colnames(attachment_name_corpus_tdm) <- data$message_id
colnames(document_corpus_tdm) <- data$message_id

#Add stubs to word names
rownames(email_corpus_tdm) <- paste(rownames(email_corpus_tdm),'em', sep = '_')
rownames(subject_corpus_tdm) <- paste(rownames(subject_corpus_tdm),'su', sep = '_')
rownames(attachment_name_corpus_tdm) <- paste(rownames(attachment_name_corpus_tdm),'at', sep = '_')
rownames(document_corpus_tdm) <- paste(rownames(document_corpus_tdm),'dc', sep = '_')

#Create DTMs
email_corpus_dtm <- t(email_corpus_tdm)
subject_corpus_dtm <- t(subject_corpus_tdm)
attachment_name_corpus_dtm <- t(attachment_name_corpus_tdm)
document_corpus_dtm <- t(document_corpus_tdm)
rm(email_corpus_tdm)
rm(subject_corpus_tdm)
rm(attachment_name_corpus_tdm)
rm(document_corpus_tdm)

num_folds <- 4
t0 <- Sys.time()
glmnet_classifier <- cv.glmnet(x = email_corpus_dtm[train,], y = data[train,'tx_binary'], 
                              family = 'binomial', 
                              # L1 penalty
                              alpha = 1,
                              # interested in the area under ROC curve
                              type.measure = "auc",
                              # 5-fold cross-validation
                              nfolds = num_folds,
                              # high value is less accurate, but has faster training
                              thresh = 1e-3,
                              # again lower number of iterations for faster training
                              maxit = 1e3)
print(difftime(Sys.time(), t0, units = 'sec'))
jpeg('rplot.jpg')
plot(glmnet_classifier)
dev.off()
print(paste("max AUC =", round(max(glmnet_classifier$cvm), 4)))

num_folds <- 4
t0 <- Sys.time()
glmnet_classifier2 <- cv.glmnet(x = subject_corpus_dtm[train,], y = data[train,'tx_binary'], 
                              family = 'binomial', 
                              # L1 penalty
                              alpha = 1,
                              # interested in the area under ROC curve
                              type.measure = "auc",
                              # 5-fold cross-validation
                              nfolds = num_folds,
                              # high value is less accurate, but has faster training
                              thresh = 1e-3,
                              # again lower number of iterations for faster training
                              maxit = 1e3)
print(difftime(Sys.time(), t0, units = 'sec'))
jpeg('rplot2.jpg')
plot(glmnet_classifier2)
dev.off()
print(paste("max AUC =", round(max(glmnet_classifier2$cvm), 4)))

num_folds <- 4
t0 <- Sys.time()
glmnet_classifier3 <- cv.glmnet(x = attachment_name_corpus_dtm[train,], y = data[train,'tx_binary'], 
                              family = 'binomial', 
                              # L1 penalty
                              alpha = 1,
                              # interested in the area under ROC curve
                              type.measure = "auc",
                              # 5-fold cross-validation
                              nfolds = num_folds,
                              # high value is less accurate, but has faster training
                              thresh = 1e-3,
                              # again lower number of iterations for faster training
                              maxit = 1e3)
print(difftime(Sys.time(), t0, units = 'sec'))
jpeg('rplot3.jpg')
plot(glmnet_classifier3)
dev.off()
print(paste("max AUC =", round(max(glmnet_classifier3$cvm), 4)))

num_folds <- 4
t0 <- Sys.time()
glmnet_classifier4 <- cv.glmnet(x = document_corpus_dtm[train,], y = data[train,'tx_binary'], 
                              family = 'binomial', 
                              # L1 penalty
                              alpha = 1,
                              # interested in the area under ROC curve
                              type.measure = "auc",
                              # 5-fold cross-validation
                              nfolds = num_folds,
                              # high value is less accurate, but has faster training
                              thresh = 1e-3,
                              # again lower number of iterations for faster training
                              maxit = 1e3)
print(difftime(Sys.time(), t0, units = 'sec'))
jpeg('rplot4.jpg')
plot(glmnet_classifier4)
dev.off()
print(paste("max AUC =", round(max(glmnet_classifier4$cvm), 4)))

num_folds <- 4
t0 <- Sys.time()
glmnet_classifier5 <- cv.glmnet(x = data[train,'daysfromtxcreate'], y = data[train,'tx_binary'], 
                              family = 'binomial', 
                              # L1 penalty
                              alpha = 1,
                              # interested in the area under ROC curve
                              type.measure = "class",
                              # 5-fold cross-validation
                              nfolds = num_folds,
                              # high value is less accurate, but has faster training
                              thresh = 1e-3,
                              # again lower number of iterations for faster training
                              maxit = 1e3)
print(difftime(Sys.time(), t0, units = 'sec'))
jpeg('rplot.jpg')
plot(glmnet_classifier5)
dev.off()
print(paste("max AUC =", round(max(glmnet_classifier5$cvm), 4)))








data <- merge(data, email_corpus_dtm_count, by = 'message_id', all = TRUE)



email_corpus_dtm_ind <- apply(email_corpus_dtm_count,2,function(x){as.factor(ifelse(x>=1,1,x))})
email_corpus_dtm_count$message_id <- rownames(email_corpus_dtm_count)
email_corpus_dtm_ind <- 


sort( sapply(ls(),function(x){object.size(get(x))}))






[41] "rf1"                          "rf1a"
[43] "rf1b"                         "rf1c"
[45] "rf1x"                         "rf2"
[47] "rf2x"


sess <- tf$InteractiveSession()

x_in <- cbind(base_numeric[train & incremental,3])
y_in <- as.data.frame(data[train & incremental,'tx_binary'])

xdim <- ncol(x_in)
ydim <- 1

x <- tf$placeholder(tf$float32, shape(NULL, xdim))
y_ <- tf$placeholder(tf$float32, shape(NULL, ydim))

W <- tf$Variable(tf$zeros(shape(xdim, ydim)))
b <- tf$Variable(tf$zeros(shape(ydim)))
sess$run(tf$global_variables_initializer())
y <- tf$nn$softmax(tf$matmul(x,W) + b)
cross_entropy <- tf$reduce_mean(-tf$reduce_sum(y_ * tf$log(y), reduction_indices=1L))

optimizer <- tf$train$GradientDescentOptimizer(0.5)
train_step <- optimizer$minimize(cross_entropy)

for (i in 1:1000) {
  sess$run(train_step,
           feed_dict = dict(x = x_in, y_ = y_in))
}


#Store Dictionary
dbWriteTable(connection, c('datasci_staging','dtm3_01'), value = as.data.frame(t_counts), overwrite = FALSE, append = TRUE, row.names = TRUE)



glmnet_classifier <- cv.glmnet(x = as.matrix(base_rhs[train & incremental & (data$tx_5cat == 0 | data$tx_5cat == 1 | data$tx_5cat == 3), -5]), y = data[train & incremental & (data$tx_5cat == 0 | data$tx_5cat == 1 | data$tx_5cat == 3),'tx_binary'], 
                              family = 'multinomial', 
                              # L1 penalty
                              alpha = 1,
                              # interested in the area under ROC curve
                              type.measure = "class",
                              # 5-fold cross-validation
                              nfolds = 5,
                              # high value is less accurate, but has faster training
                              thresh = 1e-3,
                              # again lower number of iterations for faster training
                              maxit = 1e3)









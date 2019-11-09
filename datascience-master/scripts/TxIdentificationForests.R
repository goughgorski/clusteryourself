rm(list=ls())

#Load libraries
source('~/workspace/datascience/functions/load_libraries.R')
load_libraries('~/workspace/datascience/functions/')
options(stringsAsFactors = FALSE)
set.seed(123456789)
connection <- db_connect()

#Query to get data for Proposed Transactions
data <- dbGetQuery(connection, "SELECT *
                                FROM datasci_modeling.vw_tx_modeling dspvtxm")

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
nrow(data)
data <- data[!(data$event_type == 'PropertyTransactionRejectedEvent' & !is.na(data$timeline_update)),]
nrow(data)

#Outcomes
data$tx_binary <- ifelse(data$event_type == 'PropertyTransactionConfirmedEvent', 1, 0)
data$tx_3cat <- ifelse(data$event_type == 'PropertyTransactionConfirmedEvent' & !is.na(data$timeline_updates),1,  
					ifelse(data$event_type == 'PropertyTransactionConfirmedEvent' & is.na(data$timeline_updates), 2, 0))

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
data$email_domain_recat <- ifelse(!email_domain_top50 | is.na(data$email_domain), 'small_n_domain', data$email_domain)
data$email_domain_recat <- as.factor(data$email_domain_recat)

email_domain_levels <- cbind(domain = levels(data$email_domain_recat), factor_level = seq(from = 1, to = length(levels(data$email_domain_recat))))

#Write email domain table for scoring
dbWriteTable(connection, c('datasci_modeling','email_domains'), value = as.data.frame(email_domain_levels), overwrite = TRUE, append = FALSE, row.names = FALSE)

#Logical checks for message components
data$has_subject <- ifelse(!is.na(data$subject),1,0)
data$has_body <- ifelse(!is.na(data$body_text),1,0)
data$has_attachment <- ifelse(!is.na(data$attachment_names),1,0)

##Word Counts
data$body_characters <- nchar(data$body_text)
data$body_characters_win <- mad_winsor(data$body_characters, multiple = 5, na.rm = TRUE)
data$doc_characters <- nchar(data$document_content)
data$doc_characters_win <- mad_winsor(data$doc_characters, multiple = 5, na.rm = TRUE)

#Recode Missing Counts to Zero
data$body_characters_winr <- ifelse(is.na(data$body_characters_win),0,data$body_characters_win)
data$doc_characters_winr <- ifelse(is.na(data$doc_characters_win),0,data$doc_characters_win)

###Store MAD output
#Collect MAD Winsor stats
daysfromtxcreate_win_stats <- mad_winsor(data$daysfromtxcreate, multiple = 10, na.rm = TRUE, stats = TRUE)$stats
body_characters_win_stats <- mad_winsor(data$body_characters, multiple = 5, na.rm = TRUE, stats = TRUE)$stats
doc_characters_win_win_stats <- mad_winsor(data$doc_characters, multiple = 5, na.rm = TRUE, stats = TRUE)$stats

#create MAD stats records
mad_stats <- rbind(
            cbind(transform_type = 'MAD Winsor', transform_data = toJSON(cbind(varname = 'daysfromtxcreate', daysfromtxcreate_win_stats))),
            cbind(transform_type = 'MAD Winsor', transform_data = toJSON(cbind(varname = 'body_characters', body_characters_win_stats))),
            cbind(transform_type = 'MAD Winsor', transform_data = toJSON(cbind(varname = 'doc_characters', doc_characters_win_win_stats)))
            )
dbWriteTable(connection, c('datasci_modeling','model_transforms'), value = as.data.frame(mad_stats), overwrite = FALSE, append = TRUE, row.names = FALSE)

#Segment Data
incremental <- data$tx_scan_type == 'incremental_scan'
initial <- data$tx_scan_type == 'initial_scan'

#Set Indicators
rand <- runif(nrow(data))
train <- rand<.7
test <- rand >=.7 & rand <.9
validation <- rand >=.9
timeline_up <- data$tx_3cat < 2

#Combine Base Data Features
base_numeric <- as.matrix(data[, c('daysfromtxcreate_win', 'body_characters_winr', 'doc_characters_winr')])
base_factor <-  as.data.frame(data[,'email_domain_recat'])
base_binary <- as.matrix(data[, c('has_subject', 'has_body', 'has_attachment')])
base_rhs <- cbind(base_numeric, base_factor, base_binary)
colnames(base_rhs) <- c('daysfromtxcreate_win', 'body_characters_winr', 'doc_characters_winr', 'email_domain_recat', 'has_subject', 'has_body', 'has_attachment')

#Set PCA thresholds
mar_var_inc_thresh <- .01
cum_var_thresh <- .90
nzv_dim_thresh <- 1000
nzv_fr_thresh <- 995/5
nzv_pctunq_thresh <- 1

#### Create Corpora ####

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
print(noquote(paste(ncol(subject_rhs), 'unique terms will be used.')))

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
dbWriteTable(connection, c('datasci_modeling','doc_term_matrices'), value = as.data.frame(subject_rhs_ls), overwrite = TRUE, append = FALSE, row.names = FALSE)

#Near Zero Variance
nz_subject_rhs <- nearZeroVar(subject_rhs, freqCut = nzv_fr_thresh, uniqueCut = nzv_pctunq_thresh, saveMetrics = TRUE)

subject_dict <- cbind(rep('subject',length(ln_idf)),c(colnames(subject_dtm),colnames(subject_dtm2),colnames(subject_dtm3)),ln_idf)
colnames(subject_dict) <- c('component','term', 'ln_idf')
nz_subject_rhs <- cbind(term = rownames(nz_subject_rhs), nz_subject_rhs)
subject_dict <- merge(subject_dict, nz_subject_rhs, by = 'term', all = TRUE)

nzv_used <- FALSE
if (ncol(subject_rhs) > nzv_dim_thresh) {
  subject_rhs <- subject_rhs[,!(nz_subject_rhs$nzv)]
  nzv_used  <- TRUE
}
subject_dict$nzv_used <- nzv_used 

#Store Dictionary w/NZV
dbWriteTable(connection, c('datasci_modeling','term_dictionaries'), value = as.data.frame(subject_dict), overwrite = TRUE, append = FALSE, row.names = FALSE)

#PCA
# subject_pca <- prcomp(subject_rhs[train,], scale = TRUE)
# cum_var <- cumsum(subject_pca$sdev/sum(subject_pca$sdev))
# delta <- diff(cum_var,lag = 1)
# cut <- c(TRUE,delta < mar_var_inc_thresh) & cum_var > cum_var_thresh
# ncomp <- length(cum_var) - sum(cut)
# subject_rhs_reduced <- subject_pca$x[,1:ncomp]
# subject_rhs_pca <- predict(subject_pca,subject_rhs)
# subject_rhs_pca <- subject_rhs_pca[,1:ncomp]

#save.image('FalsePositives.Rdata')

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
print(noquote(paste(ncol(attname_rhs), 'unique terms will be used.')))

##Compute TfIdf
attname_rhs <- attname_rhs * ln_idf
rownames(attname_rhs) <- data$message_id

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
#dbWriteTable(connection, c('datasci_modeling','doc_term_matrices'), value = as.data.frame(attname_rhs_ls), overwrite = FALSE, append = TRUE, row.names = FALSE)

#Near Zero Variance
nz_attname_rhs <- nearZeroVar(attname_rhs, freqCut = nzv_fr_thresh, uniqueCut = nzv_pctunq_thresh, saveMetrics = TRUE)

attname_dict <- cbind(rep('attachment_names',length(ln_idf)),c(colnames(attname_dtm),colnames(attname_dtm2),colnames(attname_dtm3)),ln_idf)
colnames(attname_dict) <- c('component','term', 'ln_idf')
nz_attname_rhs <- cbind(term = rownames(nz_attname_rhs), nz_attname_rhs)
attname_dict <- merge(attname_dict, nz_attname_rhs, by = 'term', all = TRUE)

nzv_used <- FALSE
if (ncol(attname_rhs) > nzv_dim_thresh) {
  attname_rhs <- attname_rhs[,!(nz_attname_rhs$nzv)]
  nzv_used  <- TRUE
}
attname_dict$nzv_used <- nzv_used 

#Store Dictionary w/NZV
dbWriteTable(connection, c('datasci_modeling','term_dictionaries'), value = as.data.frame(attname_dict), overwrite = FALSE, append = TRUE, row.names = FALSE)

#PCA
# attname_pca <- prcomp(attname_rhs[train,], scale = TRUE)
# cum_var <- cumsum(attname_pca$sdev/sum(attname_pca$sdev))
# delta <- diff(cum_var,lag = 1)
# cut <- c(TRUE,delta < mar_var_inc_thresh) & cum_var > cum_var_thresh
# ncomp <- length(cum_var) - sum(cut)
# attname_rhs_reduced <- attname_pca$x[,1:ncomp]
# attname_rhs_pca <- predict(attname_pca,attname_rhs)
# attname_rhs_pca <- attname_rhs_pca[,1:ncomp]

#save.image('FalsePositives.Rdata')

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
print(noquote(paste(ncol(emailtxt_rhs), 'unique terms will be used.')))

##Compute TfIdf
emailtxt_rhs <- emailtxt_rhs * ln_idf
rownames(emailtxt_rhs) <- data$message_id

#Normalize DTM for storage
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
dbWriteTable(connection, c('datasci_modeling','doc_term_matrices'), value = as.data.frame(emailtxt_rhs_ls), overwrite = FALSE, append = TRUE, row.names = FALSE)

#Near Zero Variance
nz_emailtxt_rhs <- nearZeroVar(emailtxt_rhs, freqCut = nzv_fr_thresh, uniqueCut = nzv_pctunq_thresh, saveMetrics = TRUE)

emailtxt_dict <- cbind(rep('body_text',length(ln_idf)),c(colnames(emailtxt_dtm),colnames(emailtxt_dtm2),colnames(emailtxt_dtm3)),ln_idf)
colnames(emailtxt_dict) <- c('component','term', 'ln_idf')
nz_emailtxt_rhs <- cbind(term = rownames(nz_emailtxt_rhs), nz_emailtxt_rhs)
emailtxt_dict <- merge(emailtxt_dict, nz_emailtxt_rhs, by = 'term', all = TRUE)

nzv_used <- FALSE
if (ncol(emailtxt_rhs) > nzv_dim_thresh) {
  emailtxt_rhs <- emailtxt_rhs[,!(nz_emailtxt_rhs$nzv)]
  nzv_used <- TRUE
}
emailtxt_dict$nzv_used <- nzv_used 

#Store Dictionary w/NZV
#dbWriteTable(connection, c('datasci_modeling','term_dictionaries'), value = as.data.frame(emailtxt_dict), overwrite = FALSE, append = TRUE, row.names = FALSE)

#PCA
emailtxt_pca <- prcomp(emailtxt_rhs[train,], scale = TRUE)
cum_var <- cumsum(emailtxt_pca$sdev/sum(emailtxt_pca$sdev))
delta <- diff(cum_var,lag = 1)
cut <- c(TRUE,delta < mar_var_inc_thresh) & cum_var > cum_var_thresh
ncomp <- length(cum_var) - sum(cut)
print(cum_var[ncomp])
emailtxt_rhs_reduced <- emailtxt_pca$x[,1:ncomp]
emailtxt_rhs_pca <- predict(emailtxt_pca,emailtxt_rhs)
emailtxt_rhs_pca <- emailtxt_rhs_pca[,1:ncomp]

#Store PCA output
email_pca_stats <- cbind.data.frame(component = rep('body_text', nrow(emailtxt_pca$rotation)), term = rownames(emailtxt_pca$rotation), sdev = emailtxt_pca$sdev, center = emailtxt_pca$center, scale = emailtxt_pca$scale, rotation = emailtxt_pca$rotation)
email_pca_stats <- split(email_pca_stats, 1:nrow(email_pca_stats))
email_pca_stats <- toJSON(email_pca_stats)

save.image('FalsePositivesSubset.Rdata')

#Document Txt Corpora
t0 <- Sys.time()
doctxt_corpus <- pre_process_corpus(data = data, text = 'document_content', stopword_lang = 'english', remove_letters = TRUE)
print(difftime(Sys.time(), t0, units = 'min'))
doctxt_corpus_p <- round(.01* length(doctxt_corpus))
save.image('FalsePositivesSubset.Rdata')

tokenizer <-  function(x) {unlist(lapply(ngrams(words(x), 1), paste, collapse = " "), use.names = FALSE)}
doctxt_dtm <- as.matrix(DocumentTermMatrix(doctxt_corpus, 
                          control = list(tokenize = tokenizer, weighting = weightTf,
                                        bounds = list(global = c(doctxt_corpus_p, Inf)))))
print(noquote(paste(ncol(doctxt_dtm), 'unique terms found.')))
#save.image('FalsePositives.Rdata')

tokenizer <-  function(x) {unlist(lapply(ngrams(words(x), 2), paste, collapse = " "), use.names = FALSE)}
doctxt_dtm2 <- as.matrix(DocumentTermMatrix(doctxt_corpus, 
                          control = list(tokenize = tokenizer, weighting = weightTf,
                                        bounds = list(global = c(doctxt_corpus_p, Inf)))))
print(noquote(paste(ncol(doctxt_dtm2), 'unique terms found.')))

#save.image('FalsePositives.Rdata')

tokenizer <-  function(x) {unlist(lapply(ngrams(words(x), 3), paste, collapse = " "), use.names = FALSE)}
doctxt_dtm3 <- as.matrix(DocumentTermMatrix(doctxt_corpus, 
                          control = list(tokenize = tokenizer, weighting = weightTf,
                                        bounds = list(global = c(doctxt_corpus_p, Inf)))))
print(noquote(paste(ncol(doctxt_dtm3), 'unique terms found.')))
#save.image('FalsePositives.Rdata')

##Compute Idf
doctxt_rhs <- cbind(doctxt_dtm, doctxt_dtm2, doctxt_dtm3)
ln_idf <- log(sum(!is.na(data$document_content))/apply(doctxt_rhs, 2, function(x) {sum(x>0)}))
doctxt_rhs <- doctxt_rhs * ln_idf
rownames(doctxt_rhs) <- data$message_id
save.image('FalsePositivesSubset.RData')

#Store Normalized DTMs for all Componets
start <- 1
end <- ncol(doctxt_rhs)

doctxt_rhs_ls <- list()
for (i in start:end) {
  filter <- doctxt_rhs[,i]>0
  doc_id <- data$message_id[filter]
  email_part <- rep('doctxt', sum(filter))
  term <- rep(colnames(doctxt_rhs)[i], sum(filter))
  values <- doctxt_rhs[filter,i]
  doctxt_rhs_ls[[i]] <- cbind(doc_id, email_part, term, values)
}

doctxt_rhs_ls <- ldply(doctxt_rhs_ls, rbind)

#Store DTMs
dbWriteTable(connection, c('datasci_modeling','doc_term_matrices'), value = as.data.frame(doctxt_rhs_ls), overwrite = FALSE, append = TRUE, row.names = FALSE)

#Near Zero Variance 
doctxt_dict <- cbind(rep('document_content',length(ln_idf)),c(colnames(doctxt_rhs)),ln_idf)
colnames(doctxt_dict) <- c('component','term', 'ln_idf')
nz_doctxt_rhs <- nearZeroVar(doctxt_rhs, saveMetrics = TRUE)
nz_doctxt_rhs <- cbind(term = rownames(nz_doctxt_rhs), nz_doctxt_rhs)
doctxt_dict <- merge(doctxt_dict, nz_doctxt_rhs, by = 'term', all = TRUE)

#Near Zero Variance and PCA
nzv_used <- FALSE
if (ncol(doctxt_rhs) > nzv_dim_thresh) {
  doctxt_rhs <- doctxt_rhs[,!(nz_doctxt_rhs$nzv)]
  nzv_used <- TRUE
}
doctxt_dict$nzv_used <- nzv_used 

#Store Dictionary w/NZV
dbWriteTable(connection, c('datasci_modeling','term_dictionaries'), value = as.data.frame(doctxt_dict), overwrite = FALSE, append = TRUE, row.names = FALSE)

#PCA
doctxt_pca <- prcomp(doctxt_rhs[train,], scale = TRUE)
cum_var <- cumsum(doctxt_pca$sdev/sum(doctxt_pca$sdev))
delta <- diff(cum_var,lag = 1)
cut <- c(TRUE,delta < mar_var_inc_thresh) & cum_var > cum_var_thresh
ncomp <- length(cum_var) - sum(cut)
print(cum_var[ncomp])
doctxt_rhs_reduced <- doctxt_pca$x[,1:ncomp]
doctxt_rhs_pca <- predict(doctxt_pca,doctxt_rhs)
doctxt_rhs_pca <- doctxt_rhs_pca[,1:ncomp]

doctxt_pca_stats <- cbind.data.frame(component = rep('document_content', nrow(doctxt_pca$rotation)), term = rownames(doctxt_pca$rotation), sdev = doctxt_pca$sdev, center = doctxt_pca$center, scale = doctxt_pca$scale, rotation = doctxt_pca$rotation)
doctxt_pca_stats <- split(doctxt_pca_stats, 1:nrow(doctxt_pca_stats))
doctxt_pca_stats <- toJSON(doctxt_pca_stats)

#Write PCA data to DB
pca_stats <- rbind( cbind(transform_type = 'PCA', transform_data = email_pca_stats), 
                    cbind(transform_type = 'PCA', transform_data = doctxt_pca_stats))

dbWriteTable(connection, c('datasci_modeling','model_transforms'), value = as.data.frame(pca_stats), overwrite = FALSE, append = TRUE, row.names = FALSE)

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
rft_base_3cat <- tuneRF(y = as.factor(data[train & incremental & timeline_up, 'tx_3cat']), x = cbind(base_rhs[train & incremental & timeline_up,]), trace = TRUE, improve=0.0005, mtryStart = min(round(sqrt(ncol(base_rhs[train & incremental & timeline_up,]))), max_mtry))
rft_base_4cat <- tuneRF(y = as.factor(data[train & incremental, 'tx_4cat']), x = cbind(base_rhs[train & incremental,]), trace = TRUE, improve=0.0005, mtryStart = min(round(sqrt(ncol(base_rhs[train & incremental,]))), max_mtry))
rft_base_5cat <- tuneRF(y = as.factor(data[train & incremental, 'tx_5cat']), x = cbind(base_rhs[train & incremental,]), trace = TRUE, improve=0.0005, mtryStart = min(round(sqrt(ncol(base_rhs[train & incremental,]))), max_mtry))
rft_base_7cat <- tuneRF(y = as.factor(data[train & incremental, 'tx_7cat']), x = cbind(base_rhs[train & incremental,]), trace = TRUE, improve=0.0005, mtryStart = min(round(sqrt(ncol(base_rhs[train & incremental,]))), max_mtry))
print(difftime(Sys.time(), t0, units = 'sec'))

t0 <- Sys.time()
rf_base_2cat <- randomForest(y = as.factor(data[train & incremental, 'tx_binary']),x = base_rhs[train & incremental,], do.trace = TRUE, ntree = 500, mtry = rft_base_2cat[rft_base_2cat[,2] == min(rft_base_2cat[,2]),1], nodesize = 1000)
rf_base_3cat <- randomForest(y = as.factor(data[train & incremental & timeline_up, 'tx_3cat']),x = base_rhs[train & incremental & timeline_up,], do.trace = TRUE, ntree = 500, mtry = rft_base_3cat[rft_base_3cat[,2] == min(rft_base_3cat[,2]),1], nodesize = 1000)
rf_base_4cat <- randomForest(y = as.factor(data[train & incremental, 'tx_4cat']),x = base_rhs[train & incremental,], do.trace = TRUE, ntree = 500, mtry = rft_base_4cat[rft_base_4cat[,2] == min(rft_base_4cat[,2]),1], nodesize = 1000)
rf_base_5cat <- randomForest(y = as.factor(data[train & incremental, 'tx_5cat']),x = base_rhs[train & incremental,], do.trace = TRUE, ntree = 500, mtry = rft_base_5cat[rft_base_5cat[,2] == min(rft_base_5cat[,2]),1], nodesize = 1000)
rf_base_7cat <- randomForest(y = as.factor(data[train & incremental, 'tx_7cat']),x = base_rhs[train & incremental,], do.trace = TRUE, ntree = 500, mtry = rft_base_7cat[rft_base_7cat[,2] == min(rft_base_7cat[,2]),1], nodesize = 1000)
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

colnames(rf_base_pred) <- c('tx_binary', 'tx_4cat', 'tx_5cat', 'tx_7cat',  
                            'prob_con2', 'prob_con4', 'prob_con5', 'prob_con7')

rf_base_pred_rocset_2cat <- thresh_iter(.0, 1, .01, rf_base_pred, 'prob_con2', 'tx_binary')
rf_base_pred_rocset_2cat[rf_base_pred_rocset_2cat$inv_distance==max(rf_base_pred_rocset_2cat$inv_distance),]
rf_base_pred_rocset_4cat <- thresh_iter(.0, 1, .01, rf_base_pred, 'prob_con4', 'tx_binary')
rf_base_pred_rocset_4cat[rf_base_pred_rocset_4cat$inv_distance==max(rf_base_pred_rocset_4cat$inv_distance),]
rf_base_pred_rocset_5cat <- thresh_iter(.0, 1, .01, rf_base_pred, 'prob_con5', 'tx_binary')
rf_base_pred_rocset_5cat[rf_base_pred_rocset_5cat$inv_distance==max(rf_base_pred_rocset_5cat$inv_distance),]
rf_base_pred_rocset_7cat <- thresh_iter(.0, 1, .01, rf_base_pred, 'prob_con7', 'tx_binary')
rf_base_pred_rocset_7cat[rf_base_pred_rocset_7cat$inv_distance==max(rf_base_pred_rocset_7cat$inv_distance),]

cor(rf_base_pred[, grepl('prob', colnames(rf_base_pred))])

#Save Image
save.image('FalsePositives.Rdata')

#Random Forest Models - Incremental Scan - Subject
t0 <- Sys.time()
rft_sub_2cat <- tuneRF(y = as.factor(data[train & incremental, 'tx_binary']), x = cbind(subject_rhs[train & incremental,]), trace = TRUE, improve=0.0005, mtryStart = min(round(sqrt(ncol(subject_rhs[train & incremental,]))), max_mtry))
rft_sub_3cat <- tuneRF(y = as.factor(data[train & incremental & timeline_up, 'tx_3cat']), x = cbind(subject_rhs[train & incremental & timeline_up,]), trace = TRUE, improve=0.0005, mtryStart = min(round(sqrt(ncol(subject_rhs[train & incremental & timeline_up,]))), max_mtry), do.trace = TRUE)
rft_sub_4cat <- tuneRF(y = as.factor(data[train & incremental, 'tx_4cat']), x = cbind(subject_rhs[train & incremental,]), trace = TRUE, improve=0.0005, mtryStart = min(round(sqrt(ncol(subject_rhs[train & incremental,]))), max_mtry))
rft_sub_5cat <- tuneRF(y = as.factor(data[train & incremental, 'tx_5cat']), x = cbind(subject_rhs[train & incremental,]), trace = TRUE, improve=0.0005, mtryStart = min(round(sqrt(ncol(subject_rhs[train & incremental,]))), max_mtry))
rft_sub_7cat <- tuneRF(y = as.factor(data[train & incremental, 'tx_7cat']), x = cbind(subject_rhs[train & incremental,]), trace = TRUE, improve=0.0005, mtryStart = min(round(sqrt(ncol(subject_rhs[train & incremental,]))), max_mtry))
print(difftime(Sys.time(), t0, units = 'sec'))

t0 <- Sys.time()
rf_sub_2cat <- randomForest(y = as.factor(data[train & incremental, 'tx_binary']),x = subject_rhs[train & incremental,], do.trace = TRUE, ntree = 500, mtry = rft_sub_2cat[rft_sub_2cat[,2] == min(rft_sub_2cat[,2]),1], nodesize = 1000)
rf_sub_3cat <- randomForest(y = as.factor(data[train & incremental & data$tx_3cat < 2, 'tx_3cat']),x = subject_rhs[train & incremental & data$tx_3cat < 2,], do.trace = TRUE, ntree = 500, mtry = rft_sub_3cat[rft_sub_3cat[,2] == min(rft_sub_3cat[,2]),1], nodesize = 1000)

rf_sub_4cat <- randomForest(y = as.factor(data[train & incremental, 'tx_4cat']),x = subject_rhs[train & incremental,], do.trace = TRUE, ntree = 500, mtry = rft_sub_4cat[rft_sub_4cat[,2] == min(rft_sub_4cat[,2]),1], nodesize = 1000)
rf_sub_5cat <- randomForest(y = as.factor(data[train & incremental, 'tx_5cat']),x = subject_rhs[train & incremental,], do.trace = TRUE, ntree = 500, mtry = rft_sub_5cat[rft_sub_5cat[,2] == min(rft_sub_5cat[,2]),1], nodesize = 1000)
rf_sub_7cat <- randomForest(y = as.factor(data[train & incremental, 'tx_7cat']),x = subject_rhs[train & incremental,], do.trace = TRUE, ntree = 500, mtry = rft_sub_7cat[rft_sub_7cat[,2] == min(rft_sub_7cat[,2]),1], nodesize = 1000)
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
            
colnames(rf_sub_pred) <- c('tx_binary', 'tx_4cat', 'tx_5cat', 'tx_7cat', 
                            'prob_con2', 'prob_con4', 'prob_con5', 'prob_con7')

rf_sub_pred_rocset_2cat <- thresh_iter(.0, 1, .01, rf_sub_pred, 'prob_con2', 'tx_binary')
rf_sub_pred_rocset_2cat[rf_sub_pred_rocset_2cat$inv_distance==max(rf_sub_pred_rocset_2cat$inv_distance),]
rf_sub_pred_rocset_4cat <- thresh_iter(.0, 1, .01, rf_sub_pred, 'prob_con4', 'tx_binary')
rf_sub_pred_rocset_4cat[rf_sub_pred_rocset_4cat$inv_distance==max(rf_sub_pred_rocset_4cat$inv_distance),]
rf_sub_pred_rocset_5cat <- thresh_iter(.0, 1, .01, rf_sub_pred, 'prob_con5', 'tx_binary')
rf_sub_pred_rocset_5cat[rf_sub_pred_rocset_5cat$inv_distance==max(rf_sub_pred_rocset_5cat$inv_distance),]
rf_sub_pred_rocset_7cat <- thresh_iter(.0, 1, .01, rf_sub_pred, 'prob_con7', 'tx_binary')
rf_sub_pred_rocset_7cat[rf_sub_pred_rocset_7cat$inv_distance==max(rf_sub_pred_rocset_7cat$inv_distance),]

cor(rf_sub_pred[, grepl('prob', colnames(rf_sub_pred))])

#Save Image
save.image('FalsePositives.Rdata')

#Random Forest Models - Incremental Scan - Attachment Names
t0 <- Sys.time()
subsamp <- train
rft_att_2cat <- tuneRF(y = as.factor(data[train & incremental, 'tx_binary']), x = cbind(attname_rhs[train & incremental,]), trace = TRUE, improve=0.0005, mtryStart = min(round(sqrt(ncol(attname_rhs[train & incremental,]))), max_mtry))
rft_att_4cat <- tuneRF(y = as.factor(data[train & incremental, 'tx_4cat']), x = cbind(attname_rhs[train & incremental,]), trace = TRUE, improve=0.0005, mtryStart = min(round(sqrt(ncol(attname_rhs[train & incremental,]))), max_mtry))
rft_att_5cat <- tuneRF(y = as.factor(data[train & incremental, 'tx_5cat']), x = cbind(attname_rhs[train & incremental,]), trace = TRUE, improve=0.0005, mtryStart = min(round(sqrt(ncol(attname_rhs[train & incremental,]))), max_mtry))
rft_att_7cat <- tuneRF(y = as.factor(data[train & incremental, 'tx_7cat']), x = cbind(attname_rhs[train & incremental,]), trace = TRUE, improve=0.0005, mtryStart = min(round(sqrt(ncol(attname_rhs[train & incremental,]))), max_mtry))
print(difftime(Sys.time(), t0, units = 'sec'))

t0 <- Sys.time()
rf_att_2cat <- randomForest(y = as.factor(data[train & incremental, 'tx_binary']),x = attname_rhs[train & incremental,], do.trace = TRUE, ntree = 250, mtry = rft_att_2cat[rft_att_2cat[,2] == min(rft_att_2cat[,2]),1], nodesize = 1)
rf_att_4cat <- randomForest(y = as.factor(data[train & incremental, 'tx_4cat']),x = attname_rhs[train & incremental,], do.trace = TRUE, ntree = 500, mtry = rft_att_4cat[rft_att_4cat[,2] == min(rft_att_4cat[,2]),1], nodesize = 1000)
rf_att_5cat <- randomForest(y = as.factor(data[train & incremental, 'tx_5cat']),x = attname_rhs[train & incremental,], do.trace = TRUE, ntree = 500, mtry = rft_att_5cat[rft_att_5cat[,2] == min(rft_att_5cat[,2]),1], nodesize = 1000)
rf_att_7cat <- randomForest(y = as.factor(data[train & incremental, 'tx_7cat']),x = attname_rhs[train & incremental,], do.trace = TRUE, ntree = 500, mtry = rft_att_7cat[rft_att_7cat[,2] == min(rft_att_7cat[,2]),1], nodesize = 1000)
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

colnames(rf_att_pred) <- c('tx_binary', 'tx_4cat', 'tx_5cat', 'tx_7cat',
                            'prob_con2', 'prob_con4', 'prob_con5', 'prob_con7')

rf_att_pred_rocset_2cat <- thresh_iter(.0, 1, .01, rf_att_pred, 'prob_con2', 'tx_binary')
rf_att_pred_rocset_2cat[rf_att_pred_rocset_2cat$inv_distance==max(rf_att_pred_rocset_2cat$inv_distance),]
rf_att_pred_rocset_4cat <- thresh_iter(.0, 1, .01, rf_att_pred, 'prob_con4', 'tx_binary')
rf_att_pred_rocset_4cat[rf_att_pred_rocset_4cat$inv_distance==max(rf_att_pred_rocset_4cat$inv_distance),]
rf_att_pred_rocset_5cat <- thresh_iter(.0, 1, .01, rf_att_pred, 'prob_con5', 'tx_binary')
rf_att_pred_rocset_5cat[rf_att_pred_rocset_5cat$inv_distance==max(rf_att_pred_rocset_5cat$inv_distance),]
rf_att_pred_rocset_7cat <- thresh_iter(.0, 1, .01, rf_att_pred, 'prob_con7', 'tx_binary')
rf_att_pred_rocset_7cat[rf_att_pred_rocset_7cat$inv_distance==max(rf_att_pred_rocset_7cat$inv_distance),]

cor(rf_att_pred[, grepl('prob', colnames(rf_att_pred))])

#Save Image
save.image('FalsePositives.Rdata')

#Random Forest Models - Incremental Scan - Email Text
#Subsample for tuning
train_subsamp <- rand<.2

t0 <- Sys.time()
rft_emtxt_2cat <- tuneRF(y = as.factor(data[train_subsamp & incremental, 'tx_binary']), x = cbind(emailtxt_rhs_pca[train_subsamp & incremental,]), trace = TRUE, improve=0.0005, mtryStart = min(round(sqrt(ncol(emailtxt_rhs_pca[train_subsamp & incremental,]))), max_mtry), nodesize = 1000, do.trace = TRUE)
rft_emtxt_4cat <- tuneRF(y = as.factor(data[train_subsamp & incremental, 'tx_4cat']), x = cbind(emailtxt_rhs_pca[train_subsamp & incremental,]), trace = TRUE, improve=0.0005, mtryStart = min(round(sqrt(ncol(emailtxt_rhs_pca[train_subsamp & incremental,]))), max_mtry), nodesize = 1000, do.trace = TRUE)
rft_emtxt_5cat <- tuneRF(y = as.factor(data[train_subsamp & incremental, 'tx_5cat']), x = cbind(emailtxt_rhs_pca[train_subsamp & incremental,]), trace = TRUE, improve=0.0005, mtryStart = min(round(sqrt(ncol(emailtxt_rhs_pca[train_subsamp & incremental,]))), max_mtry), nodesize = 1000, do.trace = TRUE)
rft_emtxt_7cat <- tuneRF(y = as.factor(data[train_subsamp & incremental, 'tx_7cat']), x = cbind(emailtxt_rhs_pca[train_subsamp & incremental,]), trace = TRUE, improve=0.0005, mtryStart = min(round(sqrt(ncol(emailtxt_rhs_pca[train_subsamp & incremental,]))), max_mtry), nodesize = 1000, do.trace = TRUE)
print(difftime(Sys.time(), t0, units = 'sec'))

t0 <- Sys.time()
rf_emtxt_2cat <- randomForest(y = as.factor(data[train & incremental, 'tx_binary']),x = emailtxt_rhs_pca[train & incremental,], do.trace = TRUE, ntree = 250, mtry = rft_emtxt_2cat[rft_emtxt_2cat[,2] == min(rft_emtxt_2cat[,2]),1], nodesize = 1000)
rf_emtxt_4cat <- randomForest(y = as.factor(data[train & incremental, 'tx_4cat']),x = emailtxt_rhs_pca[train & incremental,], do.trace = TRUE, ntree = 250, mtry = rft_emtxt_4cat[rft_emtxt_4cat[,2] == min(rft_emtxt_4cat[,2]),1], nodesize = 1000)
rf_emtxt_5cat <- randomForest(y = as.factor(data[train & incremental, 'tx_5cat']),x = emailtxt_rhs_pca[train & incremental,], do.trace = TRUE, ntree = 250, mtry = rft_emtxt_5cat[rft_emtxt_5cat[,2] == min(rft_emtxt_5cat[,2]),1], nodesize = 1000)
rf_emtxt_7cat <- randomForest(y = as.factor(data[train & incremental, 'tx_7cat']),x = emailtxt_rhs_pca[train & incremental,], do.trace = TRUE, ntree = 300, mtry = rft_emtxt_7cat[rft_emtxt_7cat[,2] == min(rft_emtxt_7cat[,2]),1], nodesize = 1000)
print(difftime(Sys.time(), t0, units = 'sec'))

rf_emtxt_pred <- cbind.data.frame(data[train & incremental, 'tx_binary'], 
              data[train & incremental, 'tx_4cat'], 
              data[train & incremental, 'tx_5cat'],
              data[train & incremental, 'tx_7cat'],               
              rf_emtxt_2cat$votes[,ncol(rf_emtxt_2cat$votes)],
              rowSums(rf_emtxt_4cat$votes[,c(2:ncol(rf_emtxt_4cat$votes))]),
              rowSums(rf_emtxt_5cat$votes[,c(2:ncol(rf_emtxt_5cat$votes))]),
              rowSums(rf_emtxt_7cat$votes[,c(4:ncol(rf_emtxt_7cat$votes))])              
              )


colnames(rf_emtxt_pred) <- c('tx_binary', 'tx_7cat', 'tx_4cat', 'tx_5cat',
                              'prob_con2', 'prob_con4', 'prob_con5', 'prob_con7')

rf_emtxt_pred_rocset_2cat <- thresh_iter(.0, 1, .01, rf_emtxt_pred, 'prob_con2', 'tx_binary')
rf_emtxt_pred_rocset_2cat[rf_emtxt_pred_rocset_2cat$inv_distance==max(rf_emtxt_pred_rocset_2cat$inv_distance),]
rf_emtxt_pred_rocset_4cat <- thresh_iter(.0, 1, .01, rf_emtxt_pred, 'prob_con4', 'tx_binary')
rf_emtxt_pred_rocset_4cat[rf_emtxt_pred_rocset_4cat$inv_distance==max(rf_emtxt_pred_rocset_4cat$inv_distance),]
rf_emtxt_pred_rocset_5cat <- thresh_iter(.0, 1, .01, rf_emtxt_pred, 'prob_con5', 'tx_binary')
rf_emtxt_pred_rocset_5cat[rf_emtxt_pred_rocset_5cat$inv_distance==max(rf_emtxt_pred_rocset_5cat$inv_distance),]
rf_emtxt_pred_rocset_7cat <- thresh_iter(.0, 1, .01, rf_emtxt_pred, 'prob_con7', 'tx_binary')
rf_emtxt_pred_rocset_7cat[rf_emtxt_pred_rocset_7cat$inv_distance==max(rf_emtxt_pred_rocset_7cat$inv_distance),]

cor(rf_emtxt_pred[, grepl('prob', colnames(rf_emtxt_pred))])

#Save Image
save.image('FalsePositives.Rdata')

#Random Forest Models - Incremental Scan - Document Text
t0 <- Sys.time()
rft_doctxt_2cat <- tuneRF(y = as.factor(data[train_subsamp & incremental, 'tx_binary']), x = cbind(doctxt_rhs_pca[train_subsamp & incremental,]), trace = TRUE, improve=0.0005, mtryStart = min(round(sqrt(ncol(doctxt_rhs_pca[train_subsamp & incremental,]))), max_mtry), nodesize = 1000, do.trace = TRUE)
rft_doctxt_4cat <- tuneRF(y = as.factor(data[train_subsamp & incremental, 'tx_4cat']), x = cbind(doctxt_rhs_pca[train_subsamp & incremental,]), trace = TRUE, improve=0.0005, mtryStart = min(round(sqrt(ncol(doctxt_rhs_pca[train_subsamp & incremental,]))), max_mtry), nodesize = 1000, do.trace = TRUE)
rft_doctxt_5cat <- tuneRF(y = as.factor(data[train_subsamp & incremental, 'tx_5cat']), x = cbind(doctxt_rhs_pca[train_subsamp & incremental,]), trace = TRUE, improve=0.0005, mtryStart = min(round(sqrt(ncol(doctxt_rhs_pca[train_subsamp & incremental,]))), max_mtry), nodesize = 1000, do.trace = TRUE)
rft_doctxt_7cat <- tuneRF(y = as.factor(data[train_subsamp & incremental, 'tx_7cat']), x = cbind(doctxt_rhs_pca[train_subsamp & incremental,]), trace = TRUE, improve=0.0005, mtryStart = min(round(sqrt(ncol(doctxt_rhs_pca[train_subsamp & incremental,]))), max_mtry), nodesize = 1000, do.trace = TRUE)
print(difftime(Sys.time(), t0, units = 'sec'))

t0 <- Sys.time()
rf_doctxt_2cat <- randomForest(y = as.factor(data[train & incremental, 'tx_binary']),x = doctxt_rhs_pca[train & incremental,], do.trace = TRUE, ntree = 150, mtry = rft_doctxt_2cat[rft_doctxt_2cat[,2] == min(rft_doctxt_2cat[,2]),1], nodesize = 1000)
rf_doctxt_4cat <- randomForest(y = as.factor(data[train & incremental, 'tx_4cat']),x = doctxt_rhs_pca[train & incremental,], do.trace = TRUE, ntree = 250, mtry = rft_doctxt_4cat[rft_doctxt_4cat[,2] == min(rft_doctxt_4cat[,2]),1], nodesize = 1000)
rf_doctxt_5cat <- randomForest(y = as.factor(data[train & incremental, 'tx_5cat']),x = doctxt_rhs_pca[train & incremental,], do.trace = TRUE, ntree = 250, mtry = rft_doctxt_5cat[rft_doctxt_5cat[,2] == min(rft_doctxt_5cat[,2]),1], nodesize = 1000)
rf_doctxt_7cat <- randomForest(y = as.factor(data[train & incremental, 'tx_7cat']),x = doctxt_rhs_pca[train & incremental,], do.trace = TRUE, ntree = 250, mtry = rft_doctxt_7cat[rft_doctxt_7cat[,2] == min(rft_doctxt_7cat[,2]),1], nodesize = 1000)
print(difftime(Sys.time(), t0, units = 'sec'))

rf_doctxt_pred <- cbind.data.frame(data[train & incremental, 'tx_binary'], 
              data[train & incremental, 'tx_4cat'], 
              data[train & incremental, 'tx_5cat'],
              data[train & incremental, 'tx_7cat'],               
              rf_doctxt_2cat$votes[,ncol(rf_doctxt_2cat$votes)],
              rowSums(rf_doctxt_4cat$votes[,c(2:ncol(rf_doctxt_4cat$votes))]),
              rowSums(rf_doctxt_5cat$votes[,c(2:ncol(rf_doctxt_5cat$votes))]),
              rowSums(rf_doctxt_7cat$votes[,c(4:ncol(rf_doctxt_7cat$votes))])               
              )

colnames(rf_doctxt_pred) <- c('tx_binary', 'tx_7cat', 'tx_4cat', 'tx_5cat',
                            'prob_con2', 'prob_con4', 'prob_con5', 'prob_con7')

rf_doctxt_pred_rocset_2cat <- thresh_iter(.0, 1, .01, rf_doctxt_pred, 'prob_con2', 'tx_binary')
rf_doctxt_pred_rocset_2cat[rf_doctxt_pred_rocset_2cat$inv_distance==max(rf_doctxt_pred_rocset_2cat$inv_distance),]
rf_doctxt_pred_rocset_4cat <- thresh_iter(.0, 1, .01, rf_doctxt_pred, 'prob_con4', 'tx_binary')
rf_doctxt_pred_rocset_4cat[rf_doctxt_pred_rocset_4cat$inv_distance==max(rf_doctxt_pred_rocset_4cat$inv_distance),]
rf_doctxt_pred_rocset_5cat <- thresh_iter(.0, 1, .01, rf_doctxt_pred, 'prob_con5', 'tx_binary')
rf_doctxt_pred_rocset_5cat[rf_doctxt_pred_rocset_5cat$inv_distance==max(rf_doctxt_pred_rocset_5cat$inv_distance),]
rf_doctxt_pred_rocset_7cat <- thresh_iter(.0, 1, .01, rf_doctxt_pred, 'prob_con7', 'tx_binary')
rf_doctxt_pred_rocset_7cat[rf_doctxt_pred_rocset_7cat$inv_distance==max(rf_doctxt_pred_rocset_7cat$inv_distance),]

cor(rf_doctxt_pred[, grepl('prob', colnames(rf_doctxt_pred))])

#Save Image
save.image('FalsePositives.Rdata')

#Combo 1: Base + Subject + Attname
t0 <- Sys.time()
rft_metacombo_2cat <- tuneRF(y = as.factor(data[train_subsamp & incremental, 'tx_binary']), x = cbind(base_rhs[train_subsamp & incremental,], subject_rhs[train_subsamp & incremental,], attname_rhs[train_subsamp & incremental,]), trace = TRUE, improve=0.0005, mtryStart = min(round(sqrt(ncol(cbind(base_rhs[train_subsamp & incremental,], subject_rhs[train_subsamp & incremental,], attname_rhs[train_subsamp & incremental,])))), max_mtry), nodesize = 1000, do.trace = TRUE)
rft_metacombo_4cat <- tuneRF(y = as.factor(data[train_subsamp & incremental, 'tx_4cat']), x = cbind(base_rhs[train_subsamp & incremental,], subject_rhs[train_subsamp & incremental,], attname_rhs[train_subsamp & incremental,]), trace = TRUE, improve=0.0005, mtryStart = min(round(sqrt(ncol(cbind(base_rhs[train_subsamp & incremental,], subject_rhs[train_subsamp & incremental,], attname_rhs[train_subsamp & incremental,])))), max_mtry), nodesize = 1000, do.trace = TRUE)
rft_metacombo_5cat <- tuneRF(y = as.factor(data[train_subsamp & incremental, 'tx_5cat']), x = cbind(base_rhs[train_subsamp & incremental,], subject_rhs[train_subsamp & incremental,], attname_rhs[train_subsamp & incremental,]), trace = TRUE, improve=0.0005, mtryStart = min(round(sqrt(ncol(cbind(base_rhs[train_subsamp & incremental,], subject_rhs[train_subsamp & incremental,], attname_rhs[train_subsamp & incremental,])))), max_mtry), nodesize = 1000, do.trace = TRUE)
rft_metacombo_7cat <- tuneRF(y = as.factor(data[train_subsamp & incremental, 'tx_7cat']), x = cbind(base_rhs[train_subsamp & incremental,], subject_rhs[train_subsamp & incremental,], attname_rhs[train_subsamp & incremental,]), trace = TRUE, improve=0.0005, mtryStart = min(round(sqrt(ncol(cbind(base_rhs[train_subsamp & incremental,], subject_rhs[train_subsamp & incremental,], attname_rhs[train_subsamp & incremental,])))), max_mtry), nodesize = 1000, do.trace = TRUE)
print(difftime(Sys.time(), t0, units = 'sec'))

t0 <- Sys.time()
rf_metacombo_2cat <- randomForest(y = as.factor(data[train & incremental, 'tx_binary']), x = cbind(base_rhs[train & incremental,], subject_rhs[train & incremental,], attname_rhs[train & incremental,]), do.trace = TRUE, ntree = 250, mtry = rft_metacombo_2cat[rft_metacombo_2cat[,2] == min(rft_metacombo_2cat[,2]),1], nodesize = 1000)
rf_metacombo_4cat <- randomForest(y = as.factor(data[train & incremental, 'tx_4cat']), x = cbind(base_rhs[train & incremental,], subject_rhs[train & incremental,], attname_rhs[train & incremental,]), do.trace = TRUE, ntree = 250, mtry = rft_metacombo_4cat[rft_metacombo_4cat[,2] == min(rft_metacombo_4cat[,2]),1], nodesize = 1000)
rf_metacombo_5cat <- randomForest(y = as.factor(data[train & incremental, 'tx_5cat']), x = cbind(base_rhs[train & incremental,], subject_rhs[train & incremental,], attname_rhs[train & incremental,]), do.trace = TRUE, ntree = 250, mtry = rft_metacombo_5cat[rft_metacombo_5cat[,2] == min(rft_metacombo_5cat[,2]),1], nodesize = 1000)
rf_metacombo_7cat <- randomForest(y = as.factor(data[train & incremental, 'tx_7cat']), x = cbind(base_rhs[train & incremental,], subject_rhs[train & incremental,], attname_rhs[train & incremental,]), do.trace = TRUE, ntree = 250, mtry = rft_metacombo_7cat[rft_metacombo_7cat[,2] == min(rft_metacombo_7cat[,2]),1], nodesize = 1000)
print(difftime(Sys.time(), t0, units = 'sec'))

rf_metacombo_pred <- cbind.data.frame(data[train & incremental, 'tx_binary'], 
              data[train & incremental, 'tx_4cat'], 
              data[train & incremental, 'tx_5cat'],
              data[train & incremental, 'tx_7cat'],               
              rf_metacombo_2cat$votes[,ncol(rf_metacombo_2cat$votes)],
              rowSums(rf_metacombo_4cat$votes[,c(2:ncol(rf_metacombo_4cat$votes))]),
              rowSums(rf_metacombo_5cat$votes[,c(2:ncol(rf_metacombo_5cat$votes))]),
              rowSums(rf_metacombo_7cat$votes[,c(4:ncol(rf_metacombo_7cat$votes))])               
              )

colnames(rf_metacombo_pred) <- c('tx_binary', 'tx_7cat', 'tx_4cat', 'tx_5cat',
                            'prob_con2', 'prob_con4', 'prob_con5', 'prob_con7')

rf_metacombo_pred_rocset_2cat <- thresh_iter(.0, 1, .01, rf_metacombo_pred, 'prob_con2', 'tx_binary')
rf_metacombo_pred_rocset_2cat[rf_metacombo_pred_rocset_2cat$inv_distance==max(rf_metacombo_pred_rocset_2cat$inv_distance),]
rf_metacombo_pred_rocset_4cat <- thresh_iter(.0, 1, .01, rf_metacombo_pred, 'prob_con4', 'tx_binary')
rf_metacombo_pred_rocset_4cat[rf_metacombo_pred_rocset_4cat$inv_distance==max(rf_metacombo_pred_rocset_4cat$inv_distance),]
rf_metacombo_pred_rocset_5cat <- thresh_iter(.0, 1, .01, rf_metacombo_pred, 'prob_con5', 'tx_binary')
rf_metacombo_pred_rocset_5cat[rf_metacombo_pred_rocset_5cat$inv_distance==max(rf_metacombo_pred_rocset_5cat$inv_distance),]
rf_metacombo_pred_rocset_7cat <- thresh_iter(.0, 1, .01, rf_metacombo_pred, 'prob_con7', 'tx_binary')
rf_metacombo_pred_rocset_7cat[rf_metacombo_pred_rocset_7cat$inv_distance==max(rf_metacombo_pred_rocset_7cat$inv_distance),]

cor(rf_metacombo_pred[, grepl('prob', colnames(rf_metacombo_pred))])

#Save Image
save.image('FalsePositives.Rdata')

#Combo 2: Email text + Document text
t0 <- Sys.time()
rft_txtcombo_2cat <- tuneRF(y = as.factor(data[train_subsamp & incremental, 'tx_binary']), x = cbind(emailtxt_rhs_pca[train_subsamp & incremental,], doctxt_rhs_pca[train_subsamp & incremental,]), trace = TRUE, improve=0.0005, mtryStart = min(round(sqrt(ncol(cbind(emailtxt_rhs_pca[train_subsamp & incremental,], doctxt_rhs_pca[train_subsamp & incremental,])))), max_mtry), nodesize = 1000, do.trace = TRUE)
rft_txtcombo_4cat <- tuneRF(y = as.factor(data[train_subsamp & incremental, 'tx_4cat']), x = cbind(emailtxt_rhs_pca[train_subsamp & incremental,], doctxt_rhs_pca[train_subsamp & incremental,]), trace = TRUE, improve=0.0005, mtryStart = min(round(sqrt(ncol(cbind(emailtxt_rhs_pca[train_subsamp & incremental,], doctxt_rhs_pca[train_subsamp & incremental,])))), max_mtry), nodesize = 1000, do.trace = TRUE)
rft_txtcombo_5cat <- tuneRF(y = as.factor(data[train_subsamp & incremental, 'tx_5cat']), x = cbind(emailtxt_rhs_pca[train_subsamp & incremental,], doctxt_rhs_pca[train_subsamp & incremental,]), trace = TRUE, improve=0.0005, mtryStart = min(round(sqrt(ncol(cbind(emailtxt_rhs_pca[train_subsamp & incremental,], doctxt_rhs_pca[train_subsamp & incremental,])))), max_mtry), nodesize = 1000, do.trace = TRUE)
rft_txtcombo_7cat <- tuneRF(y = as.factor(data[train_subsamp & incremental, 'tx_7cat']), x = cbind(emailtxt_rhs_pca[train_subsamp & incremental,], doctxt_rhs_pca[train_subsamp & incremental,]), trace = TRUE, improve=0.0005, mtryStart = min(round(sqrt(ncol(cbind(emailtxt_rhs_pca[train_subsamp & incremental,], doctxt_rhs_pca[train_subsamp & incremental,])))), max_mtry), nodesize = 1000, do.trace = TRUE)
print(difftime(Sys.time(), t0, units = 'sec'))

t0 <- Sys.time()
rf_txtcombo_2cat <- randomForest(y = as.factor(data[train & incremental, 'tx_binary']), x = cbind(emailtxt_rhs_pca[train & incremental,], doctxt_rhs_pca[train & incremental,]), do.trace = TRUE, ntree = 250, mtry = rft_txtcombo_2cat[rft_txtcombo_2cat[,2] == min(rft_txtcombo_2cat[,2]),1], nodesize = 1000)
rf_txtcombo_4cat <- randomForest(y = as.factor(data[train & incremental, 'tx_4cat']), x = cbind(emailtxt_rhs_pca[train & incremental,], doctxt_rhs_pca[train & incremental,]), do.trace = TRUE, ntree = 250, mtry = rft_txtcombo_4cat[rft_txtcombo_4cat[,2] == min(rft_txtcombo_4cat[,2]),1], nodesize = 1000)
rf_txtcombo_5cat <- randomForest(y = as.factor(data[train & incremental, 'tx_5cat']), x = cbind(emailtxt_rhs_pca[train & incremental,], doctxt_rhs_pca[train & incremental,]), do.trace = TRUE, ntree = 250, mtry = rft_txtcombo_5cat[rft_txtcombo_5cat[,2] == min(rft_txtcombo_5cat[,2]),1], nodesize = 1000)
rf_txtcombo_7cat <- randomForest(y = as.factor(data[train & incremental, 'tx_7cat']), x = cbind(emailtxt_rhs_pca[train & incremental,], doctxt_rhs_pca[train & incremental,]), do.trace = TRUE, ntree = 250, mtry = rft_txtcombo_7cat[rft_txtcombo_7cat[,2] == min(rft_txtcombo_7cat[,2]),1], nodesize = 1000)
print(difftime(Sys.time(), t0, units = 'sec'))

rf_txtcombo_pred <- cbind.data.frame(data[train & incremental, 'tx_binary'], 
              data[train & incremental, 'tx_4cat'], 
              data[train & incremental, 'tx_5cat'],
              data[train & incremental, 'tx_7cat'],               
              rf_txtcombo_2cat$votes[,ncol(rf_txtcombo_2cat$votes)],
              rowSums(rf_txtcombo_4cat$votes[,c(2:ncol(rf_txtcombo_4cat$votes))]),
              rowSums(rf_txtcombo_5cat$votes[,c(2:ncol(rf_txtcombo_5cat$votes))]),
              rowSums(rf_txtcombo_7cat$votes[,c(4:ncol(rf_txtcombo_7cat$votes))])               
              )

colnames(rf_txtcombo_pred) <- c('tx_binary', 'tx_7cat', 'tx_4cat', 'tx_5cat',
                            'prob_con2', 'prob_con4', 'prob_con5', 'prob_con7')

rf_txtcombo_pred_rocset_2cat <- thresh_iter(.0, 1, .01, rf_txtcombo_pred, 'prob_con2', 'tx_binary')
rf_txtcombo_pred_rocset_2cat[rf_txtcombo_pred_rocset_2cat$inv_distance==max(rf_txtcombo_pred_rocset_2cat$inv_distance),]
rf_txtcombo_pred_rocset_4cat <- thresh_iter(.0, 1, .01, rf_txtcombo_pred, 'prob_con4', 'tx_binary')
rf_txtcombo_pred_rocset_4cat[rf_txtcombo_pred_rocset_4cat$inv_distance==max(rf_txtcombo_pred_rocset_4cat$inv_distance),]
rf_txtcombo_pred_rocset_5cat <- thresh_iter(.0, 1, .01, rf_txtcombo_pred, 'prob_con5', 'tx_binary')
rf_txtcombo_pred_rocset_5cat[rf_txtcombo_pred_rocset_5cat$inv_distance==max(rf_txtcombo_pred_rocset_5cat$inv_distance),]
rf_txtcombo_pred_rocset_7cat <- thresh_iter(.0, 1, .01, rf_txtcombo_pred, 'prob_con7', 'tx_binary')
rf_txtcombo_pred_rocset_7cat[rf_txtcombo_pred_rocset_7cat$inv_distance==max(rf_txtcombo_pred_rocset_7cat$inv_distance),]

cor(rf_txtcombo_pred[, grepl('prob', colnames(rf_txtcombo_pred))])

#Save Image
save.image('FalsePositives.Rdata')

#Combo 3: All Available RHS Data
t0 <- Sys.time()
rft_fullcombo_2cat <- tuneRF(y = as.factor(data[train_subsamp & incremental, 'tx_binary']), x = cbind(base_rhs[train_subsamp & incremental,], subject_rhs[train_subsamp & incremental,], attname_rhs[train_subsamp & incremental,], emailtxt_rhs_pca[train_subsamp & incremental,], doctxt_rhs_pca[train_subsamp & incremental,]), trace = TRUE, improve=0.0005, mtryStart = min(round(sqrt(ncol(cbind(base_rhs[train_subsamp & incremental,], subject_rhs[train_subsamp & incremental,], attname_rhs[train_subsamp & incremental,], emailtxt_rhs_pca[train_subsamp & incremental,], doctxt_rhs_pca[train_subsamp & incremental,])))), max_mtry), nodesize = 1000, do.trace = TRUE)
rft_fullcombo_3cat <- tuneRF(y = as.factor(data[train_subsamp & incremental & timeline_up, 'tx_3cat']), x = cbind(base_rhs[train_subsamp & incremental & timeline_up,], subject_rhs[train_subsamp & incremental & timeline_up,], attname_rhs[train_subsamp & incremental & timeline_up,], emailtxt_rhs_pca[train_subsamp & incremental & timeline_up,], doctxt_rhs_pca[train_subsamp & incremental & timeline_up,]), trace = TRUE, improve=0.0005, mtryStart = min(round(sqrt(ncol(cbind(base_rhs[train_subsamp & incremental & timeline_up,], subject_rhs[train_subsamp & incremental & timeline_up,], attname_rhs[train_subsamp & incremental & timeline_up,], emailtxt_rhs_pca[train_subsamp & incremental & timeline_up,], doctxt_rhs_pca[train_subsamp & incremental & timeline_up,])))), max_mtry), nodesize = 1000, do.trace = TRUE)

rft_fullcombo_4cat <- tuneRF(y = as.factor(data[train_subsamp & incremental, 'tx_4cat']), x = cbind(base_rhs[train_subsamp & incremental,], subject_rhs[train_subsamp & incremental,], attname_rhs[train_subsamp & incremental,], emailtxt_rhs_pca[train_subsamp & incremental,], doctxt_rhs_pca[train_subsamp & incremental,]), trace = TRUE, improve=0.0005, mtryStart = min(round(sqrt(ncol(cbind(base_rhs[train_subsamp & incremental,], subject_rhs[train_subsamp & incremental,], attname_rhs[train_subsamp & incremental,], emailtxt_rhs_pca[train_subsamp & incremental,], doctxt_rhs_pca[train_subsamp & incremental,])))), max_mtry), nodesize = 1000, do.trace = TRUE)
rft_fullcombo_5cat <- tuneRF(y = as.factor(data[train_subsamp & incremental, 'tx_5cat']), x = cbind(base_rhs[train_subsamp & incremental,], subject_rhs[train_subsamp & incremental,], attname_rhs[train_subsamp & incremental,], emailtxt_rhs_pca[train_subsamp & incremental,], doctxt_rhs_pca[train_subsamp & incremental,]), trace = TRUE, improve=0.0005, mtryStart = min(round(sqrt(ncol(cbind(base_rhs[train_subsamp & incremental,], subject_rhs[train_subsamp & incremental,], attname_rhs[train_subsamp & incremental,], emailtxt_rhs_pca[train_subsamp & incremental,], doctxt_rhs_pca[train_subsamp & incremental,])))), max_mtry), nodesize = 1000, do.trace = TRUE)
rft_fullcombo_7cat <- tuneRF(y = as.factor(data[train_subsamp & incremental, 'tx_7cat']), x = cbind(base_rhs[train_subsamp & incremental,], subject_rhs[train_subsamp & incremental,], attname_rhs[train_subsamp & incremental,], emailtxt_rhs_pca[train_subsamp & incremental,], doctxt_rhs_pca[train_subsamp & incremental,]), trace = TRUE, improve=0.0005, mtryStart = min(round(sqrt(ncol(cbind(base_rhs[train_subsamp & incremental,], subject_rhs[train_subsamp & incremental,], attname_rhs[train_subsamp & incremental,], emailtxt_rhs_pca[train_subsamp & incremental,], doctxt_rhs_pca[train_subsamp & incremental,])))), max_mtry), nodesize = 1000, do.trace = TRUE)
print(difftime(Sys.time(), t0, units = 'sec'))

t0 <- Sys.time()
rf_fullcombo_2cat <- randomForest(y = as.factor(data[train & incremental, 'tx_binary']), x = cbind(base_rhs[train & incremental,], subject_rhs[train & incremental,], attname_rhs[train & incremental,], emailtxt_rhs_pca[train & incremental,], doctxt_rhs_pca[train & incremental,]), do.trace = TRUE, ntree = 250, mtry = rft_fullcombo_2cat[rft_fullcombo_2cat[,2] == min(rft_fullcombo_2cat[,2]),1], nodesize = 1000)
rf_fullcombo_3cat <- randomForest(y = as.factor(data[train & incremental & timeline_up, 'tx_3cat']), x = cbind(base_rhs[train & incremental & timeline_up,], subject_rhs[train & incremental & timeline_up,], attname_rhs[train & incremental & timeline_up,], emailtxt_rhs_pca[train & incremental & timeline_up,], doctxt_rhs_pca[train & incremental & timeline_up,]), do.trace = TRUE, ntree = 250, mtry = rft_fullcombo_3cat[rft_fullcombo_3cat[,2] == min(rft_fullcombo_3cat[,2]),1], nodesize = 1000)

rf_fullcombo_4cat <- randomForest(y = as.factor(data[train & incremental, 'tx_4cat']), x = cbind(base_rhs[train & incremental,], subject_rhs[train & incremental,], attname_rhs[train & incremental,], emailtxt_rhs_pca[train & incremental,], doctxt_rhs_pca[train & incremental,]), do.trace = TRUE, ntree = 250, mtry = rft_fullcombo_4cat[rft_fullcombo_4cat[,2] == min(rft_fullcombo_4cat[,2]),1], nodesize = 1000)
rf_fullcombo_5cat <- randomForest(y = as.factor(data[train & incremental, 'tx_5cat']), x = cbind(base_rhs[train & incremental,], subject_rhs[train & incremental,], attname_rhs[train & incremental,], emailtxt_rhs_pca[train & incremental,], doctxt_rhs_pca[train & incremental,]), do.trace = TRUE, ntree = 250, mtry = rft_fullcombo_5cat[rft_fullcombo_5cat[,2] == min(rft_fullcombo_5cat[,2]),1], nodesize = 1000)
rf_fullcombo_7cat <- randomForest(y = as.factor(data[train & incremental, 'tx_7cat']), x = cbind(base_rhs[train & incremental,], subject_rhs[train & incremental,], attname_rhs[train & incremental,], emailtxt_rhs_pca[train & incremental,], doctxt_rhs_pca[train & incremental,]), do.trace = TRUE, ntree = 250, mtry = rft_fullcombo_7cat[rft_fullcombo_7cat[,2] == min(rft_fullcombo_7cat[,2]),1], nodesize = 1000)
print(difftime(Sys.time(), t0, units = 'sec'))

rf_fullcombo_pred <- cbind.data.frame(data[train & incremental, 'tx_binary'], 
              data[train & incremental, 'tx_4cat'], 
              data[train & incremental, 'tx_5cat'],
              data[train & incremental, 'tx_7cat'],               
              rf_fullcombo_2cat$votes[,ncol(rf_fullcombo_2cat$votes)],
              rowSums(rf_fullcombo_4cat$votes[,c(2:ncol(rf_fullcombo_4cat$votes))]),
              rowSums(rf_fullcombo_5cat$votes[,c(2:ncol(rf_fullcombo_5cat$votes))]),
              rowSums(rf_fullcombo_7cat$votes[,c(4:ncol(rf_fullcombo_7cat$votes))])               
              )

colnames(rf_fullcombo_pred) <- c('tx_binary', 'tx_7cat', 'tx_4cat', 'tx_5cat',
                            'prob_con2', 'prob_con4', 'prob_con5', 'prob_con7')

rf_fullcombo_pred_rocset_2cat <- thresh_iter(.0, 1, .01, rf_fullcombo_pred, 'prob_con2', 'tx_binary')
rf_fullcombo_pred_rocset_2cat[rf_fullcombo_pred_rocset_2cat$inv_distance==max(rf_fullcombo_pred_rocset_2cat$inv_distance),]
rf_fullcombo_pred_rocset_4cat <- thresh_iter(.0, 1, .01, rf_fullcombo_pred, 'prob_con4', 'tx_binary')
rf_fullcombo_pred_rocset_4cat[rf_fullcombo_pred_rocset_4cat$inv_distance==max(rf_fullcombo_pred_rocset_4cat$inv_distance),]
rf_fullcombo_pred_rocset_5cat <- thresh_iter(.0, 1, .01, rf_fullcombo_pred, 'prob_con5', 'tx_binary')
rf_fullcombo_pred_rocset_5cat[rf_fullcombo_pred_rocset_5cat$inv_distance==max(rf_fullcombo_pred_rocset_5cat$inv_distance),]
rf_fullcombo_pred_rocset_7cat <- thresh_iter(.0, 1, .01, rf_fullcombo_pred, 'prob_con7', 'tx_binary')
rf_fullcombo_pred_rocset_7cat[rf_fullcombo_pred_rocset_7cat$inv_distance==max(rf_fullcombo_pred_rocset_7cat$inv_distance),]

cor(rf_fullcombo_pred[, grepl('prob', colnames(rf_fullcombo_pred))])

#Save Image
save.image('FalsePositives.Rdata')

#Ensemble of Ensembles
tot_auc_2cat <- rf_base_pred_rocset_2cat[rf_base_pred_rocset_2cat$inv_distance==max(rf_base_pred_rocset_2cat$inv_distance),ncol(rf_base_pred_rocset_2cat)] + 
            	rf_sub_pred_rocset_2cat[rf_sub_pred_rocset_2cat$inv_distance==max(rf_sub_pred_rocset_2cat$inv_distance),ncol(rf_sub_pred_rocset_2cat)] + 
           		#rf_att_pred_rocset_2cat[rf_att_pred_rocset_2cat$inv_distance==max(rf_att_pred_rocset_2cat$inv_distance),ncol(rf_att_pred_rocset_2cat)] + 
            	rf_emtxt_pred_rocset_2cat[rf_emtxt_pred_rocset_2cat$inv_distance==max(rf_emtxt_pred_rocset_2cat$inv_distance),ncol(rf_emtxt_pred_rocset_2cat)] + 
            	rf_doctxt_pred_rocset_2cat[rf_doctxt_pred_rocset_2cat$inv_distance==max(rf_doctxt_pred_rocset_2cat$inv_distance),ncol(rf_doctxt_pred_rocset_2cat)] + 
				rf_metacombo_pred_rocset_2cat[rf_metacombo_pred_rocset_2cat$inv_distance==max(rf_metacombo_pred_rocset_2cat$inv_distance),ncol(rf_metacombo_pred_rocset_2cat)] +
				rf_txtcombo_pred_rocset_2cat[rf_txtcombo_pred_rocset_2cat$inv_distance==max(rf_txtcombo_pred_rocset_2cat$inv_distance),ncol(rf_txtcombo_pred_rocset_2cat)] +
				rf_fullcombo_pred_rocset_2cat[rf_fullcombo_pred_rocset_2cat$inv_distance==max(rf_fullcombo_pred_rocset_2cat$inv_distance),ncol(rf_fullcombo_pred_rocset_2cat)]

tot_auc_4cat <- rf_base_pred_rocset_4cat[rf_base_pred_rocset_4cat$inv_distance==max(rf_base_pred_rocset_4cat$inv_distance),ncol(rf_base_pred_rocset_4cat)] + 
            	rf_sub_pred_rocset_4cat[rf_sub_pred_rocset_4cat$inv_distance==max(rf_sub_pred_rocset_4cat$inv_distance),ncol(rf_sub_pred_rocset_4cat)] + 
           		rf_att_pred_rocset_4cat[rf_att_pred_rocset_4cat$inv_distance==max(rf_att_pred_rocset_4cat$inv_distance),ncol(rf_att_pred_rocset_4cat)] + 
            	rf_emtxt_pred_rocset_4cat[rf_emtxt_pred_rocset_4cat$inv_distance==max(rf_emtxt_pred_rocset_4cat$inv_distance),ncol(rf_emtxt_pred_rocset_4cat)] + 
            	rf_doctxt_pred_rocset_4cat[rf_doctxt_pred_rocset_4cat$inv_distance==max(rf_doctxt_pred_rocset_4cat$inv_distance),ncol(rf_doctxt_pred_rocset_4cat)] + 
				rf_metacombo_pred_rocset_4cat[rf_metacombo_pred_rocset_4cat$inv_distance==max(rf_metacombo_pred_rocset_4cat$inv_distance),ncol(rf_metacombo_pred_rocset_4cat)] +
				rf_txtcombo_pred_rocset_4cat[rf_txtcombo_pred_rocset_4cat$inv_distance==max(rf_txtcombo_pred_rocset_4cat$inv_distance),ncol(rf_txtcombo_pred_rocset_4cat)] +
				rf_fullcombo_pred_rocset_4cat[rf_fullcombo_pred_rocset_4cat$inv_distance==max(rf_fullcombo_pred_rocset_4cat$inv_distance),ncol(rf_fullcombo_pred_rocset_4cat)]

tot_auc_5cat <- rf_base_pred_rocset_5cat[rf_base_pred_rocset_5cat$inv_distance==max(rf_base_pred_rocset_5cat$inv_distance),ncol(rf_base_pred_rocset_5cat)] + 
            	rf_sub_pred_rocset_5cat[rf_sub_pred_rocset_5cat$inv_distance==max(rf_sub_pred_rocset_5cat$inv_distance),ncol(rf_sub_pred_rocset_5cat)] + 
           		rf_att_pred_rocset_5cat[rf_att_pred_rocset_5cat$inv_distance==max(rf_att_pred_rocset_5cat$inv_distance),ncol(rf_att_pred_rocset_5cat)] + 
            	rf_emtxt_pred_rocset_5cat[rf_emtxt_pred_rocset_5cat$inv_distance==max(rf_emtxt_pred_rocset_5cat$inv_distance),ncol(rf_emtxt_pred_rocset_5cat)] + 
            	rf_doctxt_pred_rocset_5cat[rf_doctxt_pred_rocset_5cat$inv_distance==max(rf_doctxt_pred_rocset_5cat$inv_distance),ncol(rf_doctxt_pred_rocset_5cat)] + 
				rf_metacombo_pred_rocset_5cat[rf_metacombo_pred_rocset_5cat$inv_distance==max(rf_metacombo_pred_rocset_5cat$inv_distance),ncol(rf_metacombo_pred_rocset_5cat)] +
				rf_txtcombo_pred_rocset_5cat[rf_txtcombo_pred_rocset_5cat$inv_distance==max(rf_txtcombo_pred_rocset_5cat$inv_distance),ncol(rf_txtcombo_pred_rocset_5cat)] +
				rf_fullcombo_pred_rocset_5cat[rf_fullcombo_pred_rocset_5cat$inv_distance==max(rf_fullcombo_pred_rocset_5cat$inv_distance),ncol(rf_fullcombo_pred_rocset_5cat)]

tot_auc_7cat <- rf_base_pred_rocset_7cat[rf_base_pred_rocset_7cat$inv_distance==max(rf_base_pred_rocset_7cat$inv_distance),ncol(rf_base_pred_rocset_7cat)] + 
            	rf_sub_pred_rocset_7cat[rf_sub_pred_rocset_7cat$inv_distance==max(rf_sub_pred_rocset_7cat$inv_distance),ncol(rf_sub_pred_rocset_7cat)] + 
           		rf_att_pred_rocset_7cat[rf_att_pred_rocset_7cat$inv_distance==max(rf_att_pred_rocset_7cat$inv_distance),ncol(rf_att_pred_rocset_7cat)] + 
            	rf_emtxt_pred_rocset_7cat[rf_emtxt_pred_rocset_7cat$inv_distance==max(rf_emtxt_pred_rocset_7cat$inv_distance),ncol(rf_emtxt_pred_rocset_7cat)] + 
            	rf_doctxt_pred_rocset_7cat[rf_doctxt_pred_rocset_7cat$inv_distance==max(rf_doctxt_pred_rocset_7cat$inv_distance),ncol(rf_doctxt_pred_rocset_7cat)] + 
				rf_metacombo_pred_rocset_7cat[rf_metacombo_pred_rocset_7cat$inv_distance==max(rf_metacombo_pred_rocset_7cat$inv_distance),ncol(rf_metacombo_pred_rocset_7cat)] +
				rf_txtcombo_pred_rocset_7cat[rf_txtcombo_pred_rocset_7cat$inv_distance==max(rf_txtcombo_pred_rocset_7cat$inv_distance),ncol(rf_txtcombo_pred_rocset_7cat)] +
				rf_fullcombo_pred_rocset_7cat[rf_fullcombo_pred_rocset_7cat$inv_distance==max(rf_fullcombo_pred_rocset_7cat$inv_distance),ncol(rf_fullcombo_pred_rocset_7cat)]

tot_auc <- (tot_auc_2cat + tot_auc_4cat + tot_auc_5cat + tot_auc_7cat)

rf_base_2cat_auc_wt <- (rf_base_pred_rocset_2cat[1, ncol(rf_base_pred_rocset_2cat)]/tot_auc)
rf_sub_2cat_auc_wt <- (rf_sub_pred_rocset_2cat[1, ncol(rf_sub_pred_rocset_2cat)]/tot_auc)
rf_emtxt_2cat_auc_wt <- (rf_emtxt_pred_rocset_2cat[1, ncol(rf_emtxt_pred_rocset_2cat)]/tot_auc)
rf_doctxt_2cat_auc_wt <- (rf_doctxt_pred_rocset_2cat[1, ncol(rf_doctxt_pred_rocset_2cat)]/tot_auc)
rf_metacombo_2cat_auc_wt <- (rf_metacombo_pred_rocset_2cat[1, ncol(rf_metacombo_pred_rocset_2cat)]/tot_auc)
rf_txtcombo_2cat_auc_wt <- (rf_txtcombo_pred_rocset_2cat[1, ncol(rf_txtcombo_pred_rocset_2cat)]/tot_auc)
rf_fullcombo_2cat_auc_wt <- (rf_fullcombo_pred_rocset_2cat[1, ncol(rf_fullcombo_pred_rocset_2cat)]/tot_auc)

rf_base_4cat_auc_wt <- (rf_base_pred_rocset_4cat[1, ncol(rf_base_pred_rocset_4cat)]/tot_auc)
rf_sub_4cat_auc_wt <- (rf_sub_pred_rocset_4cat[1, ncol(rf_sub_pred_rocset_4cat)]/tot_auc)
rf_att_4cat_auc_wt <- (rf_att_pred_rocset_4cat[1, ncol(rf_att_pred_rocset_4cat)]/tot_auc)
rf_emtxt_4cat_auc_wt <- (rf_emtxt_pred_rocset_4cat[1, ncol(rf_emtxt_pred_rocset_4cat)]/tot_auc)
rf_doctxt_4cat_auc_wt <- (rf_doctxt_pred_rocset_4cat[1, ncol(rf_doctxt_pred_rocset_4cat)]/tot_auc)
rf_metacombo_4cat_auc_wt <- (rf_metacombo_pred_rocset_4cat[1, ncol(rf_metacombo_pred_rocset_4cat)]/tot_auc)
rf_txtcombo_4cat_auc_wt <- (rf_txtcombo_pred_rocset_4cat[1, ncol(rf_txtcombo_pred_rocset_4cat)]/tot_auc)
rf_fullcombo_4cat_auc_wt <- (rf_fullcombo_pred_rocset_4cat[1, ncol(rf_fullcombo_pred_rocset_4cat)]/tot_auc)

rf_base_5cat_auc_wt <- (rf_base_pred_rocset_5cat[1, ncol(rf_base_pred_rocset_5cat)]/tot_auc)
rf_sub_5cat_auc_wt <- (rf_sub_pred_rocset_5cat[1, ncol(rf_sub_pred_rocset_5cat)]/tot_auc)
rf_att_5cat_auc_wt <- (rf_att_pred_rocset_5cat[1, ncol(rf_att_pred_rocset_5cat)]/tot_auc)
rf_emtxt_5cat_auc_wt <- (rf_emtxt_pred_rocset_5cat[1, ncol(rf_emtxt_pred_rocset_5cat)]/tot_auc)
rf_doctxt_5cat_auc_wt <- (rf_doctxt_pred_rocset_5cat[1, ncol(rf_doctxt_pred_rocset_5cat)]/tot_auc)
rf_metacombo_5cat_auc_wt <- (rf_metacombo_pred_rocset_5cat[1, ncol(rf_metacombo_pred_rocset_5cat)]/tot_auc)
rf_txtcombo_5cat_auc_wt <- (rf_txtcombo_pred_rocset_5cat[1, ncol(rf_txtcombo_pred_rocset_5cat)]/tot_auc)
rf_fullcombo_5cat_auc_wt <- (rf_fullcombo_pred_rocset_5cat[1, ncol(rf_fullcombo_pred_rocset_5cat)]/tot_auc)

rf_base_7cat_auc_wt <- (rf_base_pred_rocset_7cat[1, ncol(rf_base_pred_rocset_7cat)]/tot_auc)
rf_sub_7cat_auc_wt <- (rf_sub_pred_rocset_7cat[1, ncol(rf_sub_pred_rocset_7cat)]/tot_auc)
rf_att_7cat_auc_wt <- (rf_att_pred_rocset_7cat[1, ncol(rf_att_pred_rocset_7cat)]/tot_auc)
rf_emtxt_7cat_auc_wt <- (rf_emtxt_pred_rocset_7cat[1, ncol(rf_emtxt_pred_rocset_7cat)]/tot_auc)
rf_doctxt_7cat_auc_wt <- (rf_doctxt_pred_rocset_7cat[1, ncol(rf_doctxt_pred_rocset_7cat)]/tot_auc)
rf_metacombo_7cat_auc_wt <- (rf_metacombo_pred_rocset_7cat[1, ncol(rf_metacombo_pred_rocset_7cat)]/tot_auc)
rf_txtcombo_7cat_auc_wt <- (rf_txtcombo_pred_rocset_7cat[1, ncol(rf_txtcombo_pred_rocset_7cat)]/tot_auc)
rf_fullcombo_7cat_auc_wt <- (rf_fullcombo_pred_rocset_7cat[1, ncol(rf_fullcombo_pred_rocset_7cat)]/tot_auc)

combined_prob_confirmed <- (rf_base_pred[,5] * rf_base_2cat_auc_wt) + 
                            (rf_sub_pred[,5] * rf_sub_2cat_auc_wt) +
                           #(rf_att_pred[,5] * rf_att_2cat_auc_wt) +
                            (rf_emtxt_pred[,5] * rf_emtxt_2cat_auc_wt) +
                            (rf_doctxt_pred[,5] * rf_doctxt_2cat_auc_wt) + 
                            (rf_metacombo_pred[,5] * rf_metacombo_2cat_auc_wt) + 
							(rf_txtcombo_pred[,5] * rf_txtcombo_2cat_auc_wt) + 
							(rf_fullcombo_pred[,5] * rf_fullcombo_2cat_auc_wt) + 
							
							(rf_base_pred[,6] * rf_base_4cat_auc_wt) + 
                            (rf_sub_pred[,6] * rf_sub_4cat_auc_wt) +
                            (rf_att_pred[,6] * rf_att_4cat_auc_wt) +
                            (rf_emtxt_pred[,6] * rf_emtxt_4cat_auc_wt) +
                            (rf_doctxt_pred[,6] * rf_doctxt_4cat_auc_wt) + 
                            (rf_metacombo_pred[,6] * rf_metacombo_4cat_auc_wt) + 
							(rf_txtcombo_pred[,6] * rf_txtcombo_4cat_auc_wt) + 
							(rf_fullcombo_pred[,6] * rf_fullcombo_4cat_auc_wt) + 
							
							(rf_base_pred[,7] * rf_base_5cat_auc_wt) + 
                            (rf_sub_pred[,7] * rf_sub_5cat_auc_wt) +
                            (rf_att_pred[,7] * rf_att_5cat_auc_wt) +
                            (rf_emtxt_pred[,7] * rf_emtxt_5cat_auc_wt) +
                            (rf_doctxt_pred[,7] * rf_doctxt_5cat_auc_wt) + 
                            (rf_metacombo_pred[,7] * rf_metacombo_5cat_auc_wt) + 
							(rf_txtcombo_pred[,7] * rf_txtcombo_5cat_auc_wt) + 
							(rf_fullcombo_pred[,7] * rf_fullcombo_5cat_auc_wt) + 
							
							(rf_base_pred[,8] * rf_base_7cat_auc_wt) + 
                            (rf_sub_pred[,8] * rf_sub_7cat_auc_wt) +
                            (rf_att_pred[,8] * rf_att_7cat_auc_wt) +
                            (rf_emtxt_pred[,8] * rf_emtxt_7cat_auc_wt) +
                            (rf_doctxt_pred[,8] * rf_doctxt_7cat_auc_wt) + 
                            (rf_metacombo_pred[,8] * rf_metacombo_7cat_auc_wt) + 
							(rf_txtcombo_pred[,8] * rf_txtcombo_7cat_auc_wt) + 
							(rf_fullcombo_pred[,8] * rf_fullcombo_7cat_auc_wt)

rf_combo <- cbind.data.frame(data[train & incremental,'tx_binary'], 
              combined_prob_confirmed)
colnames(rf_combo) <- c('tx_binary','prob_con')
rf_combo_rocset <- thresh_iter(.0, 1, .01, rf_combo, 'prob_con', 'tx_binary')
rf_combo_rocset[rf_combo_rocset$inv_distance==max(rf_combo_rocset$inv_distance),]

#Save Image
save.image('FalsePositives.Rdata')

#Apply to Test Set
rf_base_pred_test_2cat <- predict(rf_base_2cat, cbind(as.factor(data[test & incremental, 'tx_binary']), base_rhs[test & incremental,]), type = 'prob')
rf_base_pred_test_4cat <- predict(rf_base_4cat, cbind(as.factor(data[test & incremental, 'tx_4cat']), base_rhs[test & incremental,]), type = 'prob')
rf_base_pred_test_5cat <- predict(rf_base_5cat, cbind(as.factor(data[test & incremental, 'tx_5cat']), base_rhs[test & incremental,]), type = 'prob')
rf_base_pred_test_7cat <- predict(rf_base_7cat, cbind(as.factor(data[test & incremental, 'tx_7cat']), base_rhs[test & incremental,]), type = 'prob')

rf_base_pred_test <- cbind.data.frame(data[test & incremental, 'tx_binary'], 
              data[test & incremental, 'tx_4cat'], 
              data[test & incremental, 'tx_5cat'],
              data[test & incremental, 'tx_7cat'],               
              rf_base_pred_test_2cat[,ncol(rf_base_pred_test_2cat)],
              rowSums(rf_base_pred_test_4cat[,c(2:ncol(rf_base_pred_test_4cat))]),
              rowSums(rf_base_pred_test_5cat[,c(2:ncol(rf_base_pred_test_5cat))]),
              rowSums(rf_base_pred_test_7cat[,c(4:ncol(rf_base_pred_test_7cat))])
              )

colnames(rf_base_pred_test) <- c('tx_binary', 'tx_4cat', 'tx_5cat', 'tx_7cat',  
                            'prob_con2', 'prob_con4', 'prob_con5', 'prob_con7')

rf_base_pred_test_rocset_2cat <- thresh_iter(.0, 1, .01, rf_base_pred_test, 'prob_con2', 'tx_binary')
rf_base_pred_test_rocset_2cat[rf_base_pred_test_rocset_2cat$inv_distance==max(rf_base_pred_test_rocset_2cat$inv_distance),]
rf_base_pred_test_rocset_4cat <- thresh_iter(.0, 1, .01, rf_base_pred_test, 'prob_con4', 'tx_binary')
rf_base_pred_test_rocset_4cat[rf_base_pred_test_rocset_4cat$inv_distance==max(rf_base_pred_test_rocset_4cat$inv_distance),]
rf_base_pred_test_rocset_5cat <- thresh_iter(.0, 1, .01, rf_base_pred_test, 'prob_con5', 'tx_binary')
rf_base_pred_test_rocset_5cat[rf_base_pred_test_rocset_5cat$inv_distance==max(rf_base_pred_test_rocset_5cat$inv_distance),]
rf_base_pred_test_rocset_7cat <- thresh_iter(.0, 1, .01, rf_base_pred_test, 'prob_con7', 'tx_binary')
rf_base_pred_test_rocset_7cat[rf_base_pred_test_rocset_7cat$inv_distance==max(rf_base_pred_test_rocset_7cat$inv_distance),]

cor(rf_base_pred_test[, grepl('prob', colnames(rf_base_pred_test))])

rf_sub_pred_test_2cat <- predict(rf_sub_2cat, cbind(as.factor(data[test & incremental, 'tx_binary']), subject_rhs[test & incremental,]), type = 'prob')
rf_sub_pred_test_4cat <- predict(rf_sub_4cat, cbind(as.factor(data[test & incremental, 'tx_4cat']), subject_rhs[test & incremental,]), type = 'prob')
rf_sub_pred_test_5cat <- predict(rf_sub_5cat, cbind(as.factor(data[test & incremental, 'tx_5cat']), subject_rhs[test & incremental,]), type = 'prob')
rf_sub_pred_test_7cat <- predict(rf_sub_7cat, cbind(as.factor(data[test & incremental, 'tx_7cat']), subject_rhs[test & incremental,]), type = 'prob')

rf_sub_pred_test <- cbind.data.frame(data[test & incremental, 'tx_binary'], 
              data[test & incremental, 'tx_4cat'], 
              data[test & incremental, 'tx_5cat'],
              data[test & incremental, 'tx_7cat'],               
              rf_sub_pred_test_2cat[,ncol(rf_sub_pred_test_2cat)],
              rowSums(rf_sub_pred_test_4cat[,c(2:ncol(rf_sub_pred_test_4cat))]),
              rowSums(rf_sub_pred_test_5cat[,c(2:ncol(rf_sub_pred_test_5cat))]),
              rowSums(rf_sub_pred_test_7cat[,c(4:ncol(rf_sub_pred_test_7cat))])
              )

colnames(rf_sub_pred_test) <- c('tx_binary', 'tx_4cat', 'tx_5cat', 'tx_7cat',  
                            'prob_con2', 'prob_con4', 'prob_con5', 'prob_con7')

rf_sub_pred_test_rocset_2cat <- thresh_iter(.0, 1, .01, rf_sub_pred_test, 'prob_con2', 'tx_binary')
rf_sub_pred_test_rocset_2cat[rf_sub_pred_test_rocset_2cat$inv_distance==max(rf_sub_pred_test_rocset_2cat$inv_distance),]
rf_sub_pred_test_rocset_4cat <- thresh_iter(.0, 1, .01, rf_sub_pred_test, 'prob_con4', 'tx_binary')
rf_sub_pred_test_rocset_4cat[rf_sub_pred_test_rocset_4cat$inv_distance==max(rf_sub_pred_test_rocset_4cat$inv_distance),]
rf_sub_pred_test_rocset_5cat <- thresh_iter(.0, 1, .01, rf_sub_pred_test, 'prob_con5', 'tx_binary')
rf_sub_pred_test_rocset_5cat[rf_sub_pred_test_rocset_5cat$inv_distance==max(rf_sub_pred_test_rocset_5cat$inv_distance),]
rf_sub_pred_test_rocset_7cat <- thresh_iter(.0, 1, .01, rf_sub_pred_test, 'prob_con7', 'tx_binary')
rf_sub_pred_test_rocset_7cat[rf_sub_pred_test_rocset_7cat$inv_distance==max(rf_sub_pred_test_rocset_7cat$inv_distance),]

cor(rf_sub_pred_test[, grepl('prob', colnames(rf_sub_pred_test))])

rf_att_pred_test_2cat <- predict(rf_att_2cat, cbind(as.factor(data[test & incremental, 'tx_binary']), attname_rhs[test & incremental,]), type = 'prob')
rf_att_pred_test_4cat <- predict(rf_att_4cat, cbind(as.factor(data[test & incremental, 'tx_4cat']), attname_rhs[test & incremental,]), type = 'prob')
rf_att_pred_test_5cat <- predict(rf_att_5cat, cbind(as.factor(data[test & incremental, 'tx_5cat']), attname_rhs[test & incremental,]), type = 'prob')
rf_att_pred_test_7cat <- predict(rf_att_7cat, cbind(as.factor(data[test & incremental, 'tx_7cat']), attname_rhs[test & incremental,]), type = 'prob')

rf_att_pred_test <- cbind.data.frame(data[test & incremental, 'tx_binary'], 
              data[test & incremental, 'tx_4cat'], 
              data[test & incremental, 'tx_5cat'],
              data[test & incremental, 'tx_7cat'],               
              rf_att_pred_test_2cat[,ncol(rf_att_pred_test_2cat)],
              rowSums(rf_att_pred_test_4cat[,c(2:ncol(rf_att_pred_test_4cat))]),
              rowSums(rf_att_pred_test_5cat[,c(2:ncol(rf_att_pred_test_5cat))]),
              rowSums(rf_att_pred_test_7cat[,c(4:ncol(rf_att_pred_test_7cat))])
              )

colnames(rf_att_pred_test) <- c('tx_binary', 'tx_4cat', 'tx_5cat', 'tx_7cat',  
                            'prob_con2', 'prob_con4', 'prob_con5', 'prob_con7')

rf_att_pred_test_rocset_2cat <- thresh_iter(.0, 1, .01, rf_att_pred_test, 'prob_con2', 'tx_binary')
rf_att_pred_test_rocset_2cat[rf_att_pred_test_rocset_2cat$inv_distance==max(rf_att_pred_test_rocset_2cat$inv_distance),]
rf_att_pred_test_rocset_4cat <- thresh_iter(.0, 1, .01, rf_att_pred_test, 'prob_con4', 'tx_binary')
rf_att_pred_test_rocset_4cat[rf_att_pred_test_rocset_4cat$inv_distance==max(rf_att_pred_test_rocset_4cat$inv_distance),]
rf_att_pred_test_rocset_5cat <- thresh_iter(.0, 1, .01, rf_att_pred_test, 'prob_con5', 'tx_binary')
rf_att_pred_test_rocset_5cat[rf_att_pred_test_rocset_5cat$inv_distance==max(rf_att_pred_test_rocset_5cat$inv_distance),]
rf_att_pred_test_rocset_7cat <- thresh_iter(.0, 1, .01, rf_att_pred_test, 'prob_con7', 'tx_binary')
rf_att_pred_test_rocset_7cat[rf_att_pred_test_rocset_7cat$inv_distance==max(rf_att_pred_test_rocset_7cat$inv_distance),]

cor(rf_att_pred_test[, grepl('prob', colnames(rf_att_pred_test))])

rf_emtxt_pred_test_2cat <- predict(rf_emtxt_2cat, cbind(as.factor(data[test & incremental, 'tx_binary']), emailtxt_rhs_pca[test & incremental,]), type = 'prob')
rf_emtxt_pred_test_4cat <- predict(rf_emtxt_4cat, cbind(as.factor(data[test & incremental, 'tx_4cat']), emailtxt_rhs_pca[test & incremental,]), type = 'prob')
rf_emtxt_pred_test_5cat <- predict(rf_emtxt_5cat, cbind(as.factor(data[test & incremental, 'tx_5cat']), emailtxt_rhs_pca[test & incremental,]), type = 'prob')
rf_emtxt_pred_test_7cat <- predict(rf_emtxt_7cat, cbind(as.factor(data[test & incremental, 'tx_7cat']), emailtxt_rhs_pca[test & incremental,]), type = 'prob')

rf_emtxt_pred_test <- cbind.data.frame(data[test & incremental, 'tx_binary'], 
              data[test & incremental, 'tx_4cat'], 
              data[test & incremental, 'tx_5cat'],
              data[test & incremental, 'tx_7cat'],               
              rf_emtxt_pred_test_2cat[,ncol(rf_emtxt_pred_test_2cat)],
              rowSums(rf_emtxt_pred_test_4cat[,c(2:ncol(rf_emtxt_pred_test_4cat))]),
              rowSums(rf_emtxt_pred_test_5cat[,c(2:ncol(rf_emtxt_pred_test_5cat))]),
              rowSums(rf_emtxt_pred_test_7cat[,c(4:ncol(rf_emtxt_pred_test_7cat))])
              )

colnames(rf_emtxt_pred_test) <- c('tx_binary', 'tx_4cat', 'tx_5cat', 'tx_7cat',  
                            'prob_con2', 'prob_con4', 'prob_con5', 'prob_con7')

rf_emtxt_pred_test_rocset_2cat <- thresh_iter(.0, 1, .01, rf_emtxt_pred_test, 'prob_con2', 'tx_binary')
rf_emtxt_pred_test_rocset_2cat[rf_emtxt_pred_test_rocset_2cat$inv_distance==max(rf_emtxt_pred_test_rocset_2cat$inv_distance),]
rf_emtxt_pred_test_rocset_4cat <- thresh_iter(.0, 1, .01, rf_emtxt_pred_test, 'prob_con4', 'tx_binary')
rf_emtxt_pred_test_rocset_4cat[rf_emtxt_pred_test_rocset_4cat$inv_distance==max(rf_emtxt_pred_test_rocset_4cat$inv_distance),]
rf_emtxt_pred_test_rocset_5cat <- thresh_iter(.0, 1, .01, rf_emtxt_pred_test, 'prob_con5', 'tx_binary')
rf_emtxt_pred_test_rocset_5cat[rf_emtxt_pred_test_rocset_5cat$inv_distance==max(rf_emtxt_pred_test_rocset_5cat$inv_distance),]
rf_emtxt_pred_test_rocset_7cat <- thresh_iter(.0, 1, .01, rf_emtxt_pred_test, 'prob_con7', 'tx_binary')
rf_emtxt_pred_test_rocset_7cat[rf_emtxt_pred_test_rocset_7cat$inv_distance==max(rf_emtxt_pred_test_rocset_7cat$inv_distance),]

cor(rf_emtxt_pred_test[, grepl('prob', colnames(rf_emtxt_pred_test))])

rf_doctxt_pred_test_2cat <- predict(rf_doctxt_2cat, cbind(as.factor(data[test & incremental, 'tx_binary']), doctxt_rhs_pca[test & incremental,]), type = 'prob')
rf_doctxt_pred_test_4cat <- predict(rf_doctxt_4cat, cbind(as.factor(data[test & incremental, 'tx_4cat']), doctxt_rhs_pca[test & incremental,]), type = 'prob')
rf_doctxt_pred_test_5cat <- predict(rf_doctxt_5cat, cbind(as.factor(data[test & incremental, 'tx_5cat']), doctxt_rhs_pca[test & incremental,]), type = 'prob')
rf_doctxt_pred_test_7cat <- predict(rf_doctxt_7cat, cbind(as.factor(data[test & incremental, 'tx_7cat']), doctxt_rhs_pca[test & incremental,]), type = 'prob')

rf_doctxt_pred_test <- cbind.data.frame(data[test & incremental, 'tx_binary'], 
              data[test & incremental, 'tx_4cat'], 
              data[test & incremental, 'tx_5cat'],
              data[test & incremental, 'tx_7cat'],               
              rf_doctxt_pred_test_2cat[,ncol(rf_doctxt_pred_test_2cat)],
              rowSums(rf_doctxt_pred_test_4cat[,c(2:ncol(rf_doctxt_pred_test_4cat))]),
              rowSums(rf_doctxt_pred_test_5cat[,c(2:ncol(rf_doctxt_pred_test_5cat))]),
              rowSums(rf_doctxt_pred_test_7cat[,c(4:ncol(rf_doctxt_pred_test_7cat))])
              )

colnames(rf_doctxt_pred_test) <- c('tx_binary', 'tx_4cat', 'tx_5cat', 'tx_7cat',  
                            'prob_con2', 'prob_con4', 'prob_con5', 'prob_con7')

rf_doctxt_pred_test_rocset_2cat <- thresh_iter(.0, 1, .01, rf_doctxt_pred_test, 'prob_con2', 'tx_binary')
rf_doctxt_pred_test_rocset_2cat[rf_doctxt_pred_test_rocset_2cat$inv_distance==max(rf_doctxt_pred_test_rocset_2cat$inv_distance),]
rf_doctxt_pred_test_rocset_4cat <- thresh_iter(.0, 1, .01, rf_doctxt_pred_test, 'prob_con4', 'tx_binary')
rf_doctxt_pred_test_rocset_4cat[rf_doctxt_pred_test_rocset_4cat$inv_distance==max(rf_doctxt_pred_test_rocset_4cat$inv_distance),]
rf_doctxt_pred_test_rocset_5cat <- thresh_iter(.0, 1, .01, rf_doctxt_pred_test, 'prob_con5', 'tx_binary')
rf_doctxt_pred_test_rocset_5cat[rf_doctxt_pred_test_rocset_5cat$inv_distance==max(rf_doctxt_pred_test_rocset_5cat$inv_distance),]
rf_doctxt_pred_test_rocset_7cat <- thresh_iter(.0, 1, .01, rf_doctxt_pred_test, 'prob_con7', 'tx_binary')
rf_doctxt_pred_test_rocset_7cat[rf_doctxt_pred_test_rocset_7cat$inv_distance==max(rf_doctxt_pred_test_rocset_7cat$inv_distance),]

cor(rf_doctxt_pred_test[, grepl('prob', colnames(rf_doctxt_pred_test))])

rf_metacombo_pred_test_2cat <- predict(rf_metacombo_2cat, cbind(as.factor(data[test & incremental, 'tx_binary']), base_rhs[test & incremental,], subject_rhs[test & incremental,], attname_rhs[test & incremental,]), type = 'prob')
rf_metacombo_pred_test_4cat <- predict(rf_metacombo_4cat, cbind(as.factor(data[test & incremental, 'tx_binary']), base_rhs[test & incremental,], subject_rhs[test & incremental,], attname_rhs[test & incremental,]), type = 'prob')
rf_metacombo_pred_test_5cat <- predict(rf_metacombo_5cat, cbind(as.factor(data[test & incremental, 'tx_binary']), base_rhs[test & incremental,], subject_rhs[test & incremental,], attname_rhs[test & incremental,]), type = 'prob')
rf_metacombo_pred_test_7cat <- predict(rf_metacombo_7cat, cbind(as.factor(data[test & incremental, 'tx_binary']), base_rhs[test & incremental,], subject_rhs[test & incremental,], attname_rhs[test & incremental,]), type = 'prob')

rf_metacombo_pred_test <- cbind.data.frame(data[test & incremental, 'tx_binary'], 
              data[test & incremental, 'tx_4cat'], 
              data[test & incremental, 'tx_5cat'],
              data[test & incremental, 'tx_7cat'],               
              rf_metacombo_pred_test_2cat[,ncol(rf_metacombo_pred_test_2cat)],
              rowSums(rf_metacombo_pred_test_4cat[,c(2:ncol(rf_metacombo_pred_test_4cat))]),
              rowSums(rf_metacombo_pred_test_5cat[,c(2:ncol(rf_metacombo_pred_test_5cat))]),
              rowSums(rf_metacombo_pred_test_7cat[,c(4:ncol(rf_metacombo_pred_test_7cat))])
              )

colnames(rf_metacombo_pred_test) <- c('tx_binary', 'tx_4cat', 'tx_5cat', 'tx_7cat',  
                            'prob_con2', 'prob_con4', 'prob_con5', 'prob_con7')

rf_metacombo_pred_test_rocset_2cat <- thresh_iter(.0, 1, .01, rf_metacombo_pred_test, 'prob_con2', 'tx_binary')
rf_metacombo_pred_test_rocset_2cat[rf_metacombo_pred_test_rocset_2cat$inv_distance==max(rf_metacombo_pred_test_rocset_2cat$inv_distance),]
rf_metacombo_pred_test_rocset_4cat <- thresh_iter(.0, 1, .01, rf_metacombo_pred_test, 'prob_con4', 'tx_binary')
rf_metacombo_pred_test_rocset_4cat[rf_metacombo_pred_test_rocset_4cat$inv_distance==max(rf_metacombo_pred_test_rocset_4cat$inv_distance),]
rf_metacombo_pred_test_rocset_5cat <- thresh_iter(.0, 1, .01, rf_metacombo_pred_test, 'prob_con5', 'tx_binary')
rf_metacombo_pred_test_rocset_5cat[rf_metacombo_pred_test_rocset_5cat$inv_distance==max(rf_metacombo_pred_test_rocset_5cat$inv_distance),]
rf_metacombo_pred_test_rocset_7cat <- thresh_iter(.0, 1, .01, rf_metacombo_pred_test, 'prob_con7', 'tx_binary')
rf_metacombo_pred_test_rocset_7cat[rf_metacombo_pred_test_rocset_7cat$inv_distance==max(rf_metacombo_pred_test_rocset_7cat$inv_distance),]

cor(rf_metacombo_pred_test[, grepl('prob', colnames(rf_metacombo_pred_test))])

rf_txtcombo_pred_test_2cat <- predict(rf_txtcombo_2cat, cbind(as.factor(data[test & incremental, 'tx_binary']), emailtxt_rhs_pca[test & incremental,], doctxt_rhs_pca[test & incremental,]), type = 'prob')
rf_txtcombo_pred_test_4cat <- predict(rf_txtcombo_4cat, cbind(as.factor(data[test & incremental, 'tx_binary']), emailtxt_rhs_pca[test & incremental,], doctxt_rhs_pca[test & incremental,]), type = 'prob')
rf_txtcombo_pred_test_5cat <- predict(rf_txtcombo_5cat, cbind(as.factor(data[test & incremental, 'tx_binary']), emailtxt_rhs_pca[test & incremental,], doctxt_rhs_pca[test & incremental,]), type = 'prob')
rf_txtcombo_pred_test_7cat <- predict(rf_txtcombo_7cat, cbind(as.factor(data[test & incremental, 'tx_binary']), emailtxt_rhs_pca[test & incremental,], doctxt_rhs_pca[test & incremental,]), type = 'prob')

rf_txtcombo_pred_test <- cbind.data.frame(data[test & incremental, 'tx_binary'], 
              data[test & incremental, 'tx_4cat'], 
              data[test & incremental, 'tx_5cat'],
              data[test & incremental, 'tx_7cat'],               
              rf_txtcombo_pred_test_2cat[,ncol(rf_txtcombo_pred_test_2cat)],
              rowSums(rf_txtcombo_pred_test_4cat[,c(2:ncol(rf_txtcombo_pred_test_4cat))]),
              rowSums(rf_txtcombo_pred_test_5cat[,c(2:ncol(rf_txtcombo_pred_test_5cat))]),
              rowSums(rf_txtcombo_pred_test_7cat[,c(4:ncol(rf_txtcombo_pred_test_7cat))])
              )

colnames(rf_txtcombo_pred_test) <- c('tx_binary', 'tx_4cat', 'tx_5cat', 'tx_7cat',  
                            'prob_con2', 'prob_con4', 'prob_con5', 'prob_con7')

rf_txtcombo_pred_test_rocset_2cat <- thresh_iter(.0, 1, .01, rf_txtcombo_pred_test, 'prob_con2', 'tx_binary')
rf_txtcombo_pred_test_rocset_2cat[rf_txtcombo_pred_test_rocset_2cat$inv_distance==max(rf_txtcombo_pred_test_rocset_2cat$inv_distance),]
rf_txtcombo_pred_test_rocset_4cat <- thresh_iter(.0, 1, .01, rf_txtcombo_pred_test, 'prob_con4', 'tx_binary')
rf_txtcombo_pred_test_rocset_4cat[rf_txtcombo_pred_test_rocset_4cat$inv_distance==max(rf_txtcombo_pred_test_rocset_4cat$inv_distance),]
rf_txtcombo_pred_test_rocset_5cat <- thresh_iter(.0, 1, .01, rf_txtcombo_pred_test, 'prob_con5', 'tx_binary')
rf_txtcombo_pred_test_rocset_5cat[rf_txtcombo_pred_test_rocset_5cat$inv_distance==max(rf_txtcombo_pred_test_rocset_5cat$inv_distance),]
rf_txtcombo_pred_test_rocset_7cat <- thresh_iter(.0, 1, .01, rf_txtcombo_pred_test, 'prob_con7', 'tx_binary')
rf_txtcombo_pred_test_rocset_7cat[rf_txtcombo_pred_test_rocset_7cat$inv_distance==max(rf_txtcombo_pred_test_rocset_7cat$inv_distance),]

cor(rf_txtcombo_pred_test[, grepl('prob', colnames(rf_txtcombo_pred_test))])

rf_fullcombo_pred_test_2cat <- predict(rf_fullcombo_2cat, cbind(as.factor(data[test & incremental, 'tx_binary']), base_rhs[test & incremental,], subject_rhs[test & incremental,], attname_rhs[test & incremental,], emailtxt_rhs_pca[test & incremental,], doctxt_rhs_pca[test & incremental,]), type = 'prob')
rf_fullcombo_pred_test_4cat <- predict(rf_fullcombo_4cat, cbind(as.factor(data[test & incremental, 'tx_binary']), base_rhs[test & incremental,], subject_rhs[test & incremental,], attname_rhs[test & incremental,], emailtxt_rhs_pca[test & incremental,], doctxt_rhs_pca[test & incremental,]), type = 'prob')
rf_fullcombo_pred_test_5cat <- predict(rf_fullcombo_5cat, cbind(as.factor(data[test & incremental, 'tx_binary']), base_rhs[test & incremental,], subject_rhs[test & incremental,], attname_rhs[test & incremental,], emailtxt_rhs_pca[test & incremental,], doctxt_rhs_pca[test & incremental,]), type = 'prob')
rf_fullcombo_pred_test_7cat <- predict(rf_fullcombo_7cat, cbind(as.factor(data[test & incremental, 'tx_binary']), base_rhs[test & incremental,], subject_rhs[test & incremental,], attname_rhs[test & incremental,], emailtxt_rhs_pca[test & incremental,], doctxt_rhs_pca[test & incremental,]), type = 'prob')

rf_fullcombo_pred_test <- cbind.data.frame(data[test & incremental, 'tx_binary'], 
              data[test & incremental, 'tx_4cat'], 
              data[test & incremental, 'tx_5cat'],
              data[test & incremental, 'tx_7cat'],               
              rf_fullcombo_pred_test_2cat[,ncol(rf_fullcombo_pred_test_2cat)],
              rowSums(rf_fullcombo_pred_test_4cat[,c(2:ncol(rf_fullcombo_pred_test_4cat))]),
              rowSums(rf_fullcombo_pred_test_5cat[,c(2:ncol(rf_fullcombo_pred_test_5cat))]),
              rowSums(rf_fullcombo_pred_test_7cat[,c(4:ncol(rf_fullcombo_pred_test_7cat))])
              )

colnames(rf_fullcombo_pred_test) <- c('tx_binary', 'tx_4cat', 'tx_5cat', 'tx_7cat',  
                            'prob_con2', 'prob_con4', 'prob_con5', 'prob_con7')

rf_fullcombo_pred_test_rocset_2cat <- thresh_iter(.0, 1, .01, rf_fullcombo_pred_test, 'prob_con2', 'tx_binary')
rf_fullcombo_pred_test_rocset_2cat[rf_fullcombo_pred_test_rocset_2cat$inv_distance==max(rf_fullcombo_pred_test_rocset_2cat$inv_distance),]
rf_fullcombo_pred_test_rocset_4cat <- thresh_iter(.0, 1, .01, rf_fullcombo_pred_test, 'prob_con4', 'tx_binary')
rf_fullcombo_pred_test_rocset_4cat[rf_fullcombo_pred_test_rocset_4cat$inv_distance==max(rf_fullcombo_pred_test_rocset_4cat$inv_distance),]
rf_fullcombo_pred_test_rocset_5cat <- thresh_iter(.0, 1, .01, rf_fullcombo_pred_test, 'prob_con5', 'tx_binary')
rf_fullcombo_pred_test_rocset_5cat[rf_fullcombo_pred_test_rocset_5cat$inv_distance==max(rf_fullcombo_pred_test_rocset_5cat$inv_distance),]
rf_fullcombo_pred_test_rocset_7cat <- thresh_iter(.0, 1, .01, rf_fullcombo_pred_test, 'prob_con7', 'tx_binary')
rf_fullcombo_pred_test_rocset_7cat[rf_fullcombo_pred_test_rocset_7cat$inv_distance==max(rf_fullcombo_pred_test_rocset_7cat$inv_distance),]

cor(rf_fullcombo_pred_test[, grepl('prob', colnames(rf_fullcombo_pred_test))])

combined_prob_test_confirmed <- (rf_base_pred_test[,5] * rf_base_2cat_auc_wt) + 
                            (rf_sub_pred_test[,5] * rf_sub_2cat_auc_wt) +
                           #(rf_att_pred_test[,5] * rf_att_2cat_auc_wt) +
                            (rf_emtxt_pred_test[,5] * rf_emtxt_2cat_auc_wt) +
                            (rf_doctxt_pred_test[,5] * rf_doctxt_2cat_auc_wt) + 
                            (rf_metacombo_pred_test[,5] * rf_metacombo_2cat_auc_wt) + 
							(rf_txtcombo_pred_test[,5] * rf_txtcombo_2cat_auc_wt) + 
							(rf_fullcombo_pred_test[,5] * rf_fullcombo_2cat_auc_wt) + 
							
							(rf_base_pred_test[,6] * rf_base_4cat_auc_wt) + 
                            (rf_sub_pred_test[,6] * rf_sub_4cat_auc_wt) +
                            (rf_att_pred_test[,6] * rf_att_4cat_auc_wt) +
                            (rf_emtxt_pred_test[,6] * rf_emtxt_4cat_auc_wt) +
                            (rf_doctxt_pred_test[,6] * rf_doctxt_4cat_auc_wt) + 
                            (rf_metacombo_pred_test[,6] * rf_metacombo_4cat_auc_wt) + 
							(rf_txtcombo_pred_test[,6] * rf_txtcombo_4cat_auc_wt) + 
							(rf_fullcombo_pred_test[,6] * rf_fullcombo_4cat_auc_wt) + 
							
							(rf_base_pred_test[,7] * rf_base_5cat_auc_wt) + 
                            (rf_sub_pred_test[,7] * rf_sub_5cat_auc_wt) +
                            (rf_att_pred_test[,7] * rf_att_5cat_auc_wt) +
                            (rf_emtxt_pred_test[,7] * rf_emtxt_5cat_auc_wt) +
                            (rf_doctxt_pred_test[,7] * rf_doctxt_5cat_auc_wt) + 
                            (rf_metacombo_pred_test[,7] * rf_metacombo_5cat_auc_wt) + 
							(rf_txtcombo_pred_test[,7] * rf_txtcombo_5cat_auc_wt) + 
							(rf_fullcombo_pred_test[,7] * rf_fullcombo_5cat_auc_wt) + 
							
							(rf_base_pred_test[,8] * rf_base_7cat_auc_wt) + 
                            (rf_sub_pred_test[,8] * rf_sub_7cat_auc_wt) +
                            (rf_att_pred_test[,8] * rf_att_7cat_auc_wt) +
                            (rf_emtxt_pred_test[,8] * rf_emtxt_7cat_auc_wt) +
                            (rf_doctxt_pred_test[,8] * rf_doctxt_7cat_auc_wt) + 
                            (rf_metacombo_pred_test[,8] * rf_metacombo_7cat_auc_wt) + 
							(rf_txtcombo_pred_test[,8] * rf_txtcombo_7cat_auc_wt) + 
							(rf_fullcombo_pred_test[,8] * rf_fullcombo_7cat_auc_wt)


rf_combo_test <- cbind.data.frame(data[test & incremental,'tx_binary'], 
              combined_prob_test_confirmed)
colnames(rf_combo_test) <- c('tx_binary','prob_con')
rf_combo_test_rocset <- thresh_iter(.0, 1, .01, rf_combo_test, 'prob_con', 'tx_binary')
rf_combo_test_rocset[rf_combo_test_rocset$inv_distance==max(rf_combo_test_rocset$inv_distance),]

#Save Image
save.image('FalsePositives.Rdata')

rf_base_2cat_auc_wt2 <- (rf_base_pred_rocset_2cat[1, ncol(rf_base_pred_rocset_2cat)]/(tot_auc - tot_auc_5cat))
rf_sub_2cat_auc_wt2 <- (rf_sub_pred_rocset_2cat[1, ncol(rf_sub_pred_rocset_2cat)]/(tot_auc - tot_auc_5cat))
rf_emtxt_2cat_auc_wt2 <- (rf_emtxt_pred_rocset_2cat[1, ncol(rf_emtxt_pred_rocset_2cat)]/(tot_auc - tot_auc_5cat))
rf_doctxt_2cat_auc_wt2 <- (rf_doctxt_pred_rocset_2cat[1, ncol(rf_doctxt_pred_rocset_2cat)]/(tot_auc - tot_auc_5cat))
rf_metacombo_2cat_auc_wt2 <- (rf_metacombo_pred_rocset_2cat[1, ncol(rf_metacombo_pred_rocset_2cat)]/(tot_auc - tot_auc_5cat))
rf_txtcombo_2cat_auc_wt2 <- (rf_txtcombo_pred_rocset_2cat[1, ncol(rf_txtcombo_pred_rocset_2cat)]/(tot_auc - tot_auc_5cat))
rf_fullcombo_2cat_auc_wt2 <- (rf_fullcombo_pred_rocset_2cat[1, ncol(rf_fullcombo_pred_rocset_2cat)]/(tot_auc - tot_auc_5cat))

rf_base_4cat_auc_wt2 <- (rf_base_pred_rocset_4cat[1, ncol(rf_base_pred_rocset_4cat)]/(tot_auc - tot_auc_5cat))
rf_sub_4cat_auc_wt2 <- (rf_sub_pred_rocset_4cat[1, ncol(rf_sub_pred_rocset_4cat)]/(tot_auc - tot_auc_5cat))
rf_att_4cat_auc_wt2 <- (rf_att_pred_rocset_4cat[1, ncol(rf_att_pred_rocset_4cat)]/(tot_auc - tot_auc_5cat))
rf_emtxt_4cat_auc_wt2 <- (rf_emtxt_pred_rocset_4cat[1, ncol(rf_emtxt_pred_rocset_4cat)]/(tot_auc - tot_auc_5cat))
rf_doctxt_4cat_auc_wt2 <- (rf_doctxt_pred_rocset_4cat[1, ncol(rf_doctxt_pred_rocset_4cat)]/(tot_auc - tot_auc_5cat))
rf_metacombo_4cat_auc_wt2 <- (rf_metacombo_pred_rocset_4cat[1, ncol(rf_metacombo_pred_rocset_4cat)]/(tot_auc - tot_auc_5cat))
rf_txtcombo_4cat_auc_wt2 <- (rf_txtcombo_pred_rocset_4cat[1, ncol(rf_txtcombo_pred_rocset_4cat)]/(tot_auc - tot_auc_5cat))
rf_fullcombo_4cat_auc_wt2 <- (rf_fullcombo_pred_rocset_4cat[1, ncol(rf_fullcombo_pred_rocset_4cat)]/(tot_auc - tot_auc_5cat))

rf_base_7cat_auc_wt2 <- (rf_base_pred_rocset_7cat[1, ncol(rf_base_pred_rocset_7cat)]/(tot_auc - tot_auc_5cat))
rf_sub_7cat_auc_wt2 <- (rf_sub_pred_rocset_7cat[1, ncol(rf_sub_pred_rocset_7cat)]/(tot_auc - tot_auc_5cat))
rf_att_7cat_auc_wt2 <- (rf_att_pred_rocset_7cat[1, ncol(rf_att_pred_rocset_7cat)]/(tot_auc - tot_auc_5cat))
rf_emtxt_7cat_auc_wt2 <- (rf_emtxt_pred_rocset_7cat[1, ncol(rf_emtxt_pred_rocset_7cat)]/(tot_auc - tot_auc_5cat))
rf_doctxt_7cat_auc_wt2 <- (rf_doctxt_pred_rocset_7cat[1, ncol(rf_doctxt_pred_rocset_7cat)]/(tot_auc - tot_auc_5cat))
rf_metacombo_7cat_auc_wt2 <- (rf_metacombo_pred_rocset_7cat[1, ncol(rf_metacombo_pred_rocset_7cat)]/(tot_auc - tot_auc_5cat))
rf_txtcombo_7cat_auc_wt2 <- (rf_txtcombo_pred_rocset_7cat[1, ncol(rf_txtcombo_pred_rocset_7cat)]/(tot_auc - tot_auc_5cat))
rf_fullcombo_7cat_auc_wt2 <- (rf_fullcombo_pred_rocset_7cat[1, ncol(rf_fullcombo_pred_rocset_7cat)]/(tot_auc - tot_auc_5cat))


combined_prob_test_confirmed2 <- (rf_base_pred_test[,5] * rf_base_2cat_auc_wt2) + 
                            (rf_sub_pred_test[,5] * rf_sub_2cat_auc_wt2) +
                           #(rf_att_pred_test[,5] * rf_att_2cat_auc_wt) +
                            (rf_emtxt_pred_test[,5] * rf_emtxt_2cat_auc_wt2) +
                            (rf_doctxt_pred_test[,5] * rf_doctxt_2cat_auc_wt2) + 
                            (rf_metacombo_pred_test[,5] * rf_metacombo_2cat_auc_wt2) + 
							(rf_txtcombo_pred_test[,5] * rf_txtcombo_2cat_auc_wt2) + 
							(rf_fullcombo_pred_test[,5] * rf_fullcombo_2cat_auc_wt2) + 
							
							(rf_base_pred_test[,6] * rf_base_4cat_auc_wt2) + 
                            (rf_sub_pred_test[,6] * rf_sub_4cat_auc_wt2) +
                            (rf_att_pred_test[,6] * rf_att_4cat_auc_wt2) +
                            (rf_emtxt_pred_test[,6] * rf_emtxt_4cat_auc_wt2) +
                            (rf_doctxt_pred_test[,6] * rf_doctxt_4cat_auc_wt2) + 
                            (rf_metacombo_pred_test[,6] * rf_metacombo_4cat_auc_wt2) + 
							(rf_txtcombo_pred_test[,6] * rf_txtcombo_4cat_auc_wt2) + 
							(rf_fullcombo_pred_test[,6] * rf_fullcombo_4cat_auc_wt2) + 
							
							(rf_base_pred_test[,8] * rf_base_7cat_auc_wt2) + 
                            (rf_sub_pred_test[,8] * rf_sub_7cat_auc_wt2) +
                            (rf_att_pred_test[,8] * rf_att_7cat_auc_wt2) +
                            (rf_emtxt_pred_test[,8] * rf_emtxt_7cat_auc_wt2) +
                            (rf_doctxt_pred_test[,8] * rf_doctxt_7cat_auc_wt2) + 
                            (rf_metacombo_pred_test[,8] * rf_metacombo_7cat_auc_wt2) + 
							(rf_txtcombo_pred_test[,8] * rf_txtcombo_7cat_auc_wt2) + 
							(rf_fullcombo_pred_test[,8] * rf_fullcombo_7cat_auc_wt2)

rf_combo_test2 <- cbind.data.frame(data[test & incremental,'tx_binary'], 
              combined_prob_test_confirmed2)
colnames(rf_combo_test2) <- c('tx_binary','prob_con')
rf_combo_test2_rocset <- thresh_iter(.0, 1, .01, rf_combo_test, 'prob_con', 'tx_binary')
rf_combo_test2_rocset[rf_combo_test2_rocset$inv_distance==max(rf_combo_test2_rocset$inv_distance),]

###Store necessary models and model metadata
models <- c('rf_base_2cat'
   , 'rf_base_4cat'
   , 'rf_base_5cat'
   , 'rf_base_7cat'
   , 'rf_sub_2cat'
   , 'rf_sub_4cat'
   , 'rf_sub_5cat'
   , 'rf_sub_7cat'
   , 'rf_att_2cat'
   , 'rf_att_4cat'
   , 'rf_att_5cat'
   , 'rf_att_7cat'
   , 'rf_emtxt_2cat'
   , 'rf_emtxt_4cat'
   , 'rf_emtxt_5cat'
   , 'rf_emtxt_7cat'
   , 'rf_doctxt_2cat'
   , 'rf_doctxt_4cat'
   , 'rf_doctxt_5cat'
   , 'rf_doctxt_7cat'
   , 'rf_metacombo_2cat'
   , 'rf_metacombo_4cat'
   , 'rf_metacombo_5cat'
   , 'rf_metacombo_7cat'
   , 'rf_txtcombo_2cat'
   , 'rf_txtcombo_4cat'
   , 'rf_txtcombo_5cat'
   , 'rf_txtcombo_7cat'
   , 'rf_fullcombo_2cat'
   , 'rf_fullcombo_4cat'
   , 'rf_fullcombo_5cat'
   , 'rf_fullcombo_7cat')

for (m in 1:length(models)) {
  assign(paste(models[m], '_stripped', sep = ''), strip_rf(eval(parse(text = models[m]))))
}

##
s3save(rf_base_2cat_stripped, bucket = "amitree-datascience/models/messages/rf", object = 'rf_base_2cat.rda')
s3save(rf_base_4cat_stripped, bucket = "amitree-datascience/models/messages/rf", object = 'rf_base_4cat.rda')
s3save(rf_base_5cat_stripped, bucket = "amitree-datascience/models/messages/rf", object = 'rf_base_5cat.rda')
s3save(rf_base_7cat_stripped, bucket = "amitree-datascience/models/messages/rf", object = 'rf_base_7cat.rda')
s3save(rf_sub_2cat_stripped, bucket = "amitree-datascience/models/messages/rf", object = 'rf_sub_2cat.rda')
s3save(rf_sub_4cat_stripped, bucket = "amitree-datascience/models/messages/rf", object = 'rf_sub_4cat.rda')
s3save(rf_sub_5cat_stripped, bucket = "amitree-datascience/models/messages/rf", object = 'rf_sub_5cat.rda')
s3save(rf_sub_7cat_stripped, bucket = "amitree-datascience/models/messages/rf", object = 'rf_sub_7cat.rda')
s3save(rf_att_2cat_stripped, bucket = "amitree-datascience/models/messages/rf", object = 'rf_att_2cat.rda')
s3save(rf_att_4cat_stripped, bucket = "amitree-datascience/models/messages/rf", object = 'rf_att_4cat.rda')
s3save(rf_att_5cat_stripped, bucket = "amitree-datascience/models/messages/rf", object = 'rf_att_5cat.rda')
s3save(rf_att_7cat_stripped, bucket = "amitree-datascience/models/messages/rf", object = 'rf_att_7cat.rda')
s3save(rf_emtxt_2cat_stripped, bucket = "amitree-datascience/models/messages/rf", object = 'rf_emtxt_2cat.rda')
s3save(rf_emtxt_4cat_stripped, bucket = "amitree-datascience/models/messages/rf", object = 'rf_emtxt_4cat.rda')
s3save(rf_emtxt_5cat_stripped, bucket = "amitree-datascience/models/messages/rf", object = 'rf_emtxt_5cat.rda')
s3save(rf_emtxt_7cat_stripped, bucket = "amitree-datascience/models/messages/rf", object = 'rf_emtxt_7cat.rda')
s3save(rf_doctxt_2cat_stripped, bucket = "amitree-datascience/models/messages/rf", object = 'rf_doctxt_2cat.rda')
s3save(rf_doctxt_4cat_stripped, bucket = "amitree-datascience/models/messages/rf", object = 'rf_doctxt_4cat.rda')
s3save(rf_doctxt_5cat_stripped, bucket = "amitree-datascience/models/messages/rf", object = 'rf_doctxt_5cat.rda')
s3save(rf_doctxt_7cat_stripped, bucket = "amitree-datascience/models/messages/rf", object = 'rf_doctxt_7cat.rda')
s3save(rf_metacombo_2cat_stripped, bucket = "amitree-datascience/models/messages/rf", object = 'rf_metacombo_2cat.rda')
s3save(rf_metacombo_4cat_stripped, bucket = "amitree-datascience/models/messages/rf", object = 'rf_metacombo_4cat.rda')
s3save(rf_metacombo_5cat_stripped, bucket = "amitree-datascience/models/messages/rf", object = 'rf_metacombo_5cat.rda')
s3save(rf_metacombo_7cat_stripped, bucket = "amitree-datascience/models/messages/rf", object = 'rf_metacombo_7cat.rda')
s3save(rf_txtcombo_2cat_stripped, bucket = "amitree-datascience/models/messages/rf", object = 'rf_txtcombo_2cat.rda')
s3save(rf_txtcombo_4cat_stripped, bucket = "amitree-datascience/models/messages/rf", object = 'rf_txtcombo_4cat.rda')
s3save(rf_txtcombo_5cat_stripped, bucket = "amitree-datascience/models/messages/rf", object = 'rf_txtcombo_5cat.rda')
s3save(rf_txtcombo_7cat_stripped, bucket = "amitree-datascience/models/messages/rf", object = 'rf_txtcombo_7cat.rda')
s3save(rf_fullcombo_2cat_stripped, bucket = "amitree-datascience/models/messages/rf", object = 'rf_fullcombo_2cat.rda')
s3save(rf_fullcombo_4cat_stripped, bucket = "amitree-datascience/models/messages/rf", object = 'rf_fullcombo_4cat.rda')
s3save(rf_fullcombo_5cat_stripped, bucket = "amitree-datascience/models/messages/rf", object = 'rf_fullcombo_5cat.rda')
s3save(rf_fullcombo_7cat_stripped, bucket = "amitree-datascience/models/messages/rf", object = 'rf_fullcombo_7cat.rda')

##Store model info in models table
model_location <- paste("amitree-datascience/models/messages/rf/", models, '.rda', sep = '')
model_script_location <- rep("TxIdentificationForests.R", length(model_location))
model_grain <- rep("message", length(model_location))
model_response <- rep(c("Binary", "Nominal", "Nominal", "Nominal"), length(model_location)/4)
model_outcome <- rep(c("tx_binary", "tx_4cat", "tx_5cat", "tx_7cat"), length(model_location)/4)
model_type <- rep("Random Forest", length(model_location))
model_active <- rep(TRUE, length(model_location))

model_data <- cbind(model_location, gen_script_location = model_script_location, grain_type = model_grain, response_type = model_response, outcome = model_outcome, model_type, active = model_active)
dbWriteTable(connection, c('datasci_modeling','models'), value = as.data.frame(model_data), overwrite = FALSE, append = TRUE, row.names = FALSE)

#Get model data and deactivate bad models
model_data <- dbGetQuery(connection, "SELECT * from datasci_modeling.models")
#dbGetQuery(connection, "UPDATE datasci_modeling.models SET active = FALSE
#                                        WHERE id = 41")
model_data <- dbGetQuery(connection, "SELECT * from datasci_modeling.models")

#Store Threshold Iterations
model_objects <- gregexpr("/", model_data$model_location)
model_objects <- ldply(lapply(model_objects, function(x){x[length(x)]}), c())
model_objects <- gsub(".rda","",substr(model_data$model_location, model_objects$V1 + 1, nchar(model_data$model_location)))
model_threshold_objects <- gsub("_[[:digit:]]cat", '_pred', model_objects)
model_threshold_objects <- model_threshold_objects[!duplicated(model_threshold_objects)]

preds <- (paste('prob_con', c(2,4,5,7), sep = ''))

threshold_iterations <- list()
threshold_iterations_model <- list()
counter = 1
for (i in 1:length(model_threshold_objects)) {
  print(model_threshold_objects[i])
  for (p in 1:length(preds)) {
    if (model_threshold_objects[i] == "rf_att_pred" & preds[p] == "prob_con2") {
    print(preds[p])
    } else {
    print(preds[p])
    threshold_iterations[[counter]] <- thresh_iter(.0, 1, .01, eval(parse(text = model_threshold_objects[i])), preds[p], 'tx_binary')
    threshold_iterations_model[[counter]] <- rep(model_objects[counter], nrow(threshold_iterations[[i]]))
    counter = counter + 1
  }
  }
}
threshold_iterations <- ldply(threshold_iterations, rbind)
threshold_iterations_model <- t(ldply(threshold_iterations_model, rbind))
threshold_iterations_model <- melt(threshold_iterations_model)
threshold_iterations_model <- data.frame(model = as.character(threshold_iterations_model$value))
threshold_iterations_model_id <- cbind(model_data[,c('id')], model_objects)
threshold_iterations_model_id <- merge(threshold_iterations_model_id, as.data.frame(threshold_iterations_model), by.x = 'model_objects', by.y = 'model', sort = FALSE)
threshold_iterations <- cbind(model_id = threshold_iterations_model_id[,2], threshold_iterations)
i <- sapply(threshold_iterations, is.infinite)
threshold_iterations[i] <- NA

dbWriteTable(connection, c('datasci_modeling','model_threshold_iterations'), value = as.data.frame(threshold_iterations), overwrite = FALSE, append = TRUE, row.names = FALSE)

#Store Ensemble Information
ensemble <- rep('Random Forest Ensemble 1', length(model_objects))
model_id <- model_data[,c('id')]
model_weight <- paste(model_objects, '_auc_wt', sep = '')
model_weight <- lapply(model_weight, function(x) {eval(parse(text = x))})
model_weight <- unlist(ldply(model_weight))
model_weight_type <- rep('AUC', length(model_objects))
active <- rep(TRUE, length(model_objects))
ensemble_data <- cbind(ensemble, model_id, model_weight, model_weight_type, active)

dbWriteTable(connection, c('datasci_modeling','ensembles'), value = as.data.frame(ensemble_data), overwrite = FALSE, append = TRUE, row.names = FALSE)

#Get Model ids where used
mad_model_ids <- model_data$id[grepl('_base_', model_data$model_location)]

#create lookup links for models and model transforms
mad_list <- list()
for (l in 1:length(mad_model_ids)) {mad_list[[l]] <- cbind(model_id = rep(mad_model_ids[l],nrow(mad_stats)),mad_stats)}
mad_data <- ldply(mad_list, rbind)


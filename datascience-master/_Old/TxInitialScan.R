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

#Clean Data
data <- data[!duplicated(data$message_id), ]
nrow(data)
data <- data[!is.na(data$created_at) & data$daysfromtxcreate >= 0, ]
nrow(data)

###Outcomes

#Drop dirty outcome data
data <- data[!(data$event_type == 'PropertyTransactionConfirmedEvent' & data$representation == 'unknown'),]
nrow(data)

#Outcomes
data$tx_binary <- ifelse(data$event_type == 'PropertyTransactionConfirmedEvent', 1, 0)
data$tx_binary_not_tx <- ifelse(data$event_type == 'PropertyTransactionConfirmedEvent', 1, ifelse(data$rejection_reason == 'not-a-transaction',0,NA))
data$tx_4cat <- ifelse(data$event_type == 'PropertyTransactionConfirmedEvent' & data$representation == 'buyer', 1,
                      ifelse(data$event_type == 'PropertyTransactionConfirmedEvent' & data$representation == 'seller', 2,
                      ifelse(data$event_type == 'PropertyTransactionConfirmedEvent' & data$representation == 'both', 3, 0)))

#Segment Data
incremental <- data$scan_type == 'incremental_scan'
initial <- data$scan_type == 'initial_scan'

#Set Indicators
rand <- runif(nrow(data))
train <- rand<.7
test <- rand >=.7 & rand <.9
validation <- rand >=.9

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

#Message Timing
mfp <- mfp(tx_binary ~ fp(daysfromtxcreate, df = 4), data = data[initial,], family = 'binomial')

data$fracpoly1 <- ((data$daysfromtxcreate + 1)/10)^-.5 
data$fracpoly2 <- data$fracpoly1 * log(((data$daysfromtxcreate + 1)/10))

#Domain Separation
email_domain <- strsplit(data$from,'@')
data$email_domain <- ldply(email_domain,rbind)[,2]
data$email_domain <- tolower(data$email_domain)
data$email_domain[is.na(data$email_domain)] <- 'unknown'
data$freq <- 1
domain_freq <- aggregate(freq ~ email_domain, data = data[initial,], FUN = sum)
data <- merge(data, domain_freq, by = 'email_domain', all.x = T, sort = FALSE)
names(data)[names(data) == "freq.y"] <- "domain_freq"
data <- data[,!colnames(data) %in% c('x.freq')]    
delta2 <- diff(diff(cumsum(table(domain_freq$freq)),lag = 1),lag = 1)
n_thresh <- as.numeric(names(delta2)[delta2==0][1])
print(noquote(paste("Sender email domain count threshold set at:", n_thresh, sep = ' ')))
email_domain_table <- as.data.frame(table(data$email_domain))
colnames(email_domain_table) <- c("email_domain","freq")
email_domain_table <- email_domain_table[order(email_domain_table$freq, decreasing=TRUE),]
email_domain_top100 <- data$email_domain %in% email_domain_table[1:100,1]
data$email_domain_recat <- ifelse(!email_domain_top100 | is.na(data$email_domain), 'small_n_domain', data$email_domain)
data$email_domain_recat <- as.factor(data$email_domain_recat)

email_domain_levels <- cbind(domain = levels(data$email_domain_recat), factor_level = seq(from = 1, to = length(levels(data$email_domain_recat))))

#Create Stratified Random Sample to fully capture email domains
p_email_domain_table <- cbind(email_domain_table$domain, email_domain_table$freq / nrow(data))

strat_rand_domain_list <- split(data, data[, 'email_domain'])

strat_rand <- ldply(lapply(seq_along(strat_rand_domain_list), function(x) {
 tmp <- as.data.frame(runif(nrow(strat_rand_domain_list[[x]])))
 rownames(tmp) <- row.names(strat_rand_domain_list[[x]])
 train <- (p_email_domain_table[x,1] * tmp) < p_email_domain_table[x,1] * .7
 test <- ((p_email_domain_table[x,1] * tmp) >= p_email_domain_table[x,1] * .7) & ((p_email_domain_table[x,1] * tmp) < p_email_domain_table[x,1] * .9)
 validate <- (p_email_domain_table[x,1] * tmp) >= p_email_domain_table[x,1] * .9
 output <- cbind.data.frame(rownames(strat_rand_domain_list[[x]]), train, test, validate)
 colnames(output) <- c('index', 'train', 'test', 'validate')
 return(output)})
,rbind)

strat_rand <- strat_rand[order(as.numeric(strat_rand$index), decreasing=FALSE),]

#Check domain distribution
email_domain_table2 <- as.data.frame(table(data$email_domain[strat_rand$train]))
colnames(email_domain_table2) <- c("email_domain","freq")
email_domain_table2 <- email_domain_table2[order(email_domain_table2$freq, decreasing=TRUE),]

p_email_domain_table2 <- cbind(as.character(email_domain_table2$email_domain), email_domain_table2$freq / nrow(data[strat_rand$train,]))
p_email_domain_table2[1:50,]
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
subject_rhs <- cbind.data.frame(subject_dtm, subject_dtm2, subject_dtm3)
subject_ln_idf <- log(sum(!is.na(data$subject))/apply(subject_rhs, 2, function(x) {sum(x>0)}))

##Compute TfIdf
subject_rhs2 <- subject_rhs * matrix(rep(subject_ln_idf,nrow(subject_rhs)), nrow = nrow(subject_rhs), byrow = TRUE)
colnames(subject_rhs) <- paste('sub_', colnames(subject_rhs), sep = '')
subject_rhs$message_id <- data$message_id

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
attname_rhs <- cbind.data.frame(attname_dtm, attname_dtm2, attname_dtm3)
ln_idf <- log(sum(!is.na(data$attachment_names))/apply(attname_rhs, 2, function(x) {sum(x>0)}))
print(noquote(paste(ncol(attname_rhs), 'unique terms will be used.')))

##Compute TfIdf
attname_rhs <- attname_rhs * matrix(rep(ln_idf,nrow(attname_rhs)), nrow = nrow(attname_rhs), byrow = TRUE)
colnames(attname_rhs) <- paste('attname_', colnames(attname_rhs), sep = '')
attname_rhs$message_id <- data$message_id

#Merge in TfIdf Columns
data_full <- merge(data, subject_rhs, by = 'message_id', all.x = TRUE, sort = FALSE)
data_full <- merge(data_full, attname_rhs, by = 'message_id', all.x = TRUE, sort = FALSE)

##Stepwise Modeling Check
base_vars <- c('body_characters_winr', 'doc_characters_winr', 'email_domain_recat', 'has_subject', 'has_body', 'has_attachment')
days_vars <- c('fracpoly1', 'fracpoly2')
subject_vars <- colnames(subject_rhs)[!colnames(subject_rhs) %in% 'message_id']
attname_vars <- colnames(attname_rhs)[!colnames(attname_rhs) %in% 'message_id']
depvar <- 'tx_binary'

data_modeling_set_days <- data_full[,c(depvar, days_vars)]
data_modeling_set_base <- data_full[,c(depvar, days_vars, base_vars)]
data_modeling_set_subject <- data_full[,c(depvar, days_vars, base_vars, subject_vars)]
data_modeling_set_full <- data_full[,c(depvar, days_vars, base_vars, subject_vars, attname_vars)]

logit <- glm(tx_binary ~ ., data = data_modeling_set_days[initial & strat_rand$train,], family = binomial(link = 'logit'),  control = list(trace = TRUE))
logit_pred <- cbind.data.frame(data[initial & strat_rand$train, 'tx_binary'], logit$fitted.values)
colnames(logit_pred) <- c('tx_binary', 'prob_con2')
logit_pred_rocset_2cat <- thresh_iter(.0, 1, .01, logit_pred, 'prob_con2', 'tx_binary')
logit_pred_rocset_2cat[logit_pred_rocset_2cat$inverse_distance==max(logit_pred_rocset_2cat$inverse_distance),]

logit <- glm(tx_binary ~ ., data = data_modeling_set_base[initial & strat_rand$train,], family = binomial(link = 'logit'),  control = list(trace = TRUE))
logit_pred <- cbind.data.frame(data[initial & strat_rand$train, 'tx_binary'], logit$fitted.values)
colnames(logit_pred) <- c('tx_binary', 'prob_con2')
logit_pred_rocset_2cat <- thresh_iter(.0, 1, .01, logit_pred, 'prob_con2', 'tx_binary')
logit_pred_rocset_2cat[logit_pred_rocset_2cat$inverse_distance==max(logit_pred_rocset_2cat$inverse_distance),]

logit <- glm(tx_binary ~ ., data = data_modeling_set_subject[initial & strat_rand$train,], family = binomial(link = 'logit'),  control = list(trace = TRUE))
logit_pred <- cbind.data.frame(data[initial & strat_rand$train, 'tx_binary'], logit$fitted.values)
colnames(logit_pred) <- c('tx_binary', 'prob_con2')
logit_pred_rocset_2cat <- thresh_iter(.0, 1, .01, logit_pred, 'prob_con2', 'tx_binary')
logit_pred_rocset_2cat[logit_pred_rocset_2cat$inverse_distance==max(logit_pred_rocset_2cat$inverse_distance),]

logit <- glm(tx_binary ~ ., data = data_modeling_set_full[initial & strat_rand$train,], family = binomial(link = 'logit'),  control = list(trace = TRUE))
logit_pred <- cbind.data.frame(data[initial & strat_rand$train, 'tx_binary'], logit$fitted.values)
colnames(logit_pred) <- c('tx_binary', 'prob_con2')
logit_pred_rocset_2cat <- thresh_iter(.0, 1, .01, logit_pred, 'prob_con2', 'tx_binary')
logit_pred_rocset_2cat[logit_pred_rocset_2cat$inverse_distance==max(logit_pred_rocset_2cat$inverse_distance),]

#Remove attachment name words from model - they don't help

logit <- glm(tx_binary ~ ., data = data_modeling_set_subject[initial & strat_rand$train,], family = binomial(link = 'logit'),  control = list(trace = TRUE))
logit_pred <- cbind.data.frame(data[initial & strat_rand$train, 'tx_binary'], logit$fitted.values)
colnames(logit_pred) <- c('tx_binary', 'prob_con2')
logit_pred_rocset_2cat <- thresh_iter(.0, 1, .01, logit_pred, 'prob_con2', 'tx_binary')
logit_pred_rocset_2cat[logit_pred_rocset_2cat$inverse_distance==max(logit_pred_rocset_2cat$inverse_distance),]

#Estimate on Test set
test_pred <- cbind.data.frame(data[initial & strat_rand$test, 'tx_binary'], predict.glm(logit, data_full[initial & strat_rand$test,], type = 'response'))
colnames(test_pred) <- c('tx_binary', 'prob_con2')
logit_pred_rocset_2cat <- thresh_iter(.0, 1, .01, test_pred, 'prob_con2', 'tx_binary')
logit_pred_rocset_2cat[logit_pred_rocset_2cat$inverse_distance==max(logit_pred_rocset_2cat$inverse_distance),]

#Threshold: looks like .58 would be good
#subject_ln_idf object has word weights

#Model and Parameter Storage

#Parameter table
parameters <- as.data.frame(logit$coefficients)
colnames(parameters) <- 'parameter'
row.names(parameters) <- gsub('`', '', row.names(parameters))
row.names(parameters) <- gsub('small_n_domain', 'smallndomain', row.names(parameters))
rows <- regexpr("\\_[^\\_]*$", row.names(parameters))
parameters$email_char <- substr(row.names(parameters), 0, rows-1)
parameters$email_char <- ifelse(grepl("sub", parameters$email_char), 'subject_line', ifelse(grepl("att", parameters$email_char), 'attachment_names', 
  ifelse(grepl("has", parameters$email_char), gsub('has_', '', row.names(parameters)[grepl('has_', row.names(parameters))]), parameters$email_char)))
parameters$email_char[grepl('fracpoly', row.names(parameters))] <- 'daysfromtxcreate'
parameters$value <- substr(row.names(parameters), rows + 1, rows + attr(rows, 'match.length'))
parameters$value[grepl('fracpoly', row.names(parameters))] <- row.names(parameters)[grepl('fracpoly', row.names(parameters))]
parameters$value[parameters$email_char == 'email_domain'] <- gsub('recat', '', parameters$value[parameters$email_char == 'email_domain'])
parameters$value[!parameters$email_char %in% c('email_domain', 'subject_line', 'daysfromtxcreate')] <- NA
row.names(parameters) <- 1:nrow(parameters)
parameters$type <- ifelse(parameters$email_char %in% c('component', 'email_domain'), 'binary', ifelse(parameters$email_char == '', 'intercept', 'int'))

#Apply weights to subject_line parameters
parameters$parameters[parameters$email_char == 'subject_line'] <- parameters$parameters[parameters$email_char == 'subject_line'] * subject_ln_idf

#Rename model object for storage
logit_email_char_2cat <- logit

#Model storage
model_storage(model = logit_email_char_2cat, connection = 'development', location_folder = "initial_scan", model_script = "TxInitialScan.R",
strip_model_fn = strip_model, model_grain = "message", model_response = "Binary", model_outcome = "binary", model_type = "Logistic Regression",
model_test = logit_pred, model_test_cols = c('prob_con2', 'tx_binary'))
  
#Ensemble storage
model_id <- dbGetQuery(connection, paste("SELECT id FROM datasci_modeling.models 
                                    WHERE model_name LIKE '%logit_email_char_2cat%'"))
ensemble <- data.frame(ensemble = 'Logit Tx 1', model_id = as.numeric(model_id), 
  model_weight = logit_pred_rocset_2cat$auc[logit_pred_rocset_2cat$inverse_distance==max(logit_pred_rocset_2cat$inverse_distance)],
  model_weight_type = 'AUC', active = TRUE)
dbWriteTable(connection, c('datasci_modeling', 'ensembles'), value = ensemble, overwrite = FALSE, append = TRUE, row.names = FALSE)

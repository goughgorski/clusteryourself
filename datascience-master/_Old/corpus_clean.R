
#Clear Workspace
rm(list=ls())

#Load libraries
source('~/workspace/datascience/functions/load_libraries.R')
load_libraries('~/workspace/datascience/functions/')
options(stringsAsFactors = FALSE)

local_connection <- local_db_conect('development')

#Query to get data for Proposed Transactions
data_msgs <- dbGetQuery(local_connection, paste("SELECT *
                                                FROM (
                                                  SELECT *
                                                    , ROW_NUMBER() OVER(PARTITION BY dstm.property_transaction_id ORDER BY (dspt.created_at - dstm.date) ASC) rown
                                                    , DATE_PART('day',dspt.created_at - dstm.date) daysfromtxcreate
                                                  FROM datasci.transaction_messages dstm
                                                  LEFT JOIN (
                                                    SELECT dstd.message_id
                                                      , STRING_AGG(dstd.attachment_name::text,'|') attachment_names
                                                      , STRING_AGG(dstd.document_content,'|') document_content
                                                      , SUM(1) document_count
                                                    FROM datasci.transaction_documents dstd
                                                    GROUP BY dstd.message_id) dstd
                                                    ON dstm.message_id = dstd.message_id
                                                  LEFT JOIN datasci.property_transactions_ds dspt
                                                    ON dspt.id = dstm.event_id
                                                  WHERE dstm.missing_reason IS NULL
                                                  AND dstm.event_type IN ('PropertyTransactionConfirmedEvent'
                                                  ,'PropertyTransactionRejectedEvent'
                                                  ,'PropertyTransactionManuallyCreatedEvent')
                                                  ) trans_text
                                                WHERE rown = 1")
                        )
#Get Message ID lists of confirmed and rejected transactions
confirmed <- data_msgs$message_id[data_msgs$confirmed == 1 & !is.na(data_msgs$confirmed)]
rejected <- data_msgs$message_id[data_msgs$confirmed == 0 & !is.na(data_msgs$confirmed)]

#Create Corpora
parallelStartMulticore(cpus = detectCores()-1)


corpus_email_bodies <- pre_process_corpus(data = data_msgs, text = 'body_text', stopword_lang = 'english')
p <- round(.05 * length(corpus_email_bodies))
corpus_email_bodies_tdm <- as.matrix(TermDocumentMatrix(corpus_email_bodies, 
                          control = list(weighting = weightTf,
                                        bounds = list(global = c(p, Inf)))))
colnames(corpus_email_bodies_tdm) <- data_msgs$message_id

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

#Create DTM for Modeling Confirmation Probability
corpus_email_bodies_dtm <- t(corpus_email_bodies_tdm)
suggested_tx_email_bodies_dtm <- corpus_email_bodies_dtm[rownames(corpus_email_bodies_dtm) %in% c(confirmed, rejected),]
colnames(suggested_tx_email_bodies_dtm) <- paste(colnames(suggested_tx_email_bodies_dtm),'.count',sep='')
suggested_tx_email_bodies_dtm_ind <- apply(suggested_tx_email_bodies_dtm,2,function(x){as.factor(ifelse(x>=1,1,x))})
colnames(suggested_tx_email_bodies_dtm_ind) <- paste(colnames(suggested_tx_email_bodies_dtm_ind),'.ind',sep='')
confirm <- as.factor(as.numeric(rownames(suggested_tx_email_bodies_dtm) %in% confirmed))
suggested_tx_email_bodies_dtm <- cbind(confirm, suggested_tx_email_bodies_dtm,suggested_tx_email_bodies_dtm_ind)

#Random Forest Model for TX Confirmation


rand <- runif(nrow(suggested_tx_email_bodies_dtm))
train <- rand < .7
test <- !train
rf1 <- randomForest(y = confirm[train], x = suggested_tx_email_bodies_dtm[train,2:ncol(suggested_tx_email_bodies_dtm)], do.trace = TRUE, ntree = 2, mtry = 9)


logit_mod <- glm(confirm~., data=as.data.frame(suggested_tx_email_bodies_dtm), family = 'binomial')


term_freq <- rbind(term_count_manual, term_count_confirm, term_count_reject, runif(length())) 
dbWriteTable(local_connection, c('datasci_projects','term_document_matrix'), value = as.data.frame(corpus_email_bodies_tdm), overwrite = T, append = F)


#Attachment Names
corpus_document_names<- pre_process_corpus(data = data_msgs,text = 'attachment_names', stopword_lang = 'english')
p <- round(.05 * length(corpus_document_names))
corpus_document_names_tdm <- as.matrix(TermDocumentMatrix(corpus_document_names, 
                          control = list(weighting = weightTf,
                                        bounds = list(global = c(5, Inf)))))
colnames(corpus_document_names_tdm) <- data_msgs$message_id

#Confirmed TDM
confirmed_document_names_tdm <- corpus_document_names_tdm[,colnames(corpus_document_names_tdm) %in% confirmed]
termsum_confirmed <- rowSums(confirmed_document_names_tdm)
termsum_confirmed_ind <- rowSums(ifelse(confirmed_document_names_tdm>=1,1,confirmed_document_names_tdm))
terms <- names(termsum_confirmed)
terms_confirmed <- cbind(terms, termsum_confirmed, termsum_confirmed_ind)

#Rejected TDM
rejected_document_names_tdm <- corpus_document_names_tdm[,colnames(corpus_document_names_tdm) %in% rejected]
termsum_rejected <- rowSums(rejected_document_names_tdm)
termsum_rejected_ind <- rowSums(ifelse(rejected_document_names_tdm>=1,1,rejected_document_names_tdm))
terms <- names(termsum_rejected)
terms_rejected <- cbind(terms, termsum_rejected, termsum_rejected_ind)

#Merge Confirmed and Rejected Summaries
term_rates <- merge(terms_confirmed, terms_rejected, by = 'terms', all = T)
term_rates$jitter <- runif(nrow(term_rates))
dbWriteTable(local_connection, c('datasci_projects','term_rates'), value = as.data.frame(term_rates), overwrite = T, append = F)







reader <- readTabular(mapping=list(content = "subject", id = "message_id"))
corpus_email_subject <- pre_process_corpus(data_msgs, reader = reader, stopword_lang = 'english')
corpus_email_subject_tdm <- as.matrix(TermDocumentMatrix(corpus_manual, 
                          control = list(weighting = weightTf,
                                        bounds = list(global = c(5, Inf)))))

reader <- readTabular(mapping=list(content = "document_content", id = "message_id"))
corpus_document_bodies <- pre_process_corpus(data_msgs, reader = reader, stopword_lang = 'english')
corpus_document_bodies_tdm <- as.matrix(TermDocumentMatrix(corpus_manual, 
                          control = list(weighting = weightTf,
                                        bounds = list(global = c(50, Inf)))))





termsum_manual <- rowSums(corpus_manual_tdm)
terms_manual <- rownames(corpus_manual_tdm)
term_count_manual <- cbind(rep('Manual Transactions',length(terms_manual)), terms_manual, termsum_manual)

data_msgs <- dbGetQuery(local_connection, paste("SELECT * 
                                      FROM datasci.transaction_messages
                                      WHERE event_type = 'PropertyTransactionConfirmedEvent'
                                      AND missing_reason IS NULL")
                  )

corpus_confirm <- pre_process_corpus(data_msgs$body_text, stopword_lang = 'english')
corpus_confirm_tdm <- as.matrix(TermDocumentMatrix(corpus_confirm, 
                          control = list(weighting = weightTf,
                                        bounds = list(global = c(50, Inf)))))

termsum_confirm <- rowSums(corpus_confirm_tdm)
terms_confirm <- rownames(corpus_confirm_tdm)
term_count_confirm <- cbind(rep('Confirmed Transactions',length(terms_confirm)), terms_confirm, termsum_confirm)

data_msgs <- dbGetQuery(local_connection, paste("SELECT * 
                                      FROM datasci.transaction_messages
                                      WHERE event_type = 'PropertyTransactionRejectedEvent'
                                      AND missing_reason IS NULL")
                  )

corpus_reject <- pre_process_corpus(data_msgs$body_text, stopword_lang = 'english')
corpus_reject_tdm <- as.matrix(TermDocumentMatrix(corpus_reject, 
                          control = list(weighting = weightTf,
                                        bounds = list(global = c(50, Inf)))))





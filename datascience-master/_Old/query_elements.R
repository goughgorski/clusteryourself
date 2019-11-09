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

#From To Domains
from_domain <- strsplit(data_msgs$from,'@')
data_msgs$from_domain <- ldply(from_domain,rbind)[,2]

#to_domain <- strsplit(data_msgs$from,'@')
#to_domain <- ldply(from_domain,rbind)[,2]

#Template Filters
#Is this doing anything? #Looks like the subject portion is failing
data_msgs$zephyr_filter <- grepl('no-reply@wufoo.com', data_msgs$from, ignore.case = T) & grepl('open escrow form', data_msgs$subject, ignore.case = T)
data_msgs$zephyr_filter_v2 <- grepl('zephyr', data_msgs$from, ignore.case = T) 
data_msgs$zephyr_filter_v3 <- grepl('zephyr', data_msgs$to, ignore.case = T) 
data_msgs$zephyr_filter_v4 <- grepl('zephyr', data_msgs$from, ignore.case = T) | grepl('zephyr', data_msgs$to, ignore.case = T) 

data_msgs$ctme_filter <- grepl('ctmecontracts', data_msgs$from, ignore.case = T) | grepl('ctmecontracts', data_msgs$to, ignore.case = T)
data_msgs$dotloop_filter <- grepl('dotloop', data_msgs$from, ignore.case = T) & grepl('has signed', data_msgs$subject, ignore.case = T)
data_msgs$zillow_filter <- grepl('zillow', data_msgs$from, ignore.case = T) & grepl('your listing is posted', data_msgs$subject, ignore.case = T)

#Skyslope
data_msgs$skyslope_filter <- grepl('skyslope',data_msgs$from, ignore.case = T) & grepl('all signed',data_msgs$subject, ignore.case = T)
#grepl('skyslope',data_msgs$to, ignore.case = T)) 
#& (grepl('skyslope',data_msgs$subject, ignore.case = T) | )

#Universsal Phrase Filters - Tight
data_msgs$aoffer_filter <- grepl('accepted offer', data_msgs$body_text, ignore.case = T)
data_msgs$roffer_filter <- grepl('ratified offer', data_msgs$body_text, ignore.case = T)
data_msgs$pagg_filter <- grepl('purchase agreement', data_msgs$body_text, ignore.case = T)
data_msgs$rcontract_filter <- grepl('ratified contract', data_msgs$body_text, ignore.case = T)

data_msgs$aoffer_filter_sub <- grepl('accepted offer', data_msgs$subject, ignore.case = T)
data_msgs$roffer_filter_sub <- grepl('ratified offer', data_msgs$subject, ignore.case = T)
data_msgs$pagg_filter_sub <- grepl('purchase agreement', data_msgs$subject, ignore.case = T)
data_msgs$rcontract_filter_sub <- grepl('ratified contract', data_msgs$subject, ignore.case = T)

data_msgs$aoffer_filter_html <- grepl('accepted offer', data_msgs$body_html, ignore.case = T)
data_msgs$roffer_filter_html <- grepl('ratified offer', data_msgs$body_html, ignore.case = T)
data_msgs$pagg_filter_html <- grepl('purchase agreement', data_msgs$body_html, ignore.case = T)
data_msgs$rcontract_filter_html <- grepl('ratified contract', data_msgs$body_html, ignore.case = T)

data_msgs$doc_aoffer_filter <- grepl('accepted offer', data_msgs$attachment_name, ignore.case = T)
data_msgs$doc_roffer_filter <- grepl('ratified offer', data_msgs$attachment_name, ignore.case = T)
data_msgs$doc_pagg_filter <- grepl('purchase agreement', data_msgs$attachment_name, ignore.case = T)
data_msgs$doc_rcontract_filter <- grepl('ratified contract', data_msgs$attachment_name, ignore.case = T)

data_msgs$doc_aoffer_filter_in <- grepl('accepted offer', data_msgs$document_content, ignore.case = T)
data_msgs$doc_roffer_filter_in <- grepl('ratified offer', data_msgs$document_content, ignore.case = T)
data_msgs$doc_pagg_filter_in <- grepl('purchase agreement', data_msgs$document_content, ignore.case = T)
data_msgs$doc_rcontract_filter_in <- grepl('ratified contract', data_msgs$document_content, ignore.case = T)

#Universal Phrase Filters - Loose
data_msgs$aoffer_filter_v2 <- grepl('accepted.*offer', data_msgs$body_text, ignore.case = T)
data_msgs$roffer_filter_v2 <- grepl('ratified.*offer', data_msgs$body_text, ignore.case = T)
data_msgs$pagg_filter_v2 <- grepl('purchase.*agreement', data_msgs$body_text, ignore.case = T)
data_msgs$rcontract_filter_v2 <- grepl('ratified.*contract', data_msgs$body_text, ignore.case = T)

data_msgs$aoffer_filter_sub_v2 <- grepl('accepted.*offer', data_msgs$subject, ignore.case = T)
data_msgs$roffer_filter_sub_v2 <- grepl('ratified.*offer', data_msgs$subject, ignore.case = T)
data_msgs$pagg_filter_sub_v2 <- grepl('purchase.*agreement', data_msgs$subject, ignore.case = T)
data_msgs$rcontract_filter_sub_v2 <- grepl('ratified.*contract', data_msgs$subject, ignore.case = T)

data_msgs$aoffer_filter_html_v2 <- grepl('accepted.*offer', data_msgs$body_html, ignore.case = T)
data_msgs$roffer_filter_html_v2 <- grepl('ratified.*offer', data_msgs$body_html, ignore.case = T)
data_msgs$pagg_filter_html_v2 <- grepl('purchase.*agreement', data_msgs$body_html, ignore.case = T)
data_msgs$rcontract_filter_html_v2 <- grepl('ratified.*contract', data_msgs$body_html, ignore.case = T)

data_msgs$doc_aoffer_filter_v2 <- grepl('accepted.*offer', data_msgs$attachment_name, ignore.case = T)
data_msgs$doc_roffer_filter_v2 <- grepl('ratified.*offer', data_msgs$attachment_name, ignore.case = T)
data_msgs$doc_pagg_filter_v2 <- grepl('purchase.*agreement', data_msgs$attachment_name, ignore.case = T)
data_msgs$doc_rcontract_filter_v2 <- grepl('ratified.*contract', data_msgs$attachment_name, ignore.case = T)

data_msgs$doc_aoffer_filter_in_v2 <- grepl('accepted.*offer', data_msgs$document_content, ignore.case = T)
data_msgs$doc_roffer_filter_in_v2 <- grepl('ratified.*offer', data_msgs$document_content, ignore.case = T)
data_msgs$doc_pagg_filter_in_v2 <- grepl('purchase.*agreement', data_msgs$document_content, ignore.case = T)
data_msgs$doc_rcontract_filter_in_v2 <- grepl('ratified.*contract', data_msgs$document_content, ignore.case = T)

#Extra Document Filters - Tight
data_msgs$doc_listing_filter <- grepl('listing agreement', data_msgs$attachment_name, ignore.case = T)
data_msgs$doc_contract_filter <- grepl('contract', data_msgs$attachment_name) & !grepl('lease', data_msgs$attachment_name, ignore.case = T) & grepl('pdf', data_msgs$attachment_name) 

#Extra Document Filters - Loose
data_msgs$doc_listing_filter_v2 <- grepl('listing.*agreement', data_msgs$attachment_name, ignore.case = T)
data_msgs$contract_filter_sub <- grepl('contract', data_msgs$subject) 

#Universal Phrase Filters - Unstructured
data_msgs$aoffer_filter_v3 <- grepl('accepted', data_msgs$body_text, ignore.case = T) & grepl('offer', data_msgs$body_text, ignore.case = T)
data_msgs$roffer_filter_v3 <- grepl('ratified', data_msgs$body_text, ignore.case = T) & grepl('offer', data_msgs$body_text, ignore.case = T)
data_msgs$pagg_filter_v3 <- grepl('purchase', data_msgs$body_text, ignore.case = T) & grepl('agreement', data_msgs$body_text, ignore.case = T)
data_msgs$rcontract_filter_v3 <- grepl('ratified', data_msgs$body_text, ignore.case = T) & grepl('contract', data_msgs$body_text, ignore.case = T)

data_msgs$aoffer_filter_sub_v3 <- grepl('accepted', data_msgs$subject, ignore.case = T) & grepl('offer', data_msgs$subject, ignore.case = T)
data_msgs$roffer_filter_sub_v3 <- grepl('ratified', data_msgs$subject, ignore.case = T) & grepl('offer', data_msgs$subject, ignore.case = T)
data_msgs$pagg_filter_sub_v3 <- grepl('purchase', data_msgs$subject, ignore.case = T) & grepl('agreement', data_msgs$subject, ignore.case = T)
data_msgs$rcontract_filter_sub_v3 <- grepl('ratified', data_msgs$subject, ignore.case = T) & grepl('contract', data_msgs$subject, ignore.case = T)

data_msgs$aoffer_filter_html_v3 <- grepl('accepted', data_msgs$body_html, ignore.case = T) & grepl('offer', data_msgs$body_html, ignore.case = T)
data_msgs$roffer_filter_html_v3 <- grepl('ratified', data_msgs$body_html, ignore.case = T) & grepl('offer', data_msgs$body_html, ignore.case = T)
data_msgs$pagg_filter_html_v3 <- grepl('purchase', data_msgs$body_html, ignore.case = T) & grepl('agreement', data_msgs$body_html, ignore.case = T)
data_msgs$rcontract_filter_html_v3 <- grepl('ratified', data_msgs$body_html, ignore.case = T) & grepl('contract', data_msgs$body_html, ignore.case = T)

data_msgs$doc_aoffer_filter_v3 <- grepl('accepted', data_msgs$attachment_name, ignore.case = T) & grepl('offer', data_msgs$attachment_name, ignore.case = T)
data_msgs$doc_roffer_filter_v3 <- grepl('ratified', data_msgs$attachment_name, ignore.case = T) & grepl('offer', data_msgs$attachment_name, ignore.case = T)
data_msgs$doc_pagg_filter_v3 <- grepl('purchase', data_msgs$attachment_name, ignore.case = T) & grepl('agreement', data_msgs$attachment_name, ignore.case = T)
data_msgs$doc_rcontract_filter_v3 <- grepl('ratified', data_msgs$attachment_name, ignore.case = T) & grepl('contract', data_msgs$attachment_name, ignore.case = T)

data_msgs$doc_aoffer_filter_in_v3 <- grepl('accepted', data_msgs$document_content, ignore.case = T) & grepl('offer', data_msgs$document_content, ignore.case = T)
data_msgs$doc_roffer_filter_in_v3 <- grepl('ratified', data_msgs$document_content, ignore.case = T) & grepl('offer', data_msgs$document_content, ignore.case = T)
data_msgs$doc_pagg_filter_in_v3 <- grepl('purchase', data_msgs$document_content, ignore.case = T) & grepl('agreement', data_msgs$document_content, ignore.case = T)
data_msgs$doc_rcontract_filter_in_v3 <- grepl('ratified', data_msgs$document_content, ignore.case = T) & grepl('contract', data_msgs$document_content, ignore.case = T)

#Universal Doc Name Filters - Unstructured
data_msgs$doc_listing_filter_v3 <- grepl('listing', data_msgs$attachment_name, ignore.case = T) & grepl('agreement', data_msgs$attachment_name, ignore.case = T)

#Attachment Name Filter - Suggested
data_msgs$doc_filter <- (grepl('listing', data_msgs$attachment_name, ignore.case = T) |
                                    grepl('ratified', data_msgs$attachment_name, ignore.case = T) |
                                    grepl('accepted', data_msgs$attachment_name, ignore.case = T) |
                                    grepl('executed', data_msgs$attachment_name, ignore.case = T) |
                                    grepl('purchase', data_msgs$attachment_name, ignore.case = T) |
                                    grepl('addendum', data_msgs$attachment_name, ignore.case = T) |
                                    grepl('counter', data_msgs$attachment_name, ignore.case = T) |
                                    grepl('seller', data_msgs$attachment_name, ignore.case = T)) &
                                   (grepl('offer', data_msgs$attachment_name, ignore.case = T) |
                                    grepl('residential', data_msgs$attachment_name, ignore.case = T) |
                                    grepl('agmt', data_msgs$attachment_name, ignore.case = T) |                                    
                                    grepl('agreement', data_msgs$attachment_name, ignore.case = T)|
                                    grepl('contract', data_msgs$attachment_name, ignore.case = T)) &
                                   (!grepl(' lease', data_msgs$attachment_name, ignore.case = T) &
                                    !grepl(' rental', data_msgs$attachment_name, ignore.case = T))

#Attachment Content Filter - Suggested
data_msgs$doc_content_filter <- (grepl('listing', data_msgs$document_content, ignore.case = T) |
                                    grepl('ratified', data_msgs$document_content, ignore.case = T) |
                                    grepl('accepted', data_msgs$document_content, ignore.case = T) |
                                    grepl('executed', data_msgs$document_content, ignore.case = T) |
                                    grepl('purchase', data_msgs$document_content, ignore.case = T) |
                                    grepl('addendum', data_msgs$document_content, ignore.case = T) |
                                    grepl('counter', data_msgs$document_content, ignore.case = T) |
                                    grepl('seller', data_msgs$document_content, ignore.case = T)) &
                                   (grepl('offer', data_msgs$document_content, ignore.case = T) |
                                    grepl('residential', data_msgs$document_content, ignore.case = T) |
                                    grepl('agmt', data_msgs$document_content, ignore.case = T) |                                    
                                    grepl('agreement', data_msgs$document_content, ignore.case = T)|
                                    grepl('contract', data_msgs$document_content, ignore.case = T))


#Subject Filter - Suggested
data_msgs$sub_filter <- (grepl('listing', data_msgs$subject, ignore.case = T) |
                                    grepl('ratified', data_msgs$subject, ignore.case = T) |
                                    grepl('accepted', data_msgs$subject, ignore.case = T) |
                                    grepl('executed', data_msgs$subject, ignore.case = T) |
                                    grepl('purchase', data_msgs$subject, ignore.case = T) |
                                    grepl('addendum', data_msgs$subject, ignore.case = T) |
                                    grepl('counter', data_msgs$subject, ignore.case = T) |
                                    grepl('seller', data_msgs$subject, ignore.case = T)) &
                                   (grepl('offer', data_msgs$subject, ignore.case = T) |
                                    grepl('residential', data_msgs$subject, ignore.case = T) |
                                    grepl('agmt', data_msgs$subject, ignore.case = T) |                                    
                                    grepl('agreement', data_msgs$subject, ignore.case = T)|
                                    grepl('contract', data_msgs$subject, ignore.case = T)) &
                                   (!grepl(' lease', data_msgs$subject, ignore.case = T) &
                                    !grepl(' rental', data_msgs$subject, ignore.case = T))

data_msgs$body_filter <- (grepl('listing', data_msgs$body_text, ignore.case = T) |
                                    grepl('ratified', data_msgs$body_text, ignore.case = T) |
                                    grepl('accepted', data_msgs$body_text, ignore.case = T) |
                                    grepl('executed', data_msgs$body_text, ignore.case = T) |
                                    grepl('purchase', data_msgs$body_text, ignore.case = T) |
                                    grepl('addendum', data_msgs$body_text, ignore.case = T) |
                                    grepl('counter', data_msgs$body_text, ignore.case = T) |
                                    grepl('seller', data_msgs$body_text, ignore.case = T)) &
                                   (grepl('offer', data_msgs$body_text, ignore.case = T) |
                                    grepl('residential', data_msgs$body_text, ignore.case = T) |
                                    grepl('agmt', data_msgs$body_text, ignore.case = T) |                                    
                                    grepl('agreement', data_msgs$body_text, ignore.case = T)|
                                    grepl('contract', data_msgs$body_text, ignore.case = T)) &
                                   (!grepl(' lease', data_msgs$body_text, ignore.case = T) &
                                    !grepl(' rental', data_msgs$body_text, ignore.case = T))

data_msgs$domain_filter <- data_msgs$zephyr_filter_v4 | data_msgs$ctme_filter | data_msgs$zillow_filter | data_msgs$skyslope_filter | data_msgs$dotloop_filter

#Drop unneeded ID variable
data_msgs <- data_msgs[,2:ncol(data_msgs)]

#Tag confirmation
data_msgs$confirmed = ifelse(data_msgs$event_type == 'PropertyTransactionConfirmedEvent',1, ifelse(data_msgs$event_type == 'PropertyTransactionRejectedEvent',0,NA))

#Get Message ID lists of confirmed and rejected transactions
confirmed <- data_msgs$message_id[data_msgs$confirmed == 1 & !is.na(data_msgs$confirmed)]
rejected <- data_msgs$message_id[data_msgs$confirmed == 0 & !is.na(data_msgs$confirmed)]

#Compute Univariate confirmation rates by boolean filter value
filters <- colnames(data_msgs)[grepl('filter',colnames(data_msgs))&!grepl('sum',colnames(data_msgs))]
z = qnorm(abs(1-.95)/2)
filter_rates <- list()
for (i in 1:length(filters)) {
 filter_rates[[i]] <- ddply(data_msgs,~eval(data_msgs[,filters[i]]),summarize, 
    p = mean(confirmed ,na.rm = T)
    , q = 1 - mean(confirmed ,na.rm = T)
    , n = sum(!is.na(confirmed))
    , ci = z*sqrt(mean(confirmed ,na.rm = T) * (1 - mean(confirmed ,na.rm = T))/sum(!is.na(confirmed))))
 filter_rates[[i]] <- cbind(rep(filters[i],nrow(filter_rates[[i]])),filter_rates[[i]])
}
filter_rates <- ldply(filter_rates,rbind)
colnames(filter_rates)[1:2] <- c('filter', 'filter_value')
dbWriteTable(local_connection, c('datasci_projects','filter_rates'), value = filter_rates, overwrite = T, append = F, row.names = FALSE)

#Compute section lengths and Presense/Absence
data_msgs$ind_body_text <- ifelse(!is.na(data_msgs$body_text),1,0)
data_msgs$ind_subject <- ifelse(!is.na(data_msgs$subject),1,0)
data_msgs$ind_attachment_name <- ifelse(!is.na(data_msgs$attachment_name),1,0)
data_msgs$ind_document_content <- ifelse(!is.na(data_msgs$document_content),1,0)

data_msgs$len_body_text_f <- nchar(data_msgs$body_text)
data_msgs$len_subject_f <- nchar(data_msgs$subject)
data_msgs$len_attachment_name_f <- nchar(data_msgs$attachment_name)
data_msgs$len_document_content_f <- nchar(data_msgs$document_content)

data_msgs$len_body_text <- ifelse(is.na(data_msgs$len_body_text_f),0,data_msgs$len_body_text_f)
data_msgs$len_subject <- ifelse(is.na(data_msgs$len_subject_f),0,data_msgs$len_subject_f)
data_msgs$len_attachment_name <- ifelse(is.na(data_msgs$len_attachment_name_f),0,data_msgs$len_attachment_name_f)
data_msgs$len_document_content <- ifelse(is.na(data_msgs$len_document_content_f),0,data_msgs$len_document_content_f)


#Train / Test split . . . train on all now, pull test data later
rand <- runif(nrow(data_msgs))
train <- rand <=.8
test <- !train


filters <- c('domain_filter', 'doc_filter', 'sub_filter', 'body_filter', 'doc_content_filter')
length_ind_values <- c('ind_body_text', 'ind_subject', 'ind_attachment_name', 'ind_document_content')
length_values <- c('len_body_text', 'len_subject', 'len_attachment_name', 'len_document_content')

modeling <- data_msgs[train,c('confirmed', filters, length_ind_values, length_values)]
modeling2 <- data_msgs[train&!is.na(weights$wt_dist),c('confirmed', filters, length_ind_values, length_values)]


formula_filters <- list()
for (i in 1:length(filters)) {
  formula_filters[[i]] <- combn(filters,i)
}
formula_filters <- paste(unlist(lapply(formula_filters,function(i){paste('(',apply(i,2,function(x){paste(x,collapse="*")}),')',sep = '',collapse=" + ")})), collapse = ' + ')

formula_length_ind <- paste(length_ind_values, collapse = ' + ')
formula_length <- paste('(', paste(length_ind_values, length_values, sep = ':'), ')', sep = '', collapse = ' + ')
formula_length_sq <- paste('(', paste(length_ind_values, paste('I(',paste(length_values, length_values,sep = ' * '),')',sep=''), sep = ':'), ')', sep = '', collapse = ' + ')

formula_length_val <- list()
for (i in 2:length(paste(length_ind_values, length_values, sep = ':'))) {
  l <- i-1
  formula_length_val[[l]] <- combn(paste(length_ind_values, length_values, sep = ':'),i)
}
formula_formula_length_val_int <- paste(unlist(lapply(formula_length_val,function(i){paste('(',apply(i,2,function(x){paste(x,collapse=":")}),')',sep = '',collapse=" + ")})), collapse = ' + ')

formula_lhs <- formula_filters
formula1 <- as.formula(paste('confirmed', formula_filters ,sep = ' ~ '))
logit_mod1 <- glm(formula1, data = modeling, family = 'binomial')

formula_lhs <- paste(formula_filters,formula_length_ind, sep = ' + ')
formula2 <- as.formula(paste('confirmed', formula_lhs ,sep = ' ~ '))
logit_mod2 <- glm(formula2, data = modeling, family = 'binomial')

formula_lhs <- paste(formula_filters,formula_length_ind,formula_length, sep = ' + ')
formula3 <- as.formula(paste('confirmed', formula_lhs ,sep = ' ~ '))
logit_mod3 <- glm(formula3, data = modeling, family = 'binomial')

formula_lhs <- paste(formula_filters,formula_length_ind,formula_length,formula_length_sq, sep = ' + ')
formula4 <- as.formula(paste('confirmed', formula_lhs ,sep = ' ~ '))
logit_mod4 <- glm(formula4, data = modeling, family = 'binomial')

formula_lhs <- paste(formula_filters,formula_length_ind,formula_length,formula_length_sq,formula_formula_length_val_int, sep = ' + ')
formula5 <- as.formula(paste('confirmed', formula_lhs ,sep = ' ~ '))
logit_mod5 <- glm(formula5, data = modeling, family = 'binomial')

data_msgs$predict5 <- predict.glm(logit_mod5, data_msgs, type = "response")
data_msgs$predict4 <- predict.glm(logit_mod4, data_msgs, type = "response")
data_msgs$predict3 <- predict.glm(logit_mod3, data_msgs, type = "response")
data_msgs$predict2 <- predict.glm(logit_mod2, data_msgs, type = "response")
data_msgs$predict1 <- predict.glm(logit_mod1, data_msgs, type = "response")

data_msgs$in_sample <- train
dbWriteTable(local_connection, c('datasci_projects','messages'), value = data_msgs, overwrite = T, append = F, row.names = FALSE)

#GWR
centroids <- aggregate(cbind(lat,lon) ~ acting_user_id ,data = data_msgs, FUN = mean)
weights <- cbind.data.frame(rep(centroids[1,'lon'],nrow(data_msgs)),rep(centroids[1,'lat'],nrow(data_msgs)),data_msgs$lon, data_msgs$lat, data_msgs$date)
weights$distance <- haversine_dist(weights[,1],weights[,2],weights[,3],weights[,4])
weights$wt_dist <- (1/(weights$distance))/sum((1/(weights$distance)), na.rm = T)
weights$wt_dist_sq <- (1/(weights$distance^2))/sum((1/(weights$distance^2)), na.rm = T)
weights$distance_rk <- ifelse(is.na(weights$distance),NA,rank(weights$distance))
weights$wt_dist_rk <- (((max(weights$distance_rk, na.rm = T)+1)-weights$distance_rk)/max(weights$distance_rk, na.rm = T))/sum(((max(weights$distance_rk, na.rm = T)+1)-weights$distance_rk)/max(weights$distance_rk, na.rm = T), na.rm = T)
weights$days_distance <- as.integer(as.Date(Sys.time())-as.Date(data_msgs$date))
weights$wt_days_dist <- (1/(weights$days_distance))/sum((1/(weights$days_distance)), na.rm = T)
weights$wt_days_dist_sq <- (1/(weights$days_distance^2))/sum((1/(weights$days_distance^2)), na.rm = T)
weights$days_distance_rk <- ifelse(is.na(weights$days_distance),NA,rank(weights$days_distance))
weights$wt_days_dist_rk <- (((max(weights$days_distance_rk, na.rm = T)+1)-weights$days_distance_rk)/max(weights$days_distance_rk, na.rm = T))/sum(((max(weights$days_distance_rk, na.rm = T)+1)-weights$days_distance_rk)/max(weights$days_distance_rk, na.rm = T), na.rm = T)

formula_lhs <- formula_filters
formula1 <- as.formula(paste('confirmed', formula_filters ,sep = ' ~ '))
logit_mod1 <- glm(formula1, data = modeling2, family = 'binomial', weights = weights$wt_dist[train&!is.na(weights$wt_dist)])
logit_mod1 <- glm(formula1, data = modeling, family = 'binomial')


# Calculates the geodesic distance between two points specified by radian latitude/longitude using the

# Calculate the number of cores
no_cores <- detectCores() - 1
 
# Initiate cluster
cl <- makeCluster(no_cores, type="FORK")

dyads <- list()
for (i in 1:length(centroids)){
  p <- i/length(centroids)
  print(p)
  dyads[[i]] <- merge(centroids[[i]],centroid, by = 'property_transaction_id', all.y = TRUE)
}





dyads <- lapply(centroids, function(x) {merge(x,centroid, by = 'property_transaction_id', all.y=TRUE)})

#Pull Test Data
sum(grepl('1003',data_msgs$attachment_name,ignore.case = T)| grepl('URLA',data_msgs$attachment_name,ignore.case = T)|
  grepl('Uniform Residential Loan Application',data_msgs$document_content,ignore.case = T) )

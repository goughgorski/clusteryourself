rm(list=ls())

#Load libraries
source('~/workspace/datascience/functions/load_libraries.R')
load_libraries('~/workspace/datascience/functions/')
options(stringsAsFactors = FALSE)
set.seed(123456789)
connection <- db_connect()

#Get contacts data
contacts <- dbGetQuery(connection, "SELECT * FROM datasci_modeling.vw_client_modeling")

#Contacts with Roles
data <- contacts[!is.na(contacts$role),]
data <- data[!grepl('amitree',data$email),]

#Domain Separation
email_domain <- strsplit(data$email,'@')
data$email_domain <- ldply(email_domain,rbind)[,2]
data <- data[!is.na(data$email_domain),]

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
write.csv(email_domain_table, file = '~/Documents/email_domain_table_txcontacts.csv')

email_domain_top250 <- data$email_domain %in% email_domain_table[1:250,1]
email_domain_top1000 <- data$email_domain %in% email_domain_table[1:1000,1]

data$email_domain_recat <- ifelse(!email_domain_top1000 | is.na(data$email_domain), 'small_n_domain', data$email_domain)
data$email_domain_recat <- as.factor(data$email_domain_recat)

data$client <- ifelse(data$role == 1, 1, ifelse(data$role == 10, 2, 0))
data$client_binary <- ifelse(data$client > 0,1,0)
data$domain_factor <- as.factor(data$email_domain)

logit <- glm(client_binary ~ email_domain_recat, data = data, family = binomial(link = 'logit'), control = list(trace = TRUE))
logit_pred <- cbind.data.frame(data[, 'client_binary'], logit$fitted.values)
colnames(logit_pred) <- c('client_binary', 'prob_con2')
logit_pred_rocset_2cat <- thresh_iter(.0, 1, .01, logit_pred, 'prob_con2', 'client_binary')
logit_pred_rocset_2cat[logit_pred_rocset_2cat$inverse_distance==max(logit_pred_rocset_2cat$inverse_distance),]

domain_whitelist <- cbind.data.frame(as.character(data$email_domain_recat), as.numeric(logit$fitted.values))
colnames(domain_whitelist) <- c('email_domain_recat', 'fitted.values')
domain_whitelist_agg <- aggregate(fitted.values ~ email_domain_recat, data = domain_whitelist, FUN = mean, na.rm = TRUE)
domain_whitelist_agg$fitted.values <- format(round(domain_whitelist_agg$fitted.values, 2), nsmall = 2)
domain_whitelist_agg <- domain_whitelist_agg[order(domain_whitelist_agg$fitted.values, decreasing=TRUE),]

blacklist <- grepl('info|sales|realestate|realty|homes|reply|realtor|soldby|listswith|talkto|title|escrow|settlement|attorney|mortgage|loan|lend|bank|c21|century21|bhg|betterhome|bhhs|berkshirehath|redfin|coldwell|kw|remax|transaction|coordinator', data$email)
data$blacklist <- ifelse(blacklist==TRUE, 1, 0)
logit2 <- glm(client_binary ~ email_domain_recat + blacklist, data = data, family = binomial(link = 'logit'), control = list(trace = TRUE))

logit_pred2 <- cbind.data.frame(data[, 'client_binary'], logit2$fitted.values)
colnames(logit_pred2) <- c('client_binary', 'prob_con2')
logit_pred_rocset_2cat2 <- thresh_iter(.0, 1, .01, logit_pred2, 'prob_con2', 'client_binary')
logit_pred_rocset_2cat2[logit_pred_rocset_2cat2$inverse_distance==max(logit_pred_rocset_2cat2$inverse_distance),]

logit3 <- glm(client_binary ~ blacklist, data = data[logit_pred$fitted.values >= .6,], family = binomial(link = 'logit'), control = list(trace = TRUE))

logit_pred2 <- cbind.data.frame(data[, 'client_binary'], logit2$fitted.values)
colnames(logit_pred2) <- c('client_binary', 'prob_con2')
logit_pred_rocset_2cat2 <- thresh_iter(.0, 1, .01, logit_pred2, 'prob_con2', 'client_binary')
logit_pred_rocset_2cat2[logit_pred_rocset_2cat2$inverse_distance==max(logit_pred_rocset_2cat2$inverse_distance),]

#Above Thresh 1
data$pass <- logit_pred$prob_con2 >= .6 & blacklist == FALSE
table(data$pass, data$client_binary)


mfp <- mfp(client_binary ~ fp(email_count, df = 4), data = pass, family = 'binomial', verbose = TRUE)

#search for dictionary terms in each email
#compare hits by term between client and non-client roles.  
terms <- dbGetQuery(connection, "SELECT distinct term FROM datasci_modeling.term_dictionaries")

#drop tri-gram terms
terms <- apply(terms, 1, function(x) {gsub('\\s+','', x)})
terms <- ldply(terms, rbind)

time_chache <- c()
term_grepl <- list()
for (n in 1:nrow(terms)){
	t0 <- Sys.time()
	term_grepl[[n]] <- grepl(terms[n,2], data$email)
	time_chache[n] <- difftime(Sys.time(), t0, units = 'sec')
	cat('\r', paste("record:", n, "projected time remaining:", mean(time_chache) * (nrow(terms) - n), sep = " "))
}







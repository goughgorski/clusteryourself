#Clear Environment
rm(list=ls())

#Load libraries
source('~/workspace/datascience/functions/load_libraries.R')
load_libraries('~/workspace/datascience/functions/')
library(betareg)

#Set Seed
set.seed(123456789)

#Connect to Local PSQL Database AND Pull Data
connection <- db_connect()

data <- dbGetQuery(connection, "SELECT * FROM datasci_projects.broad_inbox_messages dspbim
					WHERE dspbim.missing_reason = ''
					AND dspbim.n_attachments IS NOT NULL")

# dedupe data
data <- data[!duplicated(data[, c('external_account_id', 'message_id')]), which(!colnames(data) %in% c('created_at', 'id', 'missing_reason'))]

# define test, train, validation sets
sets <- data.frame(external_account_id = unique(data$external_account_id), rand = runif(length(unique(data$external_account_id))))
sets$set <- ifelse(sets$rand < 0.7, 'train', ifelse(sets$rand >= 0.9, 'validation', 'test'))

data <- merge(data, sets[, c('external_account_id', 'set')], by = 'external_account_id')

sets_ssf <- sets

# to, from, days of inbox covered

tmp <- data[data$external_account_id == 128186, c('external_account_id', 'message_id', 'from', 'to', 'cc','labels', 'date')]

# expand emails by to, from, cc
alters <- ldply(lapply(split(data[, c('external_account_id', 'message_id', 'from', 'to', 'cc', 'labels', 'date')], data[, 'external_account_id']), function(x) {
#print(unique(x[, 'external_account_id']))
	tmp <- x
	tmp[, c('to', 'from', 'cc')] <- apply(tmp[, c('to', 'from', 'cc')], 2, tolower)
	tmp[, c('to', 'from', 'cc')] <- ldply(apply(tmp[, c('to', 'from', 'cc')], 1, function(x){
		msg <-sapply(x, function(y){
			if (is.na(y) | all(grepl('@gmail.com', y) == FALSE)) {return(y)}
			nodes <- strsplit(y, ',')[[1]]
			gnodes <- nodes[grep('@gmail.com', nodes)]
			gnodes <- gsub('@gmail.com', '', gnodes)
			gnodes <- gsub('[.]', '', gnodes)
			gnodes[grep('+', gnodes)] <- gsub('[+].*$', '', gnodes[grep('+', gnodes)])
			gnodes <- paste(gnodes, '@gmail.com', sep = '')
			if (length(gnodes) == length(nodes)) {return(paste(gnodes, collapse = ','))}
			return(paste(paste(gnodes, collapse = ','), paste(nodes[-grep('@gmail.com', nodes)], collapse = ','), sep = ','))
			})
		as.data.frame(t(as.data.frame(msg)))
		}), rbind)[, c('to', 'from', 'cc')]

	tmp$to <- as.character(tmp$to)
	tmp$from <- as.character(tmp$from)
	tmp$cc <- as.character(tmp$cc)

	# determine user email address
	user_email <- unique(tmp$from[grepl('SENT|DRAFT', tmp$labels)])

	if (length(user_email) == 0) {
		user_email <- as.data.frame(table(unlist(strsplit(tmp$to[grepl('INBOX|IMPORTANT|CATEGORY_', tmp$labels)], ','))))
		user_email <- as.character(user_email[which.max(user_email[,'Freq']), 'Var1'])
	}

	if (length(user_email) == 0) {print(noquote(paste('unable to determine email address for external_account_id', unique(tmp$external_account_id))))}

	# remove draft emails
	tmp <- tmp[!grepl('DRAFT',tmp$labels),]

	if (nrow(tmp) > 0) {

	tmp <- ldply(apply(tmp, 1, function(x) {

		cc <- unlist(strsplit(x['cc'], ','))
		to <- unlist(strsplit(x['to'], ','))
		to <- c(to, cc)
		if(length(to[!is.na(to)]) == 0) {to <- ""}
		data.frame(message_id = unique(x['message_id']), to = to[!is.na(to)], from = unique(x['from']),
		 from_ego = ifelse(unique(x['from']) %in% user_email, 1, 0), date = unique(x['date']))
		}), rbind)

	tmp$alter <- ifelse(tmp$from %in% user_email, as.character(tmp$to), as.character(tmp$from))
	tmp <- tmp[!tmp$alter %in% user_email,]
	tmp <- tmp[tmp$alter != 'mailer-daemon@google.com',]

	tmp$to_ego <- ifelse(tmp$to %in% user_email, 1, 0)
	tmp$to_alter <- 0
	tmp$to_alter[as.character(tmp$to) == as.character(tmp$alter)] <- 1
	tmp$to_alter2d <- 0
	tmp$to_alter2d[tmp$to_ego == 0 & tmp$to_alter == 0] <- 1

	# add rows for intra-alter messages
	a2d <- tmp[tmp$to_alter2d == 1,]
	tmp$xalter <- 0
	a2d$alter <- a2d$to
	if (nrow(a2d) > 0) {
		a2d$to_alter <- 0
		a2d$to_alter2d <- 0
		a2d$xalter <- 1
		tmp <- rbind(tmp, a2d)
	}
	
	# count of days of inbox coverage and message velocity
	tmp$date <- as.POSIXct(tmp$date)
	#tmp$days_inbox_coverage <- as.numeric(round(difftime(max(x[,'date']), min(x[,'date']), units = 'days'), digits = 3))
	tmp$msgs_per_day <- length(unique(tmp[,'message_id']))/as.numeric(difftime(max(tmp[,'date']), min(tmp[,'date']), units = 'days'))
	tmp$msgs_per_day <- ifelse(tmp$msgs_per_day == Inf, NA, tmp$msgs_per_day)

	}

	return(tmp)
		
}), rbind)

# merge back into data	
data <- merge(data[, which(!colnames(data) %in% c('to', 'from'))], 
	alters[, which(!colnames(alters) %in% c('external_account_id', 'cc', 'labels', 'date'))], by = 'message_id')

tmp <- data[data$external_account_id == 128186 & data$alter == 'melisa@kw.com',
	c('external_account_id', 'alter', 'message_id', 'date', 'labels', 'to_ego', 'to_alter', 'to_alter2d', 'xalter', 'n_attachments', 'attachment_names', 'msgs_per_day')]

# collapse at alter level 
alters <- ldply(lapply(split(data, data[, c('external_account_id', 'alter')], drop = T), function(x) {

	tmp <- x

	ulab_avg <- mean(ifelse(grepl('Label_', tmp$labels), 1, 0), na.rm = T)
	
	ct_to_ego <- sum(tmp$to_ego)
	ct_to_alter <- sum(tmp$to_alter)
	ct_to_alter2d <- sum(tmp$to_alter2d)
	ct_xalter <- sum(tmp$xalter)
	ct_attachments <- sum(tmp$n_attachments)
	attachments_dummy <- ifelse(ct_attachments > 0, 1, 0)
	ct_pdfs <- sum(regexpr('.pdf',unlist(strsplit(tmp$attachment_names, 'attachment;|inline;'))) > 1, na.rm = T)
	pdf_dummy <- ifelse(ct_pdfs > 0, 1, 0)

	cat_personal_label_avg <- mean(ifelse(grepl('CATEGORY_PERSONAL', tmp$labels), 1, 0), na.rm = T)
	cat_promotions_label_avg <- mean(ifelse(grepl('CATEGORY_PROMOTIONS', tmp$labels), 1, 0), na.rm = T)
	cat_social_label_avg <- mean(ifelse(grepl('CATEGORY_SOCIAL', tmp$labels), 1, 0), na.rm = T)
	cat_updates_label_avg <- mean(ifelse(grepl('CATEGORY_UPDATES', tmp$labels), 1, 0), na.rm = T)
	cat_forums_label_avg <- mean(ifelse(grepl('CATEGORY_FORUMS', tmp$labels), 1, 0), na.rm = T)
	important_label_avg <- mean(ifelse(grepl('IMPORTANT', tmp$labels), 1, 0), na.rm = T)

	#cat_personal_label <- ifelse(any(grepl('CATEGORY_PERSONAL', tmp$labels)), 1, 0)
	#cat_promotions_label <- ifelse(any(grepl('CATEGORY_PROMOTIONS', tmp$labels)), 1, 0)
	#cat_social_label <- ifelse(any(grepl('CATEGORY_SOCIAL', tmp$labels)), 1, 0)
	#cat_updates_label <- ifelse(any(grepl('CATEGORY_UPDATES', tmp$labels)), 1, 0)
	#cat_forums_label <- ifelse(any(grepl('CATEGORY_FORUMS', tmp$labels)), 1, 0)
	#important_label <- ifelse(any(grepl('IMPORTANT', tmp$labels)), 1, 0)

	multi_msg <- ifelse(nrow(tmp[tmp$to_ego == 1 | tmp$to_alter == 1,]) > 1, 1, 0)
	
	if (multi_msg == 1) {
		timestamps <- tmp[tmp$to_ego == 1 | tmp$to_alter == 1, 'date']
		gaps <- c()
		for (i in 2:nrow(tmp[tmp$to_ego == 1 | tmp$to_alter == 1,])) {
			diff <- as.numeric(difftime(timestamps[order(timestamps)][i], timestamps[order(timestamps)][i - 1], units = 'days'))
			gaps <- c(gaps, diff)
		}
		gaps_mean <- mean(gaps)
		gaps_sd <- ifelse(is.na(sd(gaps)), 0, sd(gaps))
	} else {gaps_mean <- 0
			gaps_sd <- 0
	}

	multi_msg_to <- ifelse(nrow(tmp[tmp$to_ego == 1,]) > 1, 1, 0)
	
	if (multi_msg_to == 1) {
		timestamps <- tmp[tmp$to_ego == 1, 'date']
		gaps <- c()
		for (i in 2:nrow(tmp[tmp$to_ego == 1,])) {
			diff <- as.numeric(difftime(timestamps[order(timestamps)][i], timestamps[order(timestamps)][i - 1], units = 'days'))
			gaps <- c(gaps, diff)
		}
		gaps_to_mean <- mean(gaps)
		gaps_to_sd <- ifelse(is.na(sd(gaps)), 0, sd(gaps))
	} else {gaps_to_mean <- 0
			gaps_to_sd <- 0
	}

	multi_msg_from <- ifelse(nrow(tmp[tmp$to_alter == 1,]) > 1, 1, 0)
	
	if (multi_msg_from == 1) {
		timestamps <- tmp[tmp$to_alter == 1, 'date']
		gaps <- c()
		for (i in 2:nrow(tmp[tmp$to_alter == 1,])) {
			diff <- as.numeric(difftime(timestamps[order(timestamps)][i], timestamps[order(timestamps)][i - 1], units = 'days'))
			gaps <- c(gaps, diff)
		}
		gaps_from_mean <- mean(gaps)
		gaps_from_sd <- ifelse(is.na(sd(gaps)), 0, sd(gaps))
	} else {gaps_from_mean <- 0
			gaps_from_sd <- 0
	}

	data.frame(external_account_id = unique(x[,'external_account_id']),
				alter = unique(x[,'alter']),
				ulab_avg = ulab_avg,
				ct_to_ego = ct_to_ego,
				ct_to_alter = ct_to_alter,
				ct_to_alter2d = ct_to_alter2d,
				ct_xalter = ct_xalter,
				attachments_dummy = attachments_dummy,
				pdf_dummy = pdf_dummy,
				ct_attachments = ct_attachments,
				ct_pdfs = ct_pdfs,
				cat_personal_label_avg = cat_personal_label_avg,
				cat_promotions_label_avg = cat_promotions_label_avg,
				cat_social_label_avg = cat_social_label_avg,
				cat_updates_label_avg = cat_updates_label_avg,
				cat_forums_label_avg = cat_forums_label_avg,
				important_label_avg = important_label_avg,
				msgs_per_day = unique(x[, 'msgs_per_day']),
				multi_msg = multi_msg, 
				gaps_mean = gaps_mean, 
				gaps_sd = gaps_sd,
				multi_msg_to = multi_msg_to, 
				gaps_to_mean = gaps_to_mean, 
				gaps_to_sd = gaps_to_sd,
				multi_msg_from = multi_msg_from, 
				gaps_from_mean = gaps_from_mean, 
				gaps_from_sd = gaps_from_sd,
				set = unique(x[, 'set'])
				)

}), rbind)

# adding fractional polynomials
mfp_ct_to_ego <- mfp(ulab ~ fp(ct_to_ego, df = 4), alters[alters$alter != '' & alters$set == 'train',], family = 'binomial')
mfp_ct_to_alter <- mfp(ulab ~ fp(ct_to_alter, df = 4), alters[alters$alter != '' & alters$set == 'train',], family = 'binomial')
mfp_gaps_mean <- mfp(ulab ~ fp(gaps_mean, df = 4), alters[alters$alter != '' & alters$set == 'train',], family = 'binomial')
mfp_gaps_sd <- mfp(ulab ~ fp(gaps_sd, df = 4), alters[alters$alter != '' & alters$set == 'train',], family = 'binomial')
mfp_gaps_to_mean <- mfp(ulab ~ fp(gaps_to_mean, df = 4), alters[alters$alter != '' & alters$set == 'train',], family = 'binomial')
mfp_gaps_to_sd <- mfp(ulab ~ fp(gaps_to_sd, df = 4), alters[alters$alter != '' & alters$set == 'train',], family = 'binomial')
mfp_gaps_from_mean <- mfp(ulab ~ fp(gaps_from_mean, df = 4), alters[alters$alter != '' & alters$set == 'train',], family = 'binomial')
mfp_gaps_from_sd <- mfp(ulab ~ fp(gaps_from_sd, df = 4), alters[alters$alter != '' & alters$set == 'train',], family = 'binomial')

alters$fracpoly1_ct_to_ego <- ((alters$ct_to_ego+1)/10)^-2
alters$fracpoly2_ct_to_ego <- ((alters$ct_to_ego+1)/10)^-0.5
alters$fracpoly1_ct_to_alter <- (alters$ct_to_alter+1)^-2
alters$fracpoly2_ct_to_alter <- (alters$ct_to_alter+1)^-0.5
alters$fracpoly1_gaps_mean <- (alters$gaps_mean+0.1)^-2
alters$fracpoly2_gaps_mean <- (alters$gaps_mean+0.1)^-0.5
alters$fracpoly1_gaps_sd <- (alters$gaps_sd+0.1)^-2
alters$fracpoly2_gaps_sd <- (alters$gaps_sd+0.1)^-2*log((alters$gaps_sd+0.1))
#alters$fracpoly2_gaps_sd <- (alters$gaps_sd+0.1)^0.5
alters$fracpoly1_gaps_to_mean <- (alters$gaps_to_mean+0.1)^-2
alters$fracpoly2_gaps_to_mean <- (alters$gaps_to_mean+0.1)^-0.5
alters$fracpoly1_gaps_to_sd <- (alters$gaps_to_sd+0.1)^-1
#alters$fracpoly1_gaps_to_sd <- log((alters$gaps_to_sd+0.1))
#alters$fracpoly2_gaps_to_sd <- (alters$gaps_to_sd+0.1)^0.5
alters$fracpoly1_gaps_from_mean <- (alters$gaps_from_mean+0.1)^-2
alters$fracpoly2_gaps_from_mean <- log((alters$gaps_from_mean+0.1))
#alters$fracpoly2_gaps_from_mean <- (alters$gaps_from_mean+0.1)^0.5
alters$fracpoly1_gaps_from_sd <- (alters$gaps_from_sd+0.1)^-2
alters$fracpoly2_gaps_from_sd <- log((alters$gaps_from_sd+0.1))
#alters$fracpoly2_gaps_from_sd <- (alters$gaps_from_sd+0.1)^0.5

alters$ulab <- ifelse(alters$ulab_avg > 0, 1, 0)

alters$cat_personal_label <- ifelse(alters$cat_personal_label_avg > 0, 1, 0)
alters$cat_promotions_label <- ifelse(alters$cat_promotions_label_avg > 0, 1, 0)
alters$cat_social_label <- ifelse(alters$cat_social_label_avg > 0, 1, 0)
alters$cat_updates_label <- ifelse(alters$cat_updates_label_avg > 0, 1, 0)
alters$cat_forums_label <- ifelse(alters$cat_forums_label_avg > 0, 1, 0)
alters$important_label <- ifelse(alters$important_label_avg > 0, 1, 0)

alters$cat_personal_label_min <- ifelse(alters$cat_personal_label_avg == 1, 1, 0)
alters$cat_promotions_label_min <- ifelse(alters$cat_promotions_label_avg == 1, 1, 0)
alters$cat_social_label_min <- ifelse(alters$cat_social_label_avg == 1, 1, 0)
alters$cat_updates_label_min <- ifelse(alters$cat_updates_label_avg == 1, 1, 0)
alters$cat_forums_label_min <- ifelse(alters$cat_forums_label_avg == 1, 1, 0)
alters$important_label_min <- ifelse(alters$important_label_avg == 1, 1, 0)

alters_train <- alters[alters$alter != '' & alters$set == 'train',]

scale_vars <- c('ct_to_ego', 'ct_to_alter', 'ct_to_alter2d', 'ct_xalter', 'ct_attachments', 'ct_pdfs', 'msgs_per_day',
'gaps_mean', 'gaps_sd', 'gaps_to_mean', 'gaps_to_sd', 'gaps_from_mean', 'gaps_from_sd')

alters_scaling_table <- t(cbind(as.data.frame(apply(alters_train[, scale_vars], 2, mean, na.rm = T)),
						as.data.frame(apply(alters_train[, scale_vars], 2, sd, na.rm = T))))
row.names(alters_scaling_table) <- c('mean', 'sd')

alters[, scale_vars] <- as.data.frame(sapply(seq_along(scale_vars), function(x) {
	(alters[, scale_vars[x]] - alters_scaling_table['mean', scale_vars[x]])/alters_scaling_table['sd', scale_vars[x]]
	}))

logit_ulab <- glm(ulab ~ ct_to_ego + ct_to_alter + ct_to_alter2d + attachments_dummy + pdf_dummy + ct_attachments + ct_pdfs +
				cat_personal_label + cat_promotions_label + cat_social_label + cat_updates_label + cat_forums_label + important_label +
				msgs_per_day + multi_msg_to + multi_msg_to:gaps_to_mean + multi_msg_to:gaps_to_sd +
				multi_msg_from + multi_msg_from:gaps_from_mean + multi_msg_from:gaps_from_sd,
				data = alters_train, family = binomial(link = 'logit'))

logit_ulab_fp <- glm(ulab ~ ct_to_ego + fracpoly1_ct_to_ego + fracpoly2_ct_to_ego + ct_to_alter + fracpoly1_ct_to_alter + fracpoly2_ct_to_alter + 
				ct_to_alter2d + ct_xalter + attachments_dummy + pdf_dummy + ct_attachments + ct_pdfs +
				cat_personal_label + cat_promotions_label + cat_social_label + cat_updates_label + cat_forums_label + important_label +
				msgs_per_day + 
				multi_msg + multi_msg:gaps_mean + multi_msg:gaps_sd + multi_msg:fracpoly1_gaps_mean +
				multi_msg:fracpoly2_gaps_mean + multi_msg:fracpoly1_gaps_sd + multi_msg:fracpoly2_gaps_sd + 
				multi_msg_to + multi_msg_to:gaps_to_mean + multi_msg_to:gaps_to_sd + multi_msg_to:fracpoly1_gaps_to_mean +
				multi_msg_to:fracpoly2_gaps_to_mean + multi_msg_to:fracpoly1_gaps_to_sd + 
				multi_msg_from + multi_msg_from:gaps_from_mean + multi_msg_from:gaps_from_sd + multi_msg_from:fracpoly1_gaps_from_mean +
				multi_msg_from:fracpoly2_gaps_from_mean + multi_msg_from:fracpoly1_gaps_from_sd + multi_msg_from:fracpoly2_gaps_from_sd,
				data = alters_train, family = binomial(link = 'logit'))

# contact graph specific model
logit_ulab_contact_graph <- glm(ulab ~ ct_to_ego + fracpoly1_ct_to_ego + fracpoly2_ct_to_ego + ct_to_alter + fracpoly1_ct_to_alter + fracpoly2_ct_to_alter + 
				ct_to_alter2d,
				data = alters_train, family = binomial(link = 'logit'))

# content specific model
logit_ulab_content <- glm(ulab ~ attachments_dummy + pdf_dummy + ct_attachments + ct_pdfs,
				data = alters_train, family = binomial(link = 'logit'))

# third-party tagging specific model
logit_ulab_3p_tag <- glm(ulab ~ cat_personal_label + cat_promotions_label + cat_social_label + cat_updates_label + cat_forums_label + important_label,
				data = alters_train, family = binomial(link = 'logit'))

# temporal specific model
logit_ulab_temporal <- glm(ulab ~ msgs_per_day + 
				multi_msg + multi_msg:gaps_mean + multi_msg:gaps_sd + multi_msg:fracpoly1_gaps_mean +
				multi_msg:fracpoly2_gaps_mean + multi_msg:fracpoly1_gaps_sd + 
				multi_msg_to + multi_msg_to:gaps_to_mean + multi_msg_to:gaps_to_sd + multi_msg_to:fracpoly1_gaps_to_mean +
				multi_msg_to:fracpoly2_gaps_to_mean + multi_msg_to:fracpoly1_gaps_to_sd + multi_msg_to:fracpoly2_gaps_to_sd + 
				multi_msg_from + multi_msg_from:gaps_from_mean + multi_msg_from:gaps_from_sd + multi_msg_from:fracpoly1_gaps_from_mean +
				multi_msg_from:fracpoly2_gaps_from_mean + multi_msg_from:fracpoly1_gaps_from_sd + multi_msg_from:fracpoly2_gaps_from_sd,
				data = alters_train, family = binomial(link = 'logit'))

# constrain model by removing insignificant variables
logit_ulab_fp <- glm(ulab ~ fracpoly1_ct_to_ego + fracpoly2_ct_to_ego + fracpoly2_ct_to_alter + 
				ct_to_alter2d + ct_xalter + attachments_dummy + pdf_dummy + ct_attachments + ct_pdfs +
				cat_personal_label + cat_promotions_label + cat_social_label + cat_updates_label + important_label +
				msgs_per_day + 
				multi_msg + multi_msg:fracpoly1_gaps_sd + multi_msg:fracpoly2_gaps_sd + 
				multi_msg_to + multi_msg_to:gaps_to_sd + multi_msg_to:fracpoly1_gaps_to_mean +
				multi_msg_to:fracpoly2_gaps_to_mean + multi_msg_to:fracpoly1_gaps_to_sd +
				multi_msg_from:gaps_from_sd + multi_msg_from:fracpoly1_gaps_from_mean +
				multi_msg_from:fracpoly1_gaps_from_sd + multi_msg_from:fracpoly2_gaps_from_sd,
				data = alters_train, family = binomial(link = 'logit'))

model_thresh <- data.frame(pred = logit_ulab_fp$fitted, outcome = alters_train$ulab[which(complete.cases(alters_train))])

model_thresh <- thresh_iter(0, 1, 0.01, model_thresh, 'pred', 'outcome')

# test model
coef_mat <- t(as.data.frame(logit_ulab_fp$coefficients))

for (i in 1:length(grep(':', colnames(coef_mat)))){
	tmp <- data.frame(alters[, strsplit(colnames(coef_mat)[grep(':', colnames(coef_mat))][i], ':')[[1]][1]] * 
	alters[, strsplit(colnames(coef_mat)[grep(':', colnames(coef_mat))][i], ':')[[1]][2]])
	colnames(tmp) <- paste(strsplit(colnames(coef_mat)[grep(':', colnames(coef_mat))][i], ':')[[1]], collapse = '_')
	if(i == 1) {interacts <- tmp} else {interacts <- cbind(interacts, tmp)}
}
alters <- cbind(alters, interacts)

colnames(coef_mat) <- gsub(':', '_', colnames(coef_mat))

alters_test <- alters[alters$alter != '' & alters$set == 'test',]
alters_test[, scale_vars] <- as.data.frame(sapply(seq_along(scale_vars), function(x) {
	(alters_test[, scale_vars[x]] - alters_scaling_table['mean', scale_vars[x]])/alters_scaling_table['sd', scale_vars[x]]
	}))

# use model parameters to fit values
pred <- as.matrix(data.frame(Intercept = 1, alters_test[, colnames(coef_mat)[2:ncol(coef_mat)]]))
pred <- pred %*% logit_ulab_fp$coefficients
pred <-  1/(1 + exp(-pred))
alters_test$pred <- pred

thresh_test <- thresh_iter(0, 1, 0.01, alters_test, 'pred', 'ulab')

#### BETA REGRESSION WORK ####

logit_ulab <- glm(ulab ~ ct_to_ego + ct_to_alter + important_label, data = alters_train, family = binomial(link = 'logit'))

beta_ulab <- betareg(ulab_perc ~ ct_to_ego + ct_to_alter + important_label, data = alters_train, link = 'logit')

####

# store model, ensemble, and transforms for use in deployment function
model_storage(model_obj = logit_ulab_fp, location_folder = "contact_sf", model_script = "Network_graph_label_analysis.R",
strip_model_fn = strip_model, model_grain = "contact", model_response = "Binary", model_outcome = "binary", model_type = "Logistic Regression",
model_test = alters_test, model_test_cols = c('pred', 'ulab'))

model_data <- dbGetQuery(connection, "SELECT * from datasci_modeling.models")

ensemble <- 'Contact SF Logit Ensemble 1'
model_id <- model_data$id[model_data$model_name == paste('logit_ulab_fp', '_stripped', sep = '')]
model_weight_type <- 'AUC'
active <- TRUE
model_threshold_type <- 'f2'
ensemble_data <- cbind(ensemble, model_id, model_weight_type, active, model_threshold_type)

dbWriteTable(connection, c('datasci_modeling','ensembles'), value = as.data.frame(ensemble_data), overwrite = FALSE, append = TRUE, row.names = FALSE)

contact_sf_scaling_stats <- cbind(transform_type = 'Contact SF Scaling', transform_data = toJSON(as.data.frame(alters_scaling_table)))
dbWriteTable(connection, c('datasci_modeling','model_transforms'), value = as.data.frame(contact_sf_scaling_stats), overwrite = FALSE, append = TRUE, row.names = FALSE)

# single contact for testing
alter <- list(ct_to_ego = 48, ct_to_alter = 64, ct_to_alter2d = 9, ct_attachments = 6, ct_pdfs = 6, cat_personal_label = 1, cat_promotions_label = 0,
			cat_social_label = 0, cat_updates_label = 0, cat_forums_label = 0, important_label = 1,
			gaps_mean = 0.3877908, gaps_to_mean = 0.9158464, gaps_from_mean = 0.6738351, gaps_sd = 1.037477, gaps_to_sd = 1.830384, gaps_from_sd = NA,
			msgs_per_day = 23.17107)

##### multi-contact group work #####

# store multi-contact iterations of model, ensemble, and transforms (include 2nd degree alters) for use in deployment function
logit_ulab_xalter_fp_mc <- logit_ulab_fp
alters_test_mc <- alters_test
alters_scaling_table_mc <- alters_scaling_table

model_storage(model_obj = logit_ulab_xalter_fp_mc, location_folder = "contact_sf", model_script = "Network_graph_label_analysis.R",
strip_model_fn = strip_model, model_grain = "contact", model_response = "Binary", model_outcome = "binary", model_type = "Logistic Regression",
transforms_def = 'fracpoly|:', model_test = alters_test_mc, model_test_cols = c('pred', 'ulab'))

model_data <- dbGetQuery(connection, "SELECT * from datasci_modeling.models")

ensemble <- 'Multi-Contact SF Logit Ensemble 2'
model_id <- model_data$id[model_data$model_name == paste('logit_ulab_xalter_fp_mc', '_stripped', sep = '')]
model_weight_type <- 'AUC'
active <- TRUE
model_threshold_type <- 'f2'
ensemble_data <- cbind(ensemble, model_id, model_weight_type, active, model_threshold_type)

dbWriteTable(connection, c('datasci_modeling','ensembles'), value = as.data.frame(ensemble_data), overwrite = FALSE, append = TRUE, row.names = FALSE)

contact_sf_scaling_stats_mc <- cbind(transform_type = 'Multi-Contact SF Scaling', transform_data = toJSON(as.data.frame(alters_scaling_table_mc)))
dbWriteTable(connection, c('datasci_modeling','model_transforms'), value = as.data.frame(contact_sf_scaling_stats_mc), overwrite = FALSE, append = TRUE, row.names = FALSE)


#tmp <- data[!duplicated(as.character(data$message_id)) & data$external_account_id == 128186, c('external_account_id', 'message_id', 'from', 'to', 'cc','labels', 'date')]

tmp <- data[data$external_account_id == 128186, c('external_account_id', 'message_id', 'from', 'to', 'cc', 'from_ego', 'to_ego', 'to_alter', 'to_alter2d','labels', 'date', 'alter')]

# calculate user-level matrices of scaled co-occurrence counts of messages between contacts
alter_mat <- lapply(split(data, data[, 'external_account_id']), function(x) {

	tmp <- x

	tmp[, c('external_account_id', 'from_ego', 'to_ego', 'to_alter', 'to_alter2d')] <- apply(tmp[, c('external_account_id', 'from_ego', 'to_ego', 'to_alter', 'to_alter2d')], 2, as.numeric)
	tmp[, c('message_id', 'from', 'to', 'cc', 'labels', 'date', 'alter')] <- apply(tmp[, c('message_id', 'from', 'to', 'cc', 'labels', 'date', 'alter')], 2, as.character)

	tmp <- tmp[tmp$to_ego == 0 & tmp$to != '' & tmp$from != '',]

	if (nrow(tmp) > 0) { ct_msg <- as.data.frame(table(tmp$alter))
						colnames(ct_msg) <- c('alter', 'ct')}

	multi_to <- table(as.character(tmp$message_id))
	multi_to <- names(multi_to[multi_to >1])

	tmp <- tmp[as.character(tmp$message_id) %in% multi_to,]

	if (nrow(tmp) == 0) {return(list(NA, NA, NA, NA, NA))}

	alts_to <- unique(tmp$to)
	alts_from <- unique(tmp$from[tmp$from_ego == 0])
	alts_full <- c(alts_to, alts_from)
	alts_full <- sort(unique(alts_full))

	alter_mat <- lapply(split(tmp, tmp[, 'message_id']), function(x) {
		if (all(x[, 'to_alter2d'] == 1)) {alts <- c(unique(x[, 'from']), x[, 'to'])} else {alts <- c(x[, 'to'])}
		mat <- matrix(ncol = length(alts_full), nrow = length(alts_full), dimnames = list(alts_full, alts_full), 0)
		mat[rownames(mat) %in% alts, colnames(mat) %in% alts] <- 1
		return(mat)
		})
		
	alter_mat <- Reduce('+', alter_mat)
	alter_mat <- alter_mat * upper.tri(alter_mat)

	ct_msg <- merge(ct_msg, data.frame(alter = alts_full), by = 'alter')
	cooc <- alter_mat
	a2xa1 <- alter_mat/ct_msg$ct
	a1xa2 <- t(alter_mat)/ct_msg$ct

	# melt matrix long by unique pairs
	rows <- matrix(nrow = nrow(alter_mat), ncol = ncol(alter_mat), rownames(alter_mat))
	cols <- t(matrix(nrow = nrow(alter_mat), ncol = ncol(alter_mat), rownames(alter_mat)))
	pairs <- matrix(nrow = nrow(alter_mat), ncol = ncol(alter_mat), 
		sapply(seq_along(rows), function(x) {paste(rows[x], cols[x], cooc[x], a2xa1[x], t(a1xa2)[x], sep = ',')}))
	pairs <- data.frame(pairs = pairs[upper.tri(pairs)])
	pairs <- ldply(apply(pairs, 1, function(x) {
		data.frame(external_account_id = unique(tmp$external_account_id),
			a1 = strsplit(as.character(x), ',')[[1]][1], a2 = strsplit(as.character(x), ',')[[1]][2],
			cooc = strsplit(as.character(x), ',')[[1]][3], a2xa1 = strsplit(as.character(x), ',')[[1]][4], a1xa2 = strsplit(as.character(x), ',')[[1]][5])
		}), rbind)
	return(list(cooc, a2xa1, a1xa2, pairs, ct_msg))

})

tmp <- data[data$external_account_id == 128235, c('external_account_id', 'message_id', 'from', 'to', 'cc', 'from_ego', 'to_ego', 'to_alter', 'to_alter2d','labels', 'date')]

# calculate user-level matrices of contacts sharing a user-created label (T/F)
ulab_shared_mat <- lapply(split(data, data[, 'external_account_id']), function(x) {

	tmp <- x

	tmp[, c('external_account_id', 'from_ego', 'to_ego', 'to_alter', 'to_alter2d')] <- apply(tmp[, c('external_account_id', 'from_ego', 'to_ego', 'to_alter', 'to_alter2d')], 2, as.numeric)
	tmp[, c('message_id', 'from', 'to', 'cc', 'labels', 'date')] <- apply(tmp[, c('message_id', 'from', 'to', 'cc', 'labels', 'date')], 2, as.character)

	tmp <- tmp[tmp$to != '' & tmp$from != '' & tmp$labels != '',]

	alts_to <- unique(tmp$to[tmp$to_ego == 0])
	alts_from <- unique(tmp$from[tmp$from_ego == 0])
	alts_full <- c(alts_to, alts_from)
	alts_full <- sort(unique(alts_full))

	labels <- ldply(apply(tmp, 1, function(x) {data.frame(message_id = unique(x['message_id']), label = unlist(strsplit(x['labels'], ',')))}))
	labels$ulab_shared <- grepl('Label_', labels$label)

	tmp <- merge(tmp, labels[, c('message_id', 'label', 'ulab_shared')], by = 'message_id')
	tmp <- tmp[tmp$ulab_shared == T,]

	if (nrow(tmp) == 0) {id <- unique(x[, 'external_account_id'])
						mat <- matrix(ncol = length(alts_full), nrow = length(alts_full), dimnames = list(alts_full, alts_full), FALSE)
						rows <- matrix(nrow = length(alts_full), ncol = length(alts_full), alts_full)
						cols <- t(matrix(nrow = length(alts_full), ncol = length(alts_full), alts_full))
						pairs <- matrix(nrow = length(alts_full), ncol = length(alts_full), 
							sapply(seq_along(rows), function(x) {paste(rows[x], cols[x], sep = ',')}))
						pairs <- data.frame(pairs = pairs[upper.tri(pairs)])
						pairs <- ldply(apply(pairs, 1, function(x) {
							data.frame(external_account_id = id,
								a1 = strsplit(as.character(x), ',')[[1]][1], a2 = strsplit(as.character(x), ',')[[1]][2],
								ulab_shared = FALSE)
							}), rbind)
						return(list(mat, pairs))}

	tmp$label <- as.character(tmp$label)

	ulab_shared <- lapply(split(tmp, tmp[, 'label']), function(x) {

		to <- unique(as.character(x$to[x$to_ego == 0]))
		from <- unique(as.character(x$from[x$from_ego == 0]))
		alts <- c(to, from)
		alts <- sort(unique(alts))

		mat <- matrix(ncol = length(alts_full), nrow = length(alts_full), dimnames = list(alts_full, alts_full), 0)
			mat[rownames(mat) %in% alts, colnames(mat) %in% alts] <- 1
			return(mat)
		})

	ulab_shared <- Reduce('+', ulab_shared)
	ulab_shared <- ulab_shared * upper.tri(ulab_shared)
	ulab_shared <- ulab_shared > 0

	# melt matrix long by unique pairs
	rows <- matrix(nrow = nrow(ulab_shared), ncol = ncol(ulab_shared), rownames(ulab_shared))
	cols <- t(matrix(nrow = nrow(ulab_shared), ncol = ncol(ulab_shared), rownames(ulab_shared)))
	pairs <- matrix(nrow = nrow(ulab_shared), ncol = ncol(ulab_shared), 
		sapply(seq_along(rows), function(x) {paste(rows[x], cols[x], ulab_shared[x],sep = ',')}))
	pairs <- data.frame(pairs = pairs[upper.tri(pairs)])
	pairs <- ldply(apply(pairs, 1, function(x) {
		data.frame(external_account_id = unique(tmp$external_account_id),
			a1 = strsplit(as.character(x), ',')[[1]][1], a2 = strsplit(as.character(x), ',')[[1]][2],
			ulab_shared = strsplit(as.character(x), ',')[[1]][3])
		}), rbind)
	return(list(ulab_shared, pairs))

	})

cooc_stat <- ldply(lapply(alter_mat, function(x){
	if(any(!is.na(x))) {
		cooc <- as.numeric(as.character(x[[4]][, 'cooc']))
		cooc_perc <- as.numeric(as.character(x[[4]][,'cooc']))/sum(as.numeric(as.character(x[[5]][,'ct'])))
		cooc_avg <- mean(cooc)
		cooc_z <- scale(as.numeric(as.character(x[[4]][,'cooc'])))
		data.frame(external_account_id  = x[[4]][,'external_account_id'], a1 = x[[4]][,'a1'], a2 = x[[4]][,'a2'], 
			cooc = cooc, cooc_perc = cooc_perc, cooc_avg = cooc_avg, cooc_z = cooc_z, a2xa1 = x[[4]][,'a2xa1'], a1xa2 = x[[4]][,'a1xa2'])}
	}), rbind)
lab_stat <- ldply(lapply(ulab_shared_mat, function(x) {x[[2]]}), rbind)

#### create alter-level predicted values and store tables ####

# populate environments
options(stringsAsFactors = FALSE)
assign('meta_data', load_data(db_connect(), schema = 'datasci_modeling', table = c('models','model_threshold_iterations','model_transforms',
 'transforms_pca', 'ensembles','email_domains','term_dictionaries')), envir = parent.frame())
assign('models.env', new.env(), envir = parent.frame())
load_active_models(model_table = meta_data$datasci_modeling.models, thresholds_table = meta_data$datasci_modeling.model_threshold_iterations,
 model_grain_type = c('contact','contact_dyad'), ensembles_table = meta_data$datasci_modeling.ensembles, threshold_FUN = max, environment = models.env)
assign('transforms.env', new.env(), envir = parent.frame())
load_model_transforms(model_transforms_table = meta_data$datasci_modeling.model_transforms, environment = transforms.env)

# calculate predicted values for alter label status
alters$pred <- apply(alters, 1, function(x) {classify_contact_sf_mc(x)$model_output[[1]]$model_score})

setwd('~/Public')
write.csv(sets_ssf, 'sets_ssf_mcsm.csv', row.names = F)
write.csv(data, 'data_mcsm.csv', row.names = F)
write.csv(alters, 'alters_mcsm.csv', row.names = F)
write.csv(cooc_stat, 'cooc_stat_mcsm.csv', row.names = F)
write.csv(lab_stat, 'lab_stat_mcsm.csv', row.names = F)

####

dyads <- merge(lab_stat, cooc_stat, by = c('external_account_id', 'a1', 'a2'), all.x = T)

dyads$cooc[is.na(dyads$cooc)] <- 0
dyads$cooc_perc[is.na(dyads$cooc_perc)] <- 0
dyads$cooc_avg[is.na(dyads$cooc_avg)] <- 0
dyads$cooc_z[is.na(dyads$cooc_z)] <- 0
dyads$a2xa1[is.na(dyads$a2xa1)] <- 0
dyads$a1xa2[is.na(dyads$a1xa2)] <- 0

# define test, train, validation sets
sets <- data.frame(external_account_id = unique(dyads$external_account_id), rand = runif(length(unique(dyads$external_account_id))))
sets$set <- ifelse(sets$rand < 0.3, 'train', ifelse(sets$rand >= 0.9, 'validation', 'test'))

dyads <- merge(dyads, sets[, c('external_account_id', 'set')], by = 'external_account_id')

# use model parameters to fit values
pred <- as.matrix(data.frame(Intercept = 1, alters[, colnames(coef_mat)[2:ncol(coef_mat)]]))
pred <- pred %*% logit_ulab_fp$coefficients
pred <-  1/(1 + exp(-pred))
alters$pred <- pred

dyads <- merge(dyads, alters[, c('external_account_id', 'alter', 'pred')], by.x = c('external_account_id', 'a1'),
			by.y = c('external_account_id', 'alter'), all.x = T)
colnames(dyads)[which(colnames(dyads) == 'pred')] <- 'a1_label_pred'
dyads <- merge(dyads, alters[, c('external_account_id', 'alter', 'pred')], by.x = c('external_account_id', 'a2'),
			by.y = c('external_account_id', 'alter'), all.x = T)
colnames(dyads)[which(colnames(dyads) == 'pred')] <- 'a2_label_pred'

dyads$cooc <- as.numeric(as.character(dyads$cooc))
dyads$cooc_perc <- as.numeric(as.character(dyads$cooc_perc))
dyads$cooc_avg <- as.numeric(as.character(dyads$cooc_avg))
dyads$cooc_z <- as.numeric(as.character(dyads$cooc_z))
dyads$a2xa1 <- as.numeric(as.character(dyads$a2xa1))
dyads$a1xa1 <- as.numeric(as.character(dyads$a1xa2))
dyads$a1_label_pred <- as.numeric(dyads$a1_label_pred)
dyads$a2_label_pred <- as.numeric(dyads$a2_label_pred)
dyads$ulab_shared <- ifelse(dyads$ulab_shared == 'TRUE', 1, 0)

dyads$cooc_z_alt <- dyads$cooc_z + 1000
mfp(ulab_shared ~ fp(cooc_z, df = 4), dyads[dyads$set == 'train',], family = 'binomial')

dyads$fracpoly1_cooc <- ((dyads$cooc_perc+0.1)/0.1)^-2
dyads$fracpoly2_cooc <- ((dyads$cooc_perc+0.1)/0.1)^-2*log(((dyads$cooc_perc+0.1)/0.1))
#dyads$fracpoly1_cooc_z <- (((dyads$cooc_z+3.8)/10)^1)
#dyads$fracpoly2_cooc_z <- (((dyads$cooc_z+3.8)/10)^1*log(((dyads$cooc_z+3.8)/10)))
dyads$fracpoly1_cooc_z <- ((dyads$cooc_z+2)^-1)
dyads$cooc_z_flg <- ifelse(dyads$cooc_z > -2, 1, 0)
#dyads$fracpoly1_cooc_z <- (dyads$cooc_z_alt/1000)^-2
#dyads$fracpoly2_cooc_z <- (dyads$cooc_z_alt/1000)^-2*log((dyads$cooc_z_alt/1000))

####
write.csv(dyads, 'dyads_mcsm.csv', row.names = F)
####

logit_dyads_cooc_z_fp <- glm(ulab_shared ~ cooc_z:a1_label_pred + cooc_z:a2_label_pred + cooc_z + fracpoly1_cooc_z + a2xa1*a1xa2,
				data = dyads[dyads$set == 'train',], family = binomial(link = 'logit'))

logit_dyads_cooc_z_fp <- glm(ulab_shared ~ cooc_z:a1_label_pred + cooc_z:a2_label_pred + cooc_z + fracpoly2_cooc_z:cooc_z_flg + a2xa1*a1xa2,
				data = dyads[dyads$set == 'train',], family = binomial(link = 'logit'))


logit_dyads_cooc_fp <- glm(ulab_shared ~ cooc_perc:a1_label_pred + cooc_perc:a2_label_pred + cooc_perc + cooc_avg + fracpoly1_cooc_z + fracpoly2_cooc_z + a2xa1*a1xa2,
				data = dyads[dyads$set == 'train',], family = binomial(link = 'logit'))

dyads_model_thresh <- data.frame(pred = logit_dyads_cooc_z_fp$fitted, outcome = logit_dyads_cooc_z_fp$y)

dyads_model_thresh <- thresh_iter(0, 1, 0.01, dyads_model_thresh, 'pred', 'outcome')

# test model
dyads_coef_mat <- t(as.data.frame(logit_dyads_cooc_z_fp$coefficients))

if(nrow(interacts) > 0) {dyads <- dyads[, which(!colnames(dyads) %in% colnames(interacts))]}
for (i in 1:length(grep(':', colnames(dyads_coef_mat)))){
	tmp <- data.frame(dyads[, strsplit(colnames(dyads_coef_mat)[grep(':', colnames(dyads_coef_mat))][i], ':')[[1]][1]] * 
	dyads[, strsplit(colnames(dyads_coef_mat)[grep(':', colnames(dyads_coef_mat))][i], ':')[[1]][2]])
	colnames(tmp) <- paste(strsplit(colnames(dyads_coef_mat)[grep(':', colnames(dyads_coef_mat))][i], ':')[[1]], collapse = '_')
	if(i == 1) {interacts <- tmp} else {interacts <- cbind(interacts, tmp)}
}
dyads <- cbind(dyads, interacts)

colnames(dyads_coef_mat) <- gsub(':', '_', colnames(dyads_coef_mat))

dyads_test <- dyads[dyads$set == 'test',]

# use model parameters to fit values
pred <- as.matrix(data.frame(Intercept = 1, dyads_test[, colnames(dyads_coef_mat)[2:ncol(dyads_coef_mat)]]))
pred <- pred %*% logit_dyads_cooc_z_fp$coefficients
pred <-  1/(1 + exp(-pred))
dyads_test$pred <- pred

dyads_thresh_test <- thresh_iter(0, 1, 0.01, dyads_test, 'pred', 'ulab_shared')

model_storage(model_obj = logit_dyads_cooc_z_fp_2, location_folder = "contact_sf", model_script = "Network_graph_label_analysis.R",
strip_model_fn = strip_model, model_grain = "contact_dyad", model_response = "Binary", model_outcome = "binary", model_type = "Logistic Regression",
transforms_def = 'fracpoly|:', model_test = dyads_test, model_test_cols = c('pred', 'ulab_shared'))

model_data <- dbGetQuery(connection, "SELECT * from datasci_modeling.models")

ensemble <- 'Contact Dyad SF Logit Ensemble 4'
model_id <- model_data$id[model_data$model_name == paste('logit_dyads_cooc_z_fp_2', '_stripped', sep = '')]
model_weight_type <- 'AUC'
active <- TRUE
model_threshold_type <- 'f2'
ensemble_data <- cbind(ensemble, model_id, model_weight_type, active, model_threshold_type)

dbWriteTable(connection, c('datasci_modeling','ensembles'), value = as.data.frame(ensemble_data), overwrite = FALSE, append = TRUE, row.names = FALSE)


####

cooc_ct <- dbGetQuery(connection, 'SELECT * FROM datasci_testing.broad_inbox_messages_ego_alter_cnt_final
									WHERE external_account_id = 128186')

# clean alter names: remove '', user_email, 'mailer-daemon'; convert to lower case and aggregate duplicates
cooc_ct <- cooc_ct[cooc_ct$alter_1 != '' & cooc_ct$alter_2 != '' & cooc_ct$alter_1 != 'mailer-daemon@google.com' &
				 cooc_ct$alter_2 != 'mailer-daemon@google.com' & !(cooc_ct$alter_1 %in% user_email)  & !(cooc_ct$alter_2 %in% user_email),]

cooc_ct$alter_1 <- tolower(cooc_ct$alter_1)
cooc_ct$alter_2 <- tolower(cooc_ct$alter_2)

cooc_ct <- aggregate(cooc_ct$no_of_times, by = list(alter_1 = cooc_ct$alter_1, alter_2 = cooc_ct$alter_2), FUN = sum)
colnames(cooc_ct)[which(colnames(cooc_ct) == 'x')] <- 'ct'




tmp <- ldply(apply(data, 1, function(x){
	alter <- strsplit(x['alter'], '@')[[1]][1]
	alter <- gsub('/.', '', alter)
	if (x['to_ego'] == 1) {
		to <- strsplit(x['to'], '@')[[1]][1]
		to <- gsub('/.', '', to)
		as.character(to) == as.character(alter) }
	if (x['from_ego'] == 1) {
		from <- strsplit(x['from'], '@')[[1]][1]
		from <- gsub('/.', '', from)
		as.character(from) == as.character(alter) }
	}))

dyads <- aggregate(message_id ~ to + from, data = dyads, length)


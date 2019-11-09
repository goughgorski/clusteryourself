rm(list=ls())

#Load libraries
source('~/workspace/datascience/functions/load_libraries.R')
load_libraries('~/workspace/datascience/functions/')
options(stringsAsFactors = FALSE)
set.seed(123456789)

#Get Test Data
connection <- db_connect()
message_data <- dbGetQuery(connection, "SELECT *
                                FROM datasci_modeling.vw_tx_modeling dspvtxm
                                order by random()
                                limit 1
                                ")
models.env <- new.env()
transforms.env <- new.env()

meta_data <- load_data(connection, schema = 'datasci_modeling', table = c('term_dictionaries', 'email_domains', 'models', 'model_threshold_iterations' , 'ensembles', 'model_transforms'))
load_active_models(model_table = meta_data$datasci_modeling.models, thresholds_table = meta_data$datasci_modeling.model_threshold_iterations, model_grain_type = 'message', threshold_criteria = 'inverse_distance', threshold_FUN = max, environment = models.env)

classify_message <- function(message_data) {

	#Message Timing
	#Get mad scalar - daysfromtxcreate_win
	message_data$daysfromtxcreate <- as.numeric(as.Date(as_datetime(Sys.Date())) - as.Date(as_datetime(message_data$date)))
	message_data$daysfromtxcreate_win = ifelse(message_data$daysfromtxcreate > get('daysfromtxcreate_win', envir = transforms.env), get('daysfromtxcreate_win', envir = transforms.env), message_data$daysfromtxcreate)

	#Domain Separation and lookup
	message_data$email_domain_recat <- match(strsplit(message_data$from,'@')[[1]][2], 
											meta_data$datasci_modeling.email_domains[,'domain']
											, nomatch = 48)
	message_data$email_domain_recat <- factor(message_data$email_domain_recat, 
										meta_data$datasci_modeling.email_domains[,'factor_level'],
										labels = meta_data$datasci_modeling.email_domains[,'domain']
										)

	#Logical checks for message components
	message_data$has_subject <- ifelse(!is.na(message_data$subject),1,0)
	message_data$has_body <- ifelse(!is.na(message_data$body_text),1,0)
	message_data$has_attachment <- ifelse(!is.na(message_data$attachment_names),1,0)

	##Character Counts
	message_data$body_characters <- nchar(message_data$body_text)
	message_data$doc_characters <- nchar(message_data$document_content)

	#Get mad scalars - body_characters_win, doc_characters_win
	message_data$body_characters_win <- ifelse(message_data$body_characters > get('body_characters_win', envir = transforms.env), get('body_characters_win', envir = transforms.env), message_data$body_characters)
	message_data$doc_characters_win <- ifelse(message_data$doc_characters > get('doc_characters_win', envir = transforms.env), get('doc_characters_win', envir = transforms.env), message_data$doc_characters)

	#Recode Missing Counts to Zero
	message_data$body_characters_winr <- ifelse(is.na(message_data$body_characters_win),0,message_data$body_characters_win)
	message_data$doc_characters_winr <- ifelse(is.na(message_data$doc_characters_win),0,message_data$doc_characters_win)

	#Combine Base Data Features
	base_numeric <- as.matrix(message_data[, c('daysfromtxcreate_win', 'body_characters_winr', 'doc_characters_winr')])
	base_factor <-  as.data.frame(message_data[,'email_domain_recat'])
	base_binary <- as.matrix(message_data[, c('has_subject', 'has_body', 'has_attachment')])
	base_rhs <- cbind(base_numeric, base_factor, base_binary)
	colnames(base_rhs) <- c('daysfromtxcreate_win', 'body_characters_winr', 'doc_characters_winr', 'email_domain_recat', 'has_subject', 'has_body', 'has_attachment')

	#Dictionary control for NZV
	not_nzv <- ifelse(meta_data$datasci_modeling.term_dictionaries$nzv_used, !(meta_data$datasci_modeling.term_dictionaries$nzv), TRUE)

	###Subject Corpora
	subject_corpus <- pre_process_corpus(data = message_data, text = 'subject', stopword_lang = 'english')
	dictionary <- meta_data$datasci_modeling.term_dictionaries[meta_data$datasci_modeling.term_dictionaries$component == 'subject' & not_nzv,'term']
	subject_rhs <- tokenize_corpus(c(1,2,3), subject_corpus, 'weightTf', dictionary)

	##Compute Idf
	dictionary <- meta_data$datasci_modeling.term_dictionaries[ meta_data$datasci_modeling.term_dictionaries$component == 'subject' & not_nzv, c('term', 'ln_idf')]
	ln_idf <- merge(cbind(term = colnames(subject_rhs), t(subject_rhs)), dictionary, by = 'term', sort = FALSE)
	subject_rhs <- subject_rhs * as.numeric(ln_idf[,'ln_idf'])
	
	#Attachment Name Corpora
	attname_corpus <- pre_process_corpus(data = message_data, text = 'attachment_names', stopword_lang = 'english')
	dictionary <- meta_data$datasci_modeling.term_dictionaries[meta_data$datasci_modeling.term_dictionaries$component == 'attachment_names' & not_nzv, 'term']
	attname_rhs <- tokenize_corpus(c(1,2,3), attname_corpus, 'weightTf', dictionary)

	##Compute Idf
	dictionary <- meta_data$datasci_modeling.term_dictionaries[ meta_data$datasci_modeling.term_dictionaries$component == 'attachment_names' & not_nzv, c('term', 'ln_idf')]
	ln_idf <- merge(cbind(term = colnames(attname_rhs), t(attname_rhs)), dictionary, by = 'term', sort = FALSE)
	attname_rhs <- attname_rhs * as.numeric(ln_idf[,'ln_idf'])

	#Email Body Text Corpora
	emailtxt_corpus <- pre_process_corpus(data = message_data, text = 'body_text', stopword_lang = 'english')
	dictionary <- meta_data$datasci_modeling.term_dictionaries[meta_data$datasci_modeling.term_dictionaries$component == 'body_text' & not_nzv, 'term']
	emailtxt_rhs <- tokenize_corpus(c(1,2,3), emailtxt_corpus, 'weightTf', dictionary)

	##Compute Idf
	dictionary <- meta_data$datasci_modeling.term_dictionaries[ meta_data$datasci_modeling.term_dictionaries$component == 'body_text' & not_nzv, c('term', 'ln_idf')]
	ln_idf <- merge(cbind(term = colnames(emailtxt_rhs), t(emailtxt_rhs)), dictionary, by = 'term', sort = FALSE)
	emailtxt_rhs <- emailtxt_rhs * as.numeric(ln_idf[,'ln_idf'])

	#Document Content Text Corpora
	doctxt_corpus <- pre_process_corpus(data = message_data, text = 'document_content', stopword_lang = 'english')
	dictionary <- meta_data$datasci_modeling.term_dictionaries[meta_data$datasci_modeling.term_dictionaries$component == 'document_content' & not_nzv, 'term']
	doctxt_rhs <- tokenize_corpus(c(1,2,3), doctxt_corpus, 'weightTf', dictionary)

	##Compute Idf
	dictionary <- meta_data$datasci_modeling.term_dictionaries[ meta_data$datasci_modeling.term_dictionaries$component == 'document_content' & not_nzv, c('term', 'ln_idf')]
	ln_idf <- merge(cbind(term = colnames(doctxt_rhs), t(doctxt_rhs)), dictionary, by = 'term', sort = FALSE)
	doctxt_rhs <- doctxt_rhs * as.numeric(ln_idf[,'ln_idf'])

	#Reduce RHS dimensionality with PCA if available
	emailtxt_rhs_pca <- scale(emailtxt_rhs, transforms.env$body_text_pca$center, transforms.env$body_text_pca$scale) %*% as.matrix(transforms.env$body_text_pca[, !(colnames(transforms.env$body_text_pca) %in% c('.id', 'component','term', 'sdev', 'center', 'scale'))])
	colnames(emailtxt_rhs_pca) <- gsub('rotation.', '', colnames(emailtxt_rhs_pca))
	doctxt_rhs_pca <- scale(doctxt_rhs, transforms.env$document_content_pca$center, transforms.env$document_content_pca$scale) %*% as.matrix(transforms.env$document_content_pca[, !(colnames(transforms.env$document_content_pca) %in% c('.id', 'component','term', 'sdev', 'center', 'scale'))])
	colnames(doctxt_rhs_pca) <- gsub('rotation.', '', colnames(doctxt_rhs_pca))

	######Load Models and Estimate Scores
	#Model Groups
	#model_rhs <- list('base_rhs', 'subject_rhs', 'attname_rhs', c('base_rhs', 'subject_rhs', 'attname_rhs'))
	#model_groups <- c('base_models', 'sub_models', 'att_models', 'metacombo_models')
	model_rhs <- list('base_rhs', 'subject_rhs', 'attname_rhs', 'emailtxt_rhs_pca', 'doctxt_rhs_pca', c('base_rhs', 'subject_rhs', 'attname_rhs'), c('emailtxt_rhs_pca', 'doctxt_rhs_pca'), c('base_rhs', 'subject_rhs', 'attname_rhs', 'emailtxt_rhs_pca', 'doctxt_rhs_pca'))
	model_groups <- c('base_models', 'sub_models', 'att_models', 'emtxt_models', 'doctxt_models', 'metacombo_models', 'txtcombo_models', 'fullcombo_models')
	models <- lapply(model_groups, function(x) {assign(paste(x),ls(models.env)[grepl(gsub('_models','',x),ls(models.env))])})
	predictions <- list()
	for (g in 1:length(models)) {
		preds <- data.frame(matrix(nrow = length(models[[g]]), ncol = 2))
		for (m in 1:length(models[[g]])) {
			print(models[[g]][m])
			model <- get(models[[g]][m], envir = models.env)
			rhs <- paste('cbind(', paste(model_rhs[[g]], collapse = ', '), ')', sep = '')
			p <- predict(model, eval(parse(text = rhs)), type = 'prob')
			if (grepl('7cat', models[[g]][m])){
				p <- sum(p[4:length(p)])
			} else {p <- sum(p[2:length(p)])}
			preds[m, 2] <- p
		}
		preds[,1] <- models[[g]]
		predictions[[g]] <- preds
	}
	predictions <- ldply(predictions,rbind)
	colnames(predictions) <- c("model_name","score")

	model_ids <- merge(meta_data$datasci_modeling.models[,c('id','model_name')], meta_data$datasci_modeling.ensembles[meta_data$datasci_modeling.ensembles$active == TRUE,c('model_id', 'ensemble')], by.x = 'id', by.y = 'model_id', all.x = TRUE, sort = FALSE)

	#Fail if more than one active ensemble exists 
	if (dim(table(model_ids$ensemble, useNA = 'ifany')) != 1 ) {stop(print('More than one active ensemble loaded'))}

	#Get model scores frame
	model_scores <- merge(predictions, model_ids, by = 'model_name', sort = FALSE)
	
	#Get Thresholds
	thresholds <- get('model_thresholds', envir = models.env)
	model_scores <- merge(model_scores, thresholds, by.x = 'id', by.y = 'model_id', all.x = TRUE, sort = FALSE)
	total_auc <- sum(thresholds$auc)
	model_scores$model_weight <- model_scores$auc/total_auc

	#Prepare Model Return Payload
	model_id <- model_scores$id	
	model_score <- model_scores$score
	model_threshold <- model_scores$threshold
	model_vote <- model_scores$score >= model_scores$threshold
	model_output <- list()
	for (i in 1:length(model_id)){model_output[[i]] <- list(model_id = unbox(model_id[i]), model_score = unbox(model_score[i]), model_threshold = model_threshold[i], model_vote = model_vote[i])}
	
	#Prepare Ensemble Return Payload
	ensemble <- names(table(model_scores$ensemble))	
	vote_share <- sum(model_vote * model_scores$model_weight)	
	boolean <- vote_share >= .5
	ensemble_output <- list(ensemble = unbox(ensemble), ensemble_score = unbox(vote_share), boolean = unbox(boolean))

	#Wrap response and return
	response <- toJSON(list(ensemble_output = ensemble_output, model_output = model_output), .withNames = TRUE, pretty = TRUE)
	return(response)
}
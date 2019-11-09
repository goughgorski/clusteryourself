classify_message_stm <- function(message_data) {

	#Convert from list to data.frame
	message_data <- as.data.frame(t(unlist(message_data)))

	print(noquote("Analyzing message"))
	expected_columns <- c('subject', 'body_text')
	missing_columns <- !(expected_columns %in% colnames(message_data))
	for (i in 1:length(expected_columns)) {
		if (missing_columns[i]){
			colnames <- c(colnames(message_data), expected_columns[i])
			message_data <- cbind(message_data, '')
			colnames(message_data) <- colnames
		}
	}

	print(noquote("Subject"))

	#Subject Corpora
	subject_dictionary <- meta_data$datasci_modeling.term_dictionaries[meta_data$datasci_modeling.term_dictionaries$component == 'subject_stm','term']
	dtm_subject <- pre_process_and_tokenize(message_data, 'subject', non_stopwords = c('you', 'i'), dictionary = subject_dictionary, ngram_iter = 2)
	if (length(readCorpus(dtm_subject, type = 'Matrix')$documents) == 0) {dtm_subject <- matrix(nrow = 2, ncol = 0)
		} else {dtm_subject <- readCorpus(dtm_subject, type = 'Matrix')$documents[[1]]}

	print(noquote("Body"))

	#Email Body Text Corpora
	body_dictionary <- meta_data$datasci_modeling.term_dictionaries[meta_data$datasci_modeling.term_dictionaries$component == 'body_stm','term']
	dtm_body <- pre_process_and_tokenize(message_data, 'body_text', non_stopwords = c('you', 'i'), dictionary = body_dictionary, ngram_iter = 2)
	if (length(readCorpus(dtm_body, type = 'Matrix')$documents) == 0) {dtm_body <- matrix(nrow = 2, ncol = 0)
		} else {dtm_body <- readCorpus(dtm_body, type = 'Matrix')$documents[[1]]}


	#Load Models and Estimate Scores
	print(noquote("Loading models"))
	logit_model_names <- gsub('_stripped', '', meta_data$datasci_modeling.models$model_name[meta_data$datasci_modeling.models$gen_script_location == 'STM_client_detection.R'
																						& meta_data$datasci_modeling.models$grain_type == 'message'])
	logit_models <- lapply(logit_model_names, get, envir = models.env)
	stm_model_names <- gsub('_stripped', '', meta_data$datasci_modeling.models$model_name[meta_data$datasci_modeling.models$gen_script_location == 'STM_client_detection.R'
																						& meta_data$datasci_modeling.models$grain_type == 'document'])
	stm_models <- lapply(stm_model_names, get, envir = models.env)


	print(noquote("Scoring message"))
	estimated_topic_prevalences <- estimate_topic_prevalence(list(list(dtm_body), list(dtm_subject)),stm_models, c('body', 'subject'))
	tp_scaling_table <- get('tp_scaling_table', envir = transforms.env)
	kfit <- get('kfit_centroids', envir = transforms.env)
	cluster_assignment <- fit_cluster(estimated_topic_prevalences, tp_scaling_table, kfit)
	coef_table <- ldply(lapply(logit_models, function(x) {t(as.data.frame(x$coefficients))}), rbind)
	colnames(coef_table) <- c(1:(ncol(coef_table)))
	coef_table$model_name <- logit_model_names
	predictions <- ldply(lapply(cluster_assignment, generate_logit_probability, coef_table), rbind)[, c('model_name', 'cluster', 'score')]

	print(noquote("Preparing response"))
	model_ids <- merge(meta_data$datasci_modeling.models[,c('id','model_name')],
	 meta_data$datasci_modeling.ensembles[meta_data$datasci_modeling.ensembles$active == TRUE &
	 meta_data$datasci_modeling.ensembles$ensemble == 'STM Logit Ensemble 1',
	 c('model_id', 'ensemble')], by.x = 'id', by.y = 'model_id', all.x = TRUE, sort = FALSE)

	#Fail if more than one active ensemble exists 
	if (dim(table(model_ids$ensemble)) != 1 ) {stop(print('More than one active, called ensemble loaded'))}

	#Get model scores frame
	model_ids$model_name <- gsub('_stripped', '', model_ids$model_name)
	model_scores <- merge(predictions, model_ids, by = 'model_name', sort = FALSE)
	
	#Get Thresholds
	thresholds <- get('model_thresholds', envir = models.env)
	model_scores <- merge(model_scores, thresholds, by.x = 'id', by.y = 'model_id', all.x = TRUE, sort = FALSE)
	total_auc <- sum(model_scores$auc)
	model_scores$model_weight <- model_scores$auc/total_auc
	if ((sum(model_scores$model_weight)+.01) < 1) {stop(print("Model weighting summation error."))}

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
	#result <- toJSON(list(ensemble_output = ensemble_output, model_output = model_output), .withNames = TRUE, pretty = TRUE)
	result <- list(ensemble_output = ensemble_output, model_output = model_output)
	
	return(result)

}
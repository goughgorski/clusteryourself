classify_contact_sf <- function(contact_data) {
	
	#Convert from list to data.frame
	contact_data <- as.data.frame(t(unlist(contact_data)))

	print(noquote("Analyzing contact data"))
	expected_columns <- c('ct_to_ego', 'ct_to_alter', 'ct_to_alter2d', 'ct_attachments', 'ct_pdfs', 'gaps_mean', 'gaps_to_mean', 'gaps_from_mean', 'gaps_sd', 'gaps_to_sd', 'gaps_from_sd',
						'msgs_per_day', 'cat_personal_label', 'cat_promotions_label', 'cat_social_label', 'cat_updates_label', 'cat_forums_label', 'important_label')
	missing_columns <- !(expected_columns %in% colnames(contact_data))
	for (i in 1:length(expected_columns)) {
		if (missing_columns[i]){
			colnames <- c(colnames(contact_data), expected_columns[i])
			contact_data <- cbind(contact_data, '')
			colnames(contact_data) <- colnames
		}
	}

	print(noquote("Calculating additional covariates"))
	contact_data <- as.data.frame(t(apply(contact_data, 2, as.numeric)))
	contact_data$attachments_dummy <- ifelse(contact_data$ct_attachments > 0, 1, 0)
	contact_data$pdf_dummy <- ifelse(contact_data$ct_pdfs > 0, 1, 0)
	contact_data$multi_msg <- ifelse(sum(contact_data[, c('ct_to_ego', 'ct_to_alter')]) > 1, 1, 0)
	contact_data$multi_msg_to <- ifelse(contact_data$ct_to_ego > 1, 1, 0)
	contact_data$multi_msg_from <- ifelse(contact_data$ct_to_alter > 1, 1, 0)

	gaps_vars <- c('gaps_mean', 'gaps_to_mean', 'gaps_from_mean', 'gaps_sd', 'gaps_to_sd', 'gaps_from_sd')
	contact_data[, gaps_vars] <- as.data.frame(t(apply(contact_data[, gaps_vars], 2, function(x) {
		ifelse(is.na(x), 0, x)})))

	contact_data$fracpoly1_ct_to_ego <- ((contact_data$ct_to_ego+1)/10)^-2
	contact_data$fracpoly2_ct_to_ego <- ((contact_data$ct_to_ego+1)/10)^-0.5
	contact_data$fracpoly1_ct_to_alter <- (contact_data$ct_to_alter+1)^-2
	contact_data$fracpoly2_ct_to_alter <- (contact_data$ct_to_alter+1)^-0.5
	contact_data$fracpoly1_gaps_mean <- (contact_data$gaps_mean+0.1)^-1
	contact_data$fracpoly2_gaps_mean <- (contact_data$gaps_mean+0.1)^-0.5
	contact_data$fracpoly1_gaps_sd <- (contact_data$gaps_sd+0.1)^-2
	contact_data$fracpoly2_gaps_sd <- (contact_data$gaps_sd+0.1)^0.5
	contact_data$fracpoly1_gaps_to_mean <- (contact_data$gaps_to_mean+0.1)^-2
	contact_data$fracpoly2_gaps_to_mean <- (contact_data$gaps_to_mean+0.1)^-0.5
	contact_data$fracpoly1_gaps_to_sd <- (contact_data$gaps_to_sd+0.1)^-1
	contact_data$fracpoly1_gaps_from_mean <- (contact_data$gaps_from_mean+0.1)^-2
	contact_data$fracpoly2_gaps_from_mean <- (contact_data$gaps_from_mean+0.1)^0.5
	contact_data$fracpoly1_gaps_from_sd <- (contact_data$gaps_from_sd+0.1)^-1
	contact_data$fracpoly2_gaps_from_sd <- log((contact_data$gaps_from_sd+0.1))

	print(noquote("Transforming data"))
	contact_sf_scaling_table <- get('contact_sf_scaling_table', envir = transforms.env)
	scale_vars <- c('ct_to_ego', 'ct_to_alter', 'ct_to_alter2d', 'ct_attachments', 'ct_pdfs', 'msgs_per_day',
	'gaps_mean', 'gaps_sd', 'gaps_to_mean', 'gaps_to_sd', 'gaps_from_mean', 'gaps_from_sd')
	contact_data[, scale_vars] <- as.data.frame(t(sapply(seq_along(scale_vars), function(x) {
	(contact_data[, scale_vars[x]] - contact_sf_scaling_table['mean', scale_vars[x]])/contact_sf_scaling_table['sd', scale_vars[x]]
	})))

	#Load Models and Estimate Scores
	print(noquote("Loading models"))
	logit_model_names <- gsub('_stripped', '', meta_data$datasci_modeling.models$model_name[meta_data$datasci_modeling.models$gen_script_location == 'Network_graph_label_analysis.R'
																						& meta_data$datasci_modeling.models$grain_type == 'contact'])
	logit_models <- lapply(logit_model_names, get, envir = models.env)
	
	print(noquote("Scoring contact"))
	coef_mat <- as.matrix(ldply(lapply(logit_models, function(x) {t(as.data.frame(x$coefficients))}), rbind))

	for (i in 1:length(grep(':', colnames(coef_mat)))){
		tmp <- data.frame(contact_data[, strsplit(colnames(coef_mat)[grep(':', colnames(coef_mat))][i], ':')[[1]][1]] * 
		contact_data[, strsplit(colnames(coef_mat)[grep(':', colnames(coef_mat))][i], ':')[[1]][2]])
		colnames(tmp) <- paste(strsplit(colnames(coef_mat)[grep(':', colnames(coef_mat))][i], ':')[[1]], collapse = '_')
		if(i == 1) {interacts <- tmp} else {interacts <- cbind(interacts, tmp)}
	}
	contact_data <- cbind(contact_data, interacts)

	colnames(coef_mat) <- gsub(':', '_', colnames(coef_mat))

	# use model parameters to fit values
	pred <- as.matrix(data.frame(Intercept = 1, contact_data[, colnames(coef_mat)[2:ncol(coef_mat)]]))
	pred <- pred %*% as.numeric(coef_mat)
	pred <-  1/(1 + exp(-pred))
	predictions <- data.frame(model_name = logit_model_names, score = pred)

	print(noquote("Preparing response"))
	model_ids <- merge(meta_data$datasci_modeling.models[,c('id','model_name')],
	 meta_data$datasci_modeling.ensembles[meta_data$datasci_modeling.ensembles$active == TRUE &
	 meta_data$datasci_modeling.ensembles$ensemble == 'Contact SF Logit Ensemble 1',
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
	result <- list(ensemble_output = ensemble_output, model_output = model_output)
	
	return(result)

}

load_models_s3 <- function(s3_model_locations = s3_model_locations, environment = environment) {
	model_objects_split <- gregexpr("/", s3_model_locations)
	model_objects_split <- ldply(lapply(model_objects_split, function(x){x[length(x)]}), c())
	model_objects <- substr(s3_model_locations, model_objects_split$V1 + 1, nchar(s3_model_locations))	
	model_buckets <- substr(s3_model_locations, 1, model_objects_split$V1 -1)
	models <- cbind.data.frame(model_objects, model_buckets)
	models <- split(models, models[,1])
	for (i in 1:length(models)) {
		assign('tmp.env', new.env())
		s3load(models[[i]]$model_objects, bucket = models[[i]]$model_buckets, key = 'AKIAIQLA6ZMA2B4SV7NQ', secret = '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', envir = tmp.env)
		environment[[gsub('.rda', '', models[[i]]$model_objects)]] <- tmp.env[[ls(tmp.env)]]
		}
	tmp.env <- NULL
	print(ls(environment))
}


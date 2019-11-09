
load_model_transforms <- function(model_transforms_table, environment) {
	model_transforms <- split(model_transforms_table, model_transforms_table$transform_type)
	for (t in 1:length(model_transforms)){
		if (names(model_transforms[t]) == 'MAD Winsor') {
			mad_winsor_data <- ldply(lapply(model_transforms[[t]]$transform_data, FUN = fromJSON), rbind)
			for (w in 1:nrow(mad_winsor_data)){
				assign(paste(mad_winsor_data$varname[w], '_win', sep = ''), mad_winsor_data$cutoff[w], envir = environment)
			}
			assign('mad_winsor_data', mad_winsor_data, envir = environment)
		}

		else if (names(model_transforms[t]) == 'Topic Prevalence Scaling') {
			tp_scaling_table <- fromJSON(model_transforms[[t]]$transform_data)
			assign('tp_scaling_table', tp_scaling_table, envir = environment)
		}

		else if (names(model_transforms[t]) == 'Kfit Centroids') {
			kfit_centroids <- fromJSON(model_transforms[[t]]$transform_data)
			assign('kfit_centroids', kfit_centroids, envir = environment)
		}

		else if (names(model_transforms[t]) == 'Contact SF Scaling') {
			contact_sf_scaling_table <- fromJSON(model_transforms[[t]]$transform_data)
			assign('contact_sf_scaling_table', contact_sf_scaling_table, envir = environment)
		}
	}
}
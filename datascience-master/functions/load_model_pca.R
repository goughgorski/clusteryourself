
load_model_pca <- function(pca_table, split, environment) {
	pca_data <- split(pca_table, pca_table[,split])
	pca_data <- lapply(pca_data, function(x) { denormalize_data(x, 'term', 'column_name', 'value', sort = FALSE)})
	lapply(seq_along(pca_data), function(x) {assign(names(pca_data[x]), pca_data[[x]], envir = transforms.env)})
}
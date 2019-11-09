
all.object.sizes <- function(environment = NULL, unit = 'GB') {
	if(is.null(environment)){
		sort(sapply(ls(parent.frame()), function(x) format(object.size(get(x)), unit = unit)))
		} else {
		sort(sapply(ls(environment), function(x) format(object.size(get(x)), unit = unit)))
		}
	}
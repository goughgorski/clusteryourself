closest_cluster <- function(x, centers) {
	  cluster_dist <- apply(centers, 1, function(y) sqrt(sum((x-y)^2)))
	  return(which.min(cluster_dist)[1])
	}
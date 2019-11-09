mad_winsor <- function(x, multiple = 3, na.rm = TRUE, stats = FALSE) {
	if(length(multiple) != 1 | multiple <= 0) {
      stop("bad value for 'multiple'")
   	  }
   median <- median(x, na.rm = na.rm)
   print(paste("Median: ", median))
   cutoff <-  (mad(x, na.rm = na.rm) * multiple) + median
   print(paste("ABS Cutoff: ", cutoff))
   x[ x > cutoff ] <- cutoff
   x[ x < -cutoff ] <- -cutoff
   
   if (stats == TRUE) {
   	stats <- data.frame(median = median, multiple = multiple, cutoff = cutoff)
   	x <- list(output = x, stats = stats) 	
   }
   return(x)
}

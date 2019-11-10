#Threshold Iteration Function
thresh_iter <- function(min, max, by, data, pred, outcome, keep = TRUE, maxstat = NULL) {
  #Loop over thresholds and output the two class stats 
  for (i in seq(from = min, to = max, by = by)) {
    #In iteration 1 create and store output
    if (i == min) {
      out <- two_class_stats(data,pred,outcome,i)
    } else {
      #In iteration >1 output to temp 
      out_temp <- two_class_stats(data,pred,outcome,i)
      #If keep is set we want all output, so rbind
      if (keep == TRUE) {
        out <- rbind(out, out_temp) 
        #If we're at the end, compute AUC
        if (i == max) {
          #Compute AUC
          out <- out[order(out$threshold, decreasing=TRUE),]
          dFPR <- c(diff(out$false_positive_rate), 0)
          dTPR <- c(diff(out$sensitivity), 0)
          auc <- sum(out$sensitivity * dFPR) + sum(dTPR * dFPR)/2
          auc <- rep(auc,nrow(out))
          out <- cbind(out, auc)
        }
      } else{
        #If keep is set to FALSE, kep the record with the maxstat
        if (out[,maxstat]<out_temp[,maxstat]) {
          out <- out_temp
        }
      }
    }
  }
  return(out)
}

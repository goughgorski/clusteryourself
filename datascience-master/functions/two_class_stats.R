#Two Class Stats Function
#Add custom beta F to processing

two_class_stats <- function(data, pred, outcome, threshold, na.rm = TRUE, custom_F_beta = NULL) {
  cat('\r',threshold)
  data$pred_class <- ifelse(data[,pred]>=threshold,1,0)
  sensitivity <- sum(data[data$pred_class == data[,outcome] & data[,outcome] == 1,outcome], na.rm = na.rm)/sum(data[,outcome], na.rm = na.rm)
  specificity <- sum(1-data[data$pred_class == data[,outcome] & data[,outcome] == 0,outcome], na.rm = na.rm)/sum(1-data[,outcome], na.rm = na.rm)
  accuracy <- sum(ifelse(data$pred_class == data[,outcome],1,0), na.rm = na.rm)/sum(!is.na(data[,outcome]), na.rm = na.rm)
  inverse_distance <- 1-(sqrt((1-sensitivity)^2 + (1-specificity)^2))
  positive_pred_val <- sum(data[data$pred_class == data[,outcome] & data[,outcome] == 1,outcome], na.rm = na.rm)/sum(data$pred_class, na.rm = na.rm)
  negative_pred_val <- sum(1-data[data$pred_class == data[,outcome] & data[,outcome] == 0,outcome], na.rm = na.rm)/sum(1-data$pred_class, na.rm = na.rm)
  false_positive_rate <- 1-specificity
  false_negative_rate <- 1-sensitivity
  positive_likelihood_ratio <- sensitivity/false_positive_rate
  negative_likelihood_ratio <- false_negative_rate/specificity
  f1 <- (1+1^2)*((sensitivity*positive_pred_val)/((1^2)*sensitivity+positive_pred_val))
  f2 <- (1+2^2)*((sensitivity*positive_pred_val)/((2^2)*sensitivity+positive_pred_val))
  fp5 <- (1+.5^2)*((sensitivity*positive_pred_val)/((.5^2)*sensitivity+positive_pred_val))
  output <- cbind.data.frame(threshold,accuracy,sensitivity,specificity,positive_pred_val,negative_pred_val,false_positive_rate,false_negative_rate,positive_likelihood_ratio,negative_likelihood_ratio,inverse_distance,f1,f2,fp5)
  return(output)
}
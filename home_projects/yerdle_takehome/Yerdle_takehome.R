install.packages('rJava')
install.packages('RJDBC')
install.packages('plyr')
install.packages('dplyr')
install.packages('pscl')
install.packages('reshape2')

library(rJava)
library(RJDBC)
library(plyr)
library(dplyr)
library(pscl)
library(reshape2)

URL <- 'https://s3.amazonaws.com/athena-downloads/drivers/AthenaJDBC41-1.1.0.jar'
fil <- basename(URL)
if (!file.exists(fil)) download.file(URL, fil)
drv <- JDBC(driverClass="com.amazonaws.athena.jdbc.AthenaDriver", fil, identifier.quote="'")
con <- jdbcConnection <- dbConnect(drv, 'jdbc:awsathena://athena.us-west-1.amazonaws.com:443/',
                                   s3_staging_dir="s3://clusteryourself-home-projects.amazonaws.com",
                                   user=Sys.getenv("ATHENA_ACCESS_KEY_ID"),
                                   password=Sys.getenv("ATHENA_SECRET_ACCESS_KEY"))

setMethod("dbGetQuery", signature(conn="JDBCConnection", statement="character"),  def=function(conn, statement, ...) {
  r <- dbSendQuery(conn, statement, ...)
  on.exit(.jcall(r@stat, "V", "close"))
  if (conn@jc %instanceof% "com.amazonaws.athena.jdbc.AthenaConnection") fetch(r, -1, 999) # Athena can only pull 999 rows at a time
  else fetch(r, -1)
})

items <- dbGetQuery(con, "SELECT * FROM takehome.yerdle_item_data")
views <- dbGetQuery(con, "SELECT * FROM takehome.yerdle_pageviews")

#descriptive statistics/data exploration
plot(items$msrp_new, items$used_list_price)
table(items$used_condition) #100% OK
table(items$category) #23 categories: Appliances, Bags, etc.
table(items$department) #7 departments: Furniture, Kids Furniture, etc.
table(is.na(items$last_known_retail_price_new))/nrow(items) #NA for 55%
table(items$first_ordered_date == '')/nrow(items) #31% not sold

#variable transformation
factor_vars <- c('used_condition', 'department', 'category', 'color')
for (i in 1:length(factor_vars)) {items[, factor_vars[i]] <- as.factor(items[, factor_vars[i]])}
items$first_ordered_date <- ifelse(items$first_ordered_date == '', NA, items$first_ordered_date)
items$first_approved_date <- as.Date(items$first_approved_date)
items$first_ordered_date <- as.Date(items$first_ordered_date)
items$furniture_flg <- ifelse(items$department %in% c('Furniture', 'Kids Furniture', 'Outdoor Furniture'), 1, 0)
items$kids_flg <- ifelse(items$department %in% c('Kids Furniture', 'Kids Storage'), 1, 0)
items$outdoor_flg <- ifelse(items$department %in% c('Outdoor Furniture', 'Outdoor Storage'), 1, 0)
items$storage_flg <- ifelse(items$department %in% c('Kids Storage', 'Outdoor Storage', 'Storage'), 1, 0)
items$kitchen_flg <- ifelse(items$department == 'Kitchen and Dining', 1, 0)

#new variable creation
items$perc_msrp <- items$used_list_price/items$msrp_new
items$days_on_market <- items$first_ordered_date - items$first_approved_date
items$days_on_market <- ifelse(is.na(items$days_on_market), as.Date('2018-11-01') -
                                 items$first_approved_date, items$days_on_market)
items$sold <- ifelse(is.na(items$first_ordered_date), 0, 1)
items$discount <- ifelse(is.na(items$last_known_retail_price_new), 0, 1)
views$pp_scaled <- scale(views$pageviews)
items <- merge(items, views[, c('item_parent_sku', 'pageviews')], by = 'item_parent_sku', all.x = T)

#LHS1 (can come back and divide days_on_market by escalating value to decrease discount)
d <- 1
items$lhs1 <- (1/((items$days_on_market/d) + 1)) * items$used_list_price * items$sold

#define test, train, validation sets
sets <- data.frame(unique_item_id = unique(items$unique_item_id), rand = runif(length(unique(items$unique_item_id))))
sets$set <- ifelse(sets$rand < 0.7, 'train', ifelse(sets$rand >= 0.9, 'validation', 'test'))
items <- merge(items, sets[, c('unique_item_id', 'set')], by = 'unique_item_id')

#scale pageviews
pp_mean <- mean(items$pageviews)

# zero-inflated negative binomial regression model with department flags
lhs1_zeroinfl <- zeroinfl(round(lhs1*100) ~ 
                            perc_msrp + discount + pageviews + furniture_flg +
                            kids_flg + outdoor_flg + storage_flg
                          , data = items[items$set == 'train',]
                          , dist = 'negbin', EM = TRUE, na.action = na.omit)

#model evaluation
train <- items$set == 'train' & complete.cases(items[, colnames(count_coef_mat)[2:length(colnames(count_coef_mat))]])
model_performance <- data.frame(pred = lhs1_zeroinfl$fitted[train],
                                outcome_flg = items$sold[train],
                                outcome = items$lhs1[train])
model_performance <- model_performance[!is.na(model_performance$pred),]

#### thresh iterations functions ####
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

#### end ####

model_thresh <- thresh_iter(0, 3000, 1, model_performance, 'pred', 'outcome_flg')

model_thresh[which.max(model_thresh$f2),]

# performance on test data
count_coef_mat <- t(as.data.frame(lhs1_zeroinfl$coefficients$count))
test <- items$set == 'test' & complete.cases(items[, colnames(count_coef_mat)[2:length(colnames(count_coef_mat))]])
count_pred <- as.matrix(data.frame(Intercept = 1, items[test, colnames(count_coef_mat)[2:ncol(count_coef_mat)]]))
head(count_pred)

count_pred <- count_pred %*% lhs1_zeroinfl$coefficients$count
zero_coef_mat <- t(as.data.frame(lhs1_zeroinfl$coefficients$zero))
zero_pred <- as.matrix(data.frame(Intercept = 1, items[test, colnames(count_coef_mat)[2:ncol(count_coef_mat)]]))
zero_pred <- zero_pred %*% lhs1_zeroinfl$coefficients$zero

pzero <- exp(zero_pred)/(1+exp(zero_pred))
pcount <- exp(count_pred)*(1-pzero)
items$pred_lhs1[test] <- pcount/100

model_performance <- data.frame(pred = items$pred_lhs1[test],
                                outcome_flg = items$sold[test],
                                outcome = items$lhs1[test])
model_performance <- model_performance[!is.na(model_performance$pred),]

model_thresh <- thresh_iter(0, 3000, 1, model_performance, 'pred', 'outcome_flg')

model_thresh[which.max(model_thresh$f2),]

#analyze for systematic underselling
items$resid <- NA
items$resid[items$sold == 1] <- items$pred_lhs1[items$sold == 1] - items$lhs1[items$sold == 1]

#matrix of residuals
for (i in seq(from = floor(min(items$pp_scaled[test])), 
              to = max(items$pp_scaled[test]), by = 2)) {
  if (i == floor(min(items$pp_scaled[test]))) {
    z <- test & items$pp_scaled > i & items$pp_scaled <= i + 2
    pp <- as.numeric(by(items$resid[z], items$department[z], function(x) {mean(x, na.rm = T)}))
    ct <- as.numeric(by(items$resid[z], items$department[z], length))
                  mat <- data.frame(min = i, max = i + 2, vws_mean = pp, ct = ct,
                                    department = levels(items$department))}
  else {
    z <- test & items$pp_scaled > i & items$pp_scaled <= i + 2
    pp <- as.numeric(by(items$resid[z], items$department[z], function(x) {mean(x, na.rm = T)}))
    ct <- as.numeric(by(items$resid[z], items$department[z], length))
        tmp <- data.frame(min = i, max = i + 2, vws_mean = pp, ct = ct, department = levels(items$department))
        mat <- rbind(mat, tmp)
  }}
mat <- mat[!is.na(mat$ct) & mat$ct > 30,]

views_depts_lhs1 <- dcast(mat, min + max ~ department, value.var = 'ct')

by <- 0.1

for (i in seq(from = floor(min(items$perc_msrp[test])), 
              to = max(items$perc_msrp[test]), by = by)) {
  if (i == floor(min(items$perc_msrp[test]))) {
    z <- test & items$perc_msrp > i & items$perc_msrp <= i + by
    pp <- as.numeric(by(items$resid[z], items$department[z], function(x) {mean(x, na.rm = T)}))
    ct <- as.numeric(by(items$resid[z], items$department[z], length))
    mat <- data.frame(min = i, max = i + by, vws_mean = pp, ct = ct,
                      department = levels(items$department))}
  else {
    z <- test & items$perc_msrp > i & items$perc_msrp <= i + by
    pp <- as.numeric(by(items$resid[z], items$department[z], function(x) {mean(x, na.rm = T)}))
    ct <- as.numeric(by(items$resid[z], items$department[z], length))
    tmp <- data.frame(min = i, max = i + by, vws_mean = pp, ct = ct, department = levels(items$department))
    mat <- rbind(mat, tmp)
  }}


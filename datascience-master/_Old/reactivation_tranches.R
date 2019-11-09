reactivation_tranches <- function(data, month.min = 3, active.var = NULL, lag.var = NULL, lags = 3, tranche.min = 526){
  
if(is.null(lag.var)){stop(noquote("No lag variables provided"))}
  
if(!lags %in% c(1:4)){stop(noquote("Number of lags must be between 1 and 4"))}

umon <- data
   
print(noquote("Processing user event count data"))
 
# subsetting only users with minimum number of months
tmp <- aggregate(umon$month, by = list(umon$external_account_id), 'max')
colnames(tmp) <- c('external_account_id', 'month')
tmp <- tmp[tmp$month >= month.min,]
umon <- umon[umon$external_account_id %in% tmp[, c('external_account_id')],]
  
# empty data frame of rows = n users, columns = n months
mths <- setNames(data.frame(matrix(ncol = length(unique(umon$month)), nrow = nrow(umon))), 
                 paste('mth_', unique(umon$month), sep=""))

# merge with user month data
umon <- cbind(umon, mths)
cols <- colnames(umon)
# populate new columns with binary based on the row's month value
umon <- data.frame(t(apply(umon, 1, function(x){
  mth <- as.numeric(x['month'])
  x <- t(x)
  i <- grep('mth_', colnames(x))
  x[i] <- ifelse(gsub("mth_", "", colnames(x))==as.numeric(mth), 1, 0)[i]
  return(x)
})))
colnames(umon) <- cols
i <- sapply(umon,is.factor)
umon[i] <- lapply(umon[i], as.character)
i <- which(!colnames(umon) %in% c('start_date', 'type'))
umon[,i] <- lapply(umon[,i], as.numeric)

# creating binary active variable based on active.var argument
print(noquote(paste("Creating binary active variable based on ", paste(active.var, sep = ' ', collapse = ', '), sep = '')))
umon$active <- ifelse(eval(parse(text = paste('umon$', active.var, ' == 0', sep = '', collapse = ' & '))), 0, 1)

# removing and storing current (incomplete) month
current_month <- aggregate(umon$month, list(umon$external_account_id), FUN = 'max')
colnames(current_month) <- c('external_account_id', 'month')
current_month <- merge(current_month, umon, by = c('external_account_id', 'month'))
umon <- anti_join(umon, current_month, by = c('external_account_id', 'month'))

# creating dataset with lags
print(noquote("Creating lag variables"))
# y side
ylag <- umon[umon$month!=0,  which(!colnames(umon) %in% lag.var)]
# x side
xlag <- umon[, c('external_account_id', 'month', lag.var)]
xlag$month <- xlag$month + 1
colnames(xlag)[which(colnames(xlag) %in% lag.var)] <- paste(colnames(xlag)[which(colnames(xlag) %in% lag.var)], '_1', sep='')
# merge
umon.lag <- merge(xlag, ylag, by = c('external_account_id', 'month'))

if(lags > 1){
# second lag
xlag <- umon[, c('external_account_id', 'month', lag.var)]
xlag$month <- xlag$month + 2
colnames(xlag)[which(colnames(xlag) %in% lag.var)] <- paste(colnames(xlag)[which(colnames(xlag) %in% lag.var)], '_2', sep='')
# merge
umon.lag <- merge(xlag, umon.lag, by = c('external_account_id', 'month'))}

if(lags > 2){
# third lag
xlag <- umon[, c('external_account_id', 'month', lag.var)]
xlag$month <- xlag$month + 3
colnames(xlag)[which(colnames(xlag) %in% lag.var)] <- paste(colnames(xlag)[which(colnames(xlag) %in% lag.var)], '_3', sep='')
# merge
umon.lag <- merge(xlag, umon.lag, by = c('external_account_id', 'month'))}

if(lags > 3){
  # fourth lag
  xlag <- umon[, c('external_account_id', 'month', lag.var)]
  xlag$month <- xlag$month + 4
  colnames(xlag)[which(colnames(xlag) %in% lag.var)] <- paste(colnames(xlag)[which(colnames(xlag) %in% lag.var)], '_4', sep='')
  # merge
  umon.lag <- merge(xlag, umon.lag, by = c('external_account_id', 'month'))}


# reconcile signup_method
umon.lag <- umon.lag[umon.lag$signup_method %in% c(2,4,5,6),]
umon.lag$signup_method[umon.lag$signup_method == 2] <- 6

umon.lag$signup_chrome_ext <- ifelse(umon.lag$signup_method == 4, 1, 0)
umon.lag$signup_mobile <- ifelse(umon.lag$signup_method == 5, 1, 0)
umon.lag$signup_desktop <- ifelse(umon.lag$signup_method == 6, 1, 0)
umon.lag$signup_method <- as.factor(umon.lag$signup_method)

# removing and storing most recent completed month
last_month <- aggregate(umon.lag$month, list(umon.lag$external_account_id), FUN = 'max')
colnames(last_month) <- c('external_account_id', 'month')
last_month <- merge(last_month, umon.lag, by = c('external_account_id', 'month'))
umon.lag <- anti_join(umon.lag, last_month, by = c('external_account_id', 'month'))

## ggplot output
#mean.data <- aggregate(umon.lag$active.y, by=list(umon.lag$month, umon.lag$signup_method, umon.lag$subscribed), mean)
#colnames(mean.data) <- c('month', 'signup_method', 'subscribed', 'prob_active')
#mean.data$signup_method <- as.character(mean.data$signup_method)
#mean.data <- mean.data[mean.data$signup_method %in% c(4,5,6),]
#mean.data$signup_method[mean.data$signup_method == 4 & mean.data$subscribed == 0] <- 'not subscribed: chrome_ext'
#mean.data$signup_method[mean.data$signup_method == 5 & mean.data$subscribed == 0] <- 'not subscribed: mobile'
#mean.data$signup_method[mean.data$signup_method == 6 & mean.data$subscribed == 0] <- 'not subscribed: desktop'
#mean.data$signup_method[mean.data$signup_method == 4 & mean.data$subscribed == 1] <- 'subscribed: chrome_ext'
#mean.data$signup_method[mean.data$signup_method == 5 & mean.data$subscribed == 1] <- 'subscribed: mobile'
#mean.data$signup_method[mean.data$signup_method == 6 & mean.data$subscribed == 1] <- 'subscribed: desktop'
#mean.data$signup_method <- as.factor(mean.data$signup_method)
#ggplot(mean.data[mean.data$month < 6,], aes(x=month, y=prob_active, colour=signup_method)) + geom_point() + geom_line()

#mean.data <- aggregate(umon$active, by=list(umon$month, umon$signup_method), mean)
#colnames(mean.data) <- c('month', 'signup_method', 'prob_active')
#mean.data$signup_method <- as.character(mean.data$signup_method)
#mean.data <- mean.data[mean.data$signup_method %in% c(4,5,6),]
#mean.data$signup_method[mean.data$signup_method == 4] <- 'chrome_ext'
#mean.data$signup_method[mean.data$signup_method == 5] <- 'mobile'
#mean.data$signup_method[mean.data$signup_method == 6] <- 'desktop'
#mean.data$signup_method <- as.factor(mean.data$signup_method)
#ggplot(mean.data[mean.data$signup_method!='mobile',], aes(x=month, y=prob_active, colour=signup_method)) + geom_point() + geom_line() + ylim(0, 1)


#mean.data <- aggregate(umon$active, by=list(umon$month, umon$subscribed), mean)
#colnames(mean.data) <- c('month', 'subscribed', 'prob_active')
#mean.data$subscribed[mean.data$subscribed == 1] <- 'subscribed'
#mean.data$subscribed[mean.data$subscribed == 0] <- 'not subscribed'
#mean.data$subscribed <- as.factor(mean.data$subscribed)
#ggplot(mean.data, aes(x=month, y=prob_active, colour=subscribed)) + geom_point() + geom_line() + ylim(0, 1) + scale_color_brewer(palette="Paired")

# compile formula and run model
print(noquote("Calculating logit model"))

no.rhs <- c('external_account_id', 'month', 'active', 'start_date', 'signup_method', 'signup_chrome_ext')

formula <- 
  as.formula(paste('active ~', paste(
  colnames(umon.lag[which(!colnames(umon.lag)
    %in% no.rhs)])[apply(umon.lag[which(!colnames(umon.lag) %in% no.rhs)], 2,
           function(x) {length(unique(x)) >1})], collapse = ' + '), '-1'))

# full model with lag data
logit_user_event_count <- suppressWarnings((glm(formula, family = binomial(link = 'logit'), data = umon.lag)))

print(noquote("Evaluating model strength"))
diff <- cbind(umon.lag, fitted = lag.logit$fitted.values)
diff <- cbind(diff, residuals = lag.logit$residuals)

tmp <- aggregate(diff$month, by = list(diff$external_account_id), 'max')
colnames(tmp) <- c('external_account_id', 'month')
diff <- merge(diff, tmp, by = c('external_account_id', 'month'))

#two_class_stats(diff, 'fitted', 'active', .5)
tmp <- thresh_iter(0, 1, 0.01, diff, 'fitted', 'active')
best.thresh <- tmp$threshold[which.max(tmp$inverse_distance)]
print(noquote(paste('Model AUC: ', round(unique(tmp$auc), digits = 4), sep = '')))

# using model estimates to create fitted values for last_month
print(noquote("Generating predicted probabilities for most recent completed month"))
estimates <- as.data.frame(t(lag.logit$coefficients[!is.na(lag.logit$coefficients)]))
last_month$fitted <- suppressWarnings(predict.glm(lag.logit, last_month, type = 'response'))

# separating into tranches
print(noquote("Separating recently inactive users into tranches"))
intv <- last_month[last_month$active == 0 & last_month$subscribed == 0 & last_month$fitted >= best.thresh,]
# removing users active in the current month
intv <- intv[!intv$external_account_id %in% current_month$external_account_id[current_month$active == 1],]
t1 <- sort(intv$fitted, decreasing = T)[tranche.min]
intv$tranche[intv$fitted >= t1] <- 1
t2 <- t1 - (((1-t1)/(1-best.thresh)) * 0.25)
while (nrow(intv[intv$fitted < t1 & intv$fitted >= t2,]) <= tranche.min){
  t2 <- t2 - 0.01}
intv$tranche[intv$fitted < t1 & intv$fitted >= t2] <- 2
t3 <- t2 - (((1-t1)/(1-best.thresh)) * 0.25)
while (nrow(intv[intv$fitted < t2 & intv$fitted >= t3,]) <= tranche.min){
  t3 <- t3 - 0.01}
intv$tranche[intv$fitted < t2 & intv$fitted >= t3] <- 3
intv$tranche[intv$fitted < t3] <- 4

print(noquote('Tranche n-size:'))
print(table(intv$tranche))

print(noquote('Assigning treatment'))
intv$treatment <- sample(0:1, nrow(intv), replace = T)
intv <- intv[, c('external_account_id', 'fitted', 'tranche', 'treatment')]

print(noquote('Storing model and threshold iterations'))
model_storage(model = logit_user_event_count, location_folder = "reactivation", model_script = "Reactivation_lag_logit.R",
strip_model_fn = strip_model, model_grain = "user", model_response = "Binary", model_outcome = "binary", model_type = "Logistic Regression",
model_test = diff, model_test_cols = c('fitted', 'active')) 


return(tranches = intv)}

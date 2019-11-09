
user_time_interval_table <- function(connection, active.var = NULL, time_int_limit = NULL, time_int = 'week',
  time_int_dummies = FALSE, lags = 1, lag.var = NULL, tx_rej_rate = TRUE, cl_rej_rate = TRUE, write_to_db = NULL,
  write_to_file = NULL){

#Check for necessary arguments
if(is.null(time_int)){stop(noquote("time_int can not be null, must be specified"))}

if(lags >0 & is.null(lag.var)){stop(noquote("no lag variables provided"))}
  
if(!lags %in% c(0:4)){stop(noquote("number of lags must be between 0 and 4"))}

if(!is.null(write_to_db)){
  if(time_int_dummies == T){stop(noquote("can not write time interval dummy columns to database"))}
  if(dbExistsTable(connection, write_to_db) == F){stop(noquote(paste(paste(write_to_db, collapse = '.'),"is not an existing table")))}}

if(is.null(write_to_db) & is.null(write_to_file)){
  stop(noquote("no output destination specified"))}

#Describe dataset to be generated
print(noquote(paste("user-", time_int, " dataset to be generated", sep = '')))
print(noquote("will include:"))

if(is.null(time_int_limit)){print(noquote("* all available historical date (no time constraint)"))} else
{print(noquote(paste("* all available data from the previous ", time_int_limit, " ", time_int, "s", sep = '')))}

if(!is.null(active.var)){print(noquote(paste("* binary active variable based on", paste(active.var, collapse = ', '))))}

if(tx_rej_rate == T){print(noquote("* tx_rej_rate cumulative transaction rejection rate variable"))}

if(cl_rej_rate == T){print(noquote("* cl_rej_rate cumulative client rejection rate variable"))}

if(lags>0){print(noquote(paste("* ", lags, " lag variables of ", lag.var, sep = '')))
  if(!is.null(active.var)){print(noquote(paste("* ", lags, " lag variables of active", sep = '')))}
  if(tx_rej_rate == T){print(noquote(paste("* ", lags, " lag variables of tx_rej_rate", sep = '')))}
  if(cl_rej_rate == T){print(noquote(paste("* ", lags, " lag variables of cl_rej_rate", sep = '')))}
}

if(!is.null(write_to_db)){print(noquote(paste("data will be written to", paste(write_to_db, collapse = '.'), "table")))}

if(!is.null(write_to_file)){print(noquote(".csv of data will be written to ~/Public/"))}

#Connect to Local PSQL Database and Pull Data
print(noquote("Connecting to PostgreSQL Database"))

data <- dbGetQuery(connection, paste("SELECT * from datasci_testing.users_", time_int, "_dataset;", sep = ""))

if (nrow(data) == 0){stop(noquote("no user time interval data pulled"))}

#Check for necessary columns in data
if(!is.null(active.var)){ 
  if(any(!active.var %in% colnames(data))){
    stop(noquote(paste("active.var ", paste(active.var[!active.var %in% colnames(data)], collapse = ', '), " not found in data", sep = '')))
    }}

if(!is.null(lag.var)){ 
  if(any(!lag.var %in% colnames(data))){
    stop(noquote(paste("lag.var ", paste(lag.var[!lag.var %in% colnames(data)], collapse = ', '), " not found in data", sep = '')))
    }}

if(tx_rej_rate == T & any(!c('external_account_id', 'rejected', 'confirmed') %in% colnames(data))){
  stop(noquote(paste(paste(c('external_account_id', 'rejected', 'confirmed')[!c('external_account_id', 'rejected', 'confirmed') %in% colnames(data)], collapse = ', '),
    " missing from data and needed to calculate cumulative transaction rejection rate", sep = '')))
}

if(cl_rej_rate == T & any(!c('external_account_id', 'cl_rejected', 'cl_confirmed') %in% colnames(data))){
  stop(noquote(paste(paste(c('external_account_id', 'cl_rejected', 'cl_confirmed')[!c('external_account_id', 'cl_rejected', 'cl_confirmed') %in% colnames(data)], collapse = ', '),
    " missing from data and needed to calculate cumulative client rejection rate", sep = '')))
}

uti <- data

#Apply time interval limit
if(!is.null(time_int_limit)){
  if(time_int == 'week'){min_date <- as.Date(today()) - (7 * time_int_limit)}
  if(time_int == 'month'){min_date <- as.Date(today()) %m-% months(time_int_limit)}
  uti <- uti[uti$start_interval_date >= min_date,]
}

print(noquote("Processing user event count data"))

if(time_int_dummies == T){
  print(noquote("Adding time interval dummy columns to data"))
  # empty data frame of rows = n users, columns = n time intervals
  time_ints <- setNames(data.frame(matrix(ncol = length(unique(uti$time_int)), nrow = nrow(uti))), 
                   paste('ti_', unique(uti$time_int), sep=""))

  # merge with user week data
  uti <- cbind(uti, time_ints)
  cols <- colnames(uti)
  # populate new columns with binary based on the row's week value
  uti <- data.frame(t(apply(uti, 1, function(x){
    ti <- as.numeric(x['time_int'])
    x <- t(x)
    i <- grep('ti_', colnames(x))
    x[i] <- ifelse(gsub("ti_", "", colnames(x))==as.numeric(ti), 1, 0)[i]
    return(x)
  })))
  colnames(uti) <- cols
  i <- sapply(uti,is.factor)
  uti[i] <- lapply(uti[i], as.character)
  i <- which(!colnames(uti) %in% c('start_interval_date', 'end_interval_date', 'signup_method'))
  uti[,i] <- lapply(uti[,i], as.numeric)
}


# creating binary active variable based on active.var argument
if(!is.null(active.var)){ 
  print(noquote(paste("Creating binary active variable based on ", paste(active.var, sep = ' ', collapse = ', '), sep = '')))
  uti$active <- ifelse(eval(parse(text = paste('uti$', active.var, ' == 0', sep = '', collapse = ' & '))), 0, 1)
}

# calculate cumulative transaction rejection rate
if(tx_rej_rate == T){
  print(noquote("Calculating user level cumulative transaction rejection rate"))
  cum_rr <- ldply(lapply(split(uti[uti$start_interval_date > '2017-11-21', ],
  uti[uti$start_interval_date > '2017-11-21', 'external_account_id']), function(x){
    tmp <- cumsum(x[, 'rejected'])/cumsum(x[, 'rejected'] + x[, 'confirmed'])
    tmp[is.na(tmp)] <- 0
    tmp <- as.data.frame(tmp)
    colnames(tmp) <- 'tx_rej_rate'
    tmp$time_int <- x[, 'time_int']
    tmp$external_account_id <- x[, 'external_account_id']
    return(tmp)
  }), rbind)


  uti <- merge(uti, cum_rr[, c('external_account_id', 'time_int', 'tx_rej_rate')], by = c('external_account_id', 'time_int'), all.x = T, sort = F)

  uti$tx_rej_rate[is.na(uti$tx_rej_rate)] <- 0
}

# calculate cumulative client rejection rate
if(cl_rej_rate == T){
  print(noquote("Calculating user level cumulative client rejection rate"))
  cum_rr <- ldply(lapply(split(uti[uti$start_interval_date > '2018-08-07', ],
  uti[uti$start_interval_date > '2018-08-07', 'external_account_id']), function(x){
    tmp <- cumsum(x[, 'cl_rejected'])/cumsum(x[, 'cl_rejected'] + x[, 'cl_confirmed'])
    tmp[is.na(tmp)] <- 0
    tmp <- as.data.frame(tmp)
    colnames(tmp) <- 'cl_rej_rate'
    tmp$time_int <- x[, 'time_int']
    tmp$external_account_id <- x[, 'external_account_id']
    return(tmp)
  }), rbind)


  uti <- merge(uti, cum_rr[, c('external_account_id', 'time_int', 'cl_rej_rate')], by = c('external_account_id', 'time_int'), all.x = T, sort = F)

  uti$cl_rej_rate[is.na(uti$cl_rej_rate)] <- 0
}

if(lags == 0) {uti_out <- uti}

# creating dataset with lags
if(lags > 0){
  print(noquote("Creating lag variables"))
  # add active, tx_rej_rate, cl_rej_rate to lag.var
  if(!is.null(active.var)){lag.var <- c(lag.var, 'active')}
  if(tx_rej_rate == T){lag.var <- c(lag.var, 'tx_rej_rate')}
  if(cl_rej_rate == T){lag.var <- c(lag.var, 'cl_rej_rate')}
  # y side
  ylag <- uti[uti$time_int!=0,  which(!colnames(uti) %in% lag.var)]
  # x side
  xlag <- uti[, c('external_account_id', 'time_int', lag.var)]
  xlag$time_int <- xlag$time_int + 1
  colnames(xlag)[which(colnames(xlag) %in% lag.var)] <- paste(colnames(xlag)[which(colnames(xlag) %in% lag.var)], '_1', sep='')
  # merge
  uti.lag <- merge(xlag, ylag, by = c('external_account_id', 'time_int'))
  uti_out <- merge(uti, uti.lag[, c('external_account_id', 'time_int',
  colnames(uti.lag)[grep(paste(lag.var, collapse = '|'), colnames(uti.lag))])], by = c('external_account_id', 'time_int'), all.x = T)
}

if(lags > 1){
  # second lag
  xlag <- uti[, c('external_account_id', 'time_int', lag.var)]
  xlag$time_int <- xlag$time_int + 2
  colnames(xlag)[which(colnames(xlag) %in% lag.var)] <- paste(colnames(xlag)[which(colnames(xlag) %in% lag.var)], '_2', sep='')
  # merge
  uti.lag <- merge(xlag, uti.lag, by = c('external_account_id', 'time_int'))
  uti_out <- merge(uti_out, uti.lag[, c('external_account_id', 'time_int',
     colnames(uti.lag)[grep(paste(paste(lag.var, '_2', sep = ''), collapse = '|'), colnames(uti.lag))])], 
    by = c('external_account_id', 'time_int'), all.x = T)
}

if(lags > 2){
  # third lag
  xlag <- uti[, c('external_account_id', 'time_int', lag.var)]
  xlag$time_int <- xlag$time_int + 3
  colnames(xlag)[which(colnames(xlag) %in% lag.var)] <- paste(colnames(xlag)[which(colnames(xlag) %in% lag.var)], '_3', sep='')
  # merge
  uti.lag <- merge(xlag, uti.lag, by = c('external_account_id', 'time_int'))
  uti_out <- merge(uti_out, uti.lag[, c('external_account_id', 'time_int',
     colnames(uti.lag)[grep(paste(paste(lag.var, '_3', sep = ''), collapse = '|'), colnames(uti.lag))])], 
    by = c('external_account_id', 'time_int'), all.x = T)
}

if(lags > 3){
  # fourth lag
  xlag <- uti[, c('external_account_id', 'time_int', lag.var)]
  xlag$time_int <- xlag$time_int + 4
  colnames(xlag)[which(colnames(xlag) %in% lag.var)] <- paste(colnames(xlag)[which(colnames(xlag) %in% lag.var)], '_4', sep='')
  # merge
  uti.lag <- merge(xlag, uti.lag, by = c('external_account_id', 'time_int'))
  uti_out <- merge(uti_out, uti.lag[, c('external_account_id', 'time_int',
     colnames(uti.lag)[grep(paste(paste(lag.var, '_4', sep = ''), collapse = '|'), colnames(uti.lag))])], 
    by = c('external_account_id', 'time_int'), all.x = T)
}

#Reconcile signup_method
uti_out$signup_method <- as.numeric(uti_out$signup_method)
uti_out <- uti_out[uti_out$signup_method %in% c(2,4,5,6),]
uti_out$signup_method[uti_out$signup_method == 2] <- 6

uti_out$signup_chrome_ext <- ifelse(uti_out$signup_method == 4, 1, 0)
uti_out$signup_mobile <- ifelse(uti_out$signup_method == 5, 1, 0)
uti_out$signup_desktop <- ifelse(uti_out$signup_method == 6, 1, 0)
uti_out$signup_method <- as.factor(uti_out$signup_method)

#Rename columns
colnames(uti_out)[colnames(uti_out) == 'time_int'] <- time_int
colnames(uti_out)[colnames(uti_out) %in% c('start_interval_date', 'end_interval_date')] <- gsub('interval', time_int, c('start_interval_date', 'end_interval_date'))
if(time_int_dummies == T){
  colnames(uti_out)[grep('ti_', colnames(uti_out))] <- gsub('ti_', paste(substring(time_int, 1, 1), '_', sep = ''), colnames(uti_out)[grep('ti_', colnames(uti_out))])
}


#Truncate table and write to database
if(!is.null(write_to_db)){
  print(noquote(paste("Truncating and rewriting", write_to_db[2], "table")))
  dbExecute(connection, paste("TRUNCATE TABLE", paste(write_to_db, collapse = '.')))

  cols <- unlist(dbGetQuery(connection, paste("SELECT column_name
                FROM information_schema.columns
                WHERE table_name = ", "'", paste(write_to_db[2]), "'",
                " AND table_schema = ", "'", paste(write_to_db[1]), "'",
                " AND column_default IS NULL", sep = '')))
  dbWriteTable(connection, write_to_db, value = uti_out[, cols], overwrite = FALSE, append = TRUE, row.names = FALSE)
  print(noquote(paste(nrow(uti_out), "rows written to", write_to_db[2], "table")))
}

if(!is.null(write_to_file)){
  print(noquote("Writing file to ~/Public/"))
  write.csv(uti_out, paste('~/Public/', write_to_file, '.csv', sep = ''), row.names = FALSE)
  print(noquote(paste(paste(write_to_file, '.csv', sep = ''), "written to ~/Public/")))
}

}


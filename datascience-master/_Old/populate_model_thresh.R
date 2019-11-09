#Clear Environment
#rm(list=ls())

#Load libraries
#source('~/workspace/datascience/functions/load_libraries.R')
#load_libraries('~/workspace/datascience/functions/')

#Set Seed
#set.seed(123456789)

#Connect to Local PSQL Database
#local_connection <- local_db_conect('development')


#Query to get data from PropertyEvents Table
data <- dbGetQuery(local_connection, "SELECT id
                                        , type
                                        , property_transaction_id
                                        , email_classification_score
                                        , two_class
                                        , email_classification_version
                                      FROM datasci.property_transactions_ds
                                      WHERE type IN ('PropertyTransactionConfirmedEvent'
                                                      ,'PropertyTransactionRejectedEvent')")
##Add Iteration over Model Versions Here

#Random Folding
data$rand <- ifelse(!is.na(data$email_classification_score) & !is.na(data$two_class),runif(sum(!is.na(data$email_classification_score)),0,1),NA)
data$rand_fold_cut <- cut(data$rand, 10, labels= FALSE, ordered_result = TRUE)

#By Threshold Computations - Split into folds, and compute statistics incrementing the threshold
split_data <- split(data, data[,'rand_fold_cut'], drop = T)
data_thresholds <- lapply(split_data,function(x){thresh_iter(.0, 1, .01, x, 'email_classification_score', 'two_class')})

#Unlist and add columns
data_thresholds <- ldply(data_thresholds,rbind)
names(data_thresholds)[names(data_thresholds) == ".id"] <- "random_fold_id"    
data_thresholds$rowid <- seq(1,nrow(data_thresholds)) 
data_thresholds$model_id <- 13
data_thresholds$timestamp <- Sys.time()

#Write Model Fit Stats Table
model_fit_thresh_exists <- dbExistsTable(local_connection, c("datasci","model_fit_thresh"))

if (model_fit_thresh_exists) { 
  append = TRUE
  overwrite = FALSE
  colnames <- dbGetQuery(local_connection, "SELECT column_name 
                         FROM information_schema.columns 
                         WHERE table_name = 'model_fit_thresh'"
  )
  
  if (!setequal(colnames(data_thresholds),colnames[,1])) { 
    stop(print('Processed table structure has changed but existing table contains data. Drop table and rerun, or adjust processing to fit existing structure.'))	
    }
  } else {
    append = FALSE
    overwrite = FALSE
}

dbWriteTable(local_connection, c('datasci','model_fit_thresh'), value = data_thresholds, overwrite = overwrite, append = append, row.names = FALSE)


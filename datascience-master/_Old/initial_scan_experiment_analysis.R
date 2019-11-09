rm(list=ls())

#Load libraries
source('~/workspace/datascience/functions/load_libraries.R')
load_libraries('~/workspace/datascience/functions/', include_bundles = c('utility'))

#Set Seed
set.seed(123456789)
connection <- db_connect()

# pull UserSegmentationEvents and scan details
scans <- dbGetQuery(connection, "SELECT * FROM datasci_modeling.lk_user_experiments")

# pull all initial scan events
data <- dbGetQuery(connection, "SELECT external_account_id, ensemble_score, 1 AS shown, created_at
								FROM datasci.property_transactions
								WHERE scan_type = 'initial_scan'
								UNION
								SELECT external_account_id, ensemble_score, 0 AS shown, created_at
								FROM datasci.property_transaction_not_created_events
								WHERE ensemble_name = 'Logit'")

min_date <- min(scans$created_at)

data <- data[data$created_at > min_date,]

# fix ensemble_score scientific notation error
data$ensemble_score[data$ensemble_score > 1] <- 0

# merge in number of found events (shown and not) for each user
n_found <- as.data.frame(table(data$external_account_id))
colnames(n_found) <- c('external_account_id', 'n_found')
n_shown <- as.data.frame(table(data$external_account_id, data$shown))
n_shown <- n_shown[n_shown$Var2 == 1, c('Var1', 'Freq')]
colnames(n_shown) <- c('external_account_id', 'n_shown')
n_found <- merge(n_found, n_shown, by = 'external_account_id')
n_found$n_shown <- ifelse(is.na(n_found$n_shown), 0, n_found$n_shown)
data <- merge(data, n_found, by = 'external_account_id')

# merge in experiment_group
data <- merge(data, scans[, c('external_account_id', 'experiment_group')], by = 'external_account_id')

# collapse to single row per user
data <- ldply(lapply(split(data, data[, 'external_account_id']), function(x) {
	tmp <- x[1,]
	tmp$max_ens_score <- max(x$ensemble_score)
	tmp$min_ens_score <- min(x$ensemble_score)
	tmp$mean_ens_score <- mean(x$ensemble_score)
	}))

# pull first week user data and merge
activity <- dbGetQuery(connection, paste("SELECT * FROM datasci_visualizations.user_week
									WHERE start_week_date >", min_date, ";", sep = "'"))								
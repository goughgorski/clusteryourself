#Clear Environment
rm(list=ls())

#Load libraries
source('~/workspace/datascience/functions/load_libraries.R')
load_libraries('~/workspace/datascience/functions/', include_bundles = c('utility'))

#Set Seed
set.seed(123456789)

#Connect to Local PSQL Database and Pull Data
print(noquote("Connecting to local PostgreSQL Database"))
connection <- db_connect()
data <- dbGetQuery(connection, "SELECT * FROM datasci_modeling.vw_event_counts_user_month")

#Execute reactivation_tranches function
tranches <- reactivation_tranches(data, lag.var = c('confirmed', 'rejected', 'manually_created', 'auto_created', 'shared'),
                             active.var = c('confirmed', 'manually_created', 'shared'))

#Write tranche assignment data frame to datasci_projects.tranche_assignments
dbWriteTable(connection, c('datasci_projects','tranche_assignments'), value = as.data.frame(tranches), overwrite = FALSE, append = TRUE, row.names = FALSE)

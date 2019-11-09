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

#Run reactivation model and tranche assigment function
try(reactivation_tranches(connection))

#Stop if reactivation_tranches function failed to update tranche_assignments table
if(dbGetQuery(connection, "select date_part('day', age(now(), max(created_at))) from datasci_projects.tranche_assignments") > 1){
	stop(noquote("No new tranche_assignments rows found - output data table will not be provided"))}

intv_data <- dbGetQuery(connection, "SELECT a.user_id as intv_user_id
						, a.external_account_id as intv_external_account_id
						, pea.email
						, a.property_transaction_id as intv_property_transaction_id
						, a.street_addr as intv_street_addr
						, to_char(now()::date, 'YYYY-MM-DD') as intv_date
						FROM (
							SELECT dspta.user_id, dspta.external_account_id, dspte.property_transaction_id, street_addr
							, date_part('day', now()::date - dspte.created_at) as age
							, row_number() over(partition by dspta.external_account_id order by date_part('day', now()::date - dspte.created_at))
							, dspta.created_at
							, max(dspta.created_at) over()
							FROM datasci_projects.tranche_assignments dspta
							LEFT JOIN datasci.property_transaction_events dspte on dspte.external_account_id = dspta.external_account_id
							LEFT JOIN datasci.property_transactions dspt on dspt.property_transaction_id = dspte.property_transaction_id
							LEFT JOIN datasci.properties dsp on dsp.id = dspt.property_id
							WHERE dspta.treatment = true
							AND dspta.created_at = (SELECT MAX(created_at) FROM datasci_projects.tranche_assignments)
							AND dspte.type in ('PropertyTransactionConfirmedEvent', 'PropertyTransactionManuallyCreatedEvent')
							AND dspt.property_transaction_id NOT IN (
								SELECT property_transaction_id FROM datasci.property_transactions WHERE deleted = 'true')
							AND (date_part('day', now()::date - dspte.created_at) - 28) >= 0
							) a
						LEFT JOIN public.external_accounts pea on pea.id = a.external_account_id
						WHERE row_number = 1")

write.csv(intv_data, paste('~/Public/reactivation_intervention_', unique(intv_data$intv_date), '.csv', sep = ''))

kill_db_connections()

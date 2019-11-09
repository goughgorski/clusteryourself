#Clear Environment
rm(list=ls())

#Load libraries
source('~/workspace/datascience/functions/load_libraries.R')
load_libraries('~/workspace/datascience/functions/', include_bundles = c('utility'))

#Set Seed
set.seed(123456789)

#Connect to Local PSQL Database AND Pull Data
print(noquote("Connecting to local PostgreSQL Database"))
connection <- db_connect()

intv_start_date <- '2018-08-20'
intv_end_date <- as.Date(intv_start_date) + 30

#Query recent activity FROM most recently tranched users
intv_activity <- dbGetQuery(connection, paste("SELECT dspta.external_account_id 
		, pea.user_id
		, dspta.fitted
		, dspta.tranche
		, dspta.treatment
		, dspta.created_at AS tranched_at
		, case WHEN dspte_confirmed.confirmed IS NULL THEN 0 ELSE dspte_confirmed.confirmed END
		, case WHEN dspte_rejected.rejected IS NULL THEN 0 ELSE dspte_rejected.rejected END
		, case WHEN dspte_manually.manually_created IS NULL THEN 0 ELSE dspte_manually.manually_created END
		, case WHEN dspte_auto.auto_created IS NULL THEN 0 ELSE dspte_auto.auto_created END
		, case WHEN dsptme_shared.shared IS NULL THEN 0 ELSE dsptme_shared.shared END
		, pea.type
		, pu.signup_method
		, CASE WHEN ps.id IS NOT NULL AND ps.subscription_canceled_on IS NULL THEN 1 ELSE 0 END AS subscribed
		FROM (
		SELECT DISTINCT * FROM datasci_projects.tranche_assignments
		WHERE created_at = (SELECT MAX(created_at) FROM datasci_projects.tranche_assignments)
		) dspta
		LEFT JOIN (
			SELECT external_account_id
		, COUNT(dspte.external_account_id) AS confirmed
		FROM (
		SELECT external_account_id, dspte.type, dspte.created_at
		, MIN(dspte.created_at) OVER(partition by dspte.external_account_id) AS start_date
		FROM datasci.property_transaction_events dspte
		LEFT JOIN public.external_accounts pea on pea.id = dspte.external_account_id
		WHERE dspte.external_account_id IS NOT NULL
		AND dspte.created_at > ", intv_start_date,
		"AND dspte.created_at <= ", intv_end_date,
			") dspte
		WHERE dspte.type = 'PropertyTransactionConfirmedEvent'
		GROUP BY dspte.external_account_id
		) dspte_confirmed on dspta.external_account_id = dspte_confirmed.external_account_id
		LEFT JOIN (
			SELECT external_account_id
		, COUNT(dspte.external_account_id) AS rejected
		FROM (
		SELECT external_account_id, dspte.type, dspte.created_at
		, MIN(dspte.created_at) OVER(partition by dspte.external_account_id) AS start_date
		FROM datasci.property_transaction_events dspte
		LEFT JOIN public.external_accounts pea on pea.id = dspte.external_account_id
		WHERE dspte.external_account_id IS NOT NULL
		AND dspte.created_at > ", intv_start_date,
		"AND dspte.created_at <= ", intv_end_date,
			") dspte
		WHERE dspte.type = 'PropertyTransactionRejectedEvent'
		GROUP BY dspte.external_account_id
		) dspte_rejected on dspta.external_account_id = dspte_rejected.external_account_id
		LEFT JOIN (
			SELECT external_account_id
		, COUNT(dspte.external_account_id) AS manually_created
		FROM (
		SELECT external_account_id, dspte.type, dspte.created_at
		, MIN(dspte.created_at) OVER(partition by dspte.external_account_id) AS start_date
		FROM datasci.property_transaction_events dspte
		LEFT JOIN public.external_accounts pea on pea.id = dspte.external_account_id
		WHERE dspte.external_account_id IS NOT NULL
		AND dspte.created_at > ", intv_start_date,
		"AND dspte.created_at <= ", intv_end_date,
			") dspte
		WHERE dspte.type = 'PropertyTransactionManuallyCreatedEvent'
		GROUP BY dspte.external_account_id
		) dspte_manually on dspta.external_account_id = dspte_manually.external_account_id
		LEFT JOIN (
			SELECT external_account_id
		, COUNT(dspte.external_account_id) AS auto_created
		FROM (
		SELECT external_account_id, dspte.type, dspte.created_at
		, MIN(dspte.created_at) OVER(partition by dspte.external_account_id) AS start_date
		FROM datasci.property_transaction_events dspte
		LEFT JOIN public.external_accounts pea on pea.id = dspte.external_account_id
		WHERE dspte.external_account_id IS NOT NULL
		AND dspte.created_at > ", intv_start_date,
		"AND dspte.created_at <= ", intv_end_date,
			") dspte
		WHERE dspte.type = 'PropertyTransactionAutomaticallyCreatedEvent'
		GROUP BY dspte.external_account_id
		) dspte_auto on dspta.external_account_id = dspte_auto.external_account_id
		LEFT JOIN (
		SELECT external_account_id
		, COUNT(dspte.external_account_id) AS shared
		FROM (
		SELECT dspte.external_account_id, dsptme.type, dsptme.created_at
		, MIN(dspte.created_at) OVER(partition by dspte.external_account_id) AS start_date
		FROM datasci.property_transaction_multi_events dsptme
		LEFT JOIN datasci.property_transaction_events dspte on dsptme.property_transaction_id = dspte.property_transaction_id
		LEFT JOIN public.external_accounts pea on pea.id = dspte.external_account_id
		WHERE dsptme.type = 'PropertyTransactionSharedEvent'
		AND dspte.created_at > ", intv_start_date,
		"AND dspte.created_at <= ", intv_end_date,
		"AND dsptme.created_at > ", intv_start_date,
		"AND dsptme.created_at <= ", intv_end_date,
			") dspte
		GROUP BY dspte.external_account_id
		) dsptme_shared on dspta.external_account_id = dsptme_shared.external_account_id
		LEFT JOIN public.external_accounts pea on pea.id = dspta.external_account_id
		LEFT JOIN public.users pu on pea.user_id = pu.id
		LEFT JOIN public.subscriptions ps on pea.user_id = ps.user_id
		WHERE pu.type = 'AgentUser'", sep = "'"))

#Create active variable
active.var = c('confirmed', 'manually_created', 'shared')

intv_activity$active <- ifelse(eval(parse(text = paste('intv_activity$', active.var, ' == 0', sep = '', collapse = ' & '))), 0, 1)

#Remove treatment users who did not receive the intervention due to unsubscribing **NEED TO TEST**
setwd('~/workspace/datascience/_Old')

recipients <- read.csv('Amitree_Users_596979_export_2018-08-22_15_55.csv')
#recipients <- read.csv('Amitree_Users_596979_export_2018-08-22_15_55.csv')

intv_activity <- intv_activity[intv_activity$treatment == FALSE | intv_activity$user_id %in% recipients$User.ID,]

pvalue_table <- ldply(lapply(split(intv_activity, intv_activity[,'tranche']), function(x){
	tmp <- x[1,]
	tmp$p.value <- chisq.test(x[, 'treatment'], x[, 'active'], correct = FALSE)$p.value
	tmp$n <- nrow(x)
	return(tmp[, c('tranche', 'p.value', 'n')])
	}), rbind)

pvalue_table <- cbind(pvalue_table, as.data.frame.matrix(table(intv_activity$tranche[intv_activity$treatment == T], 
	intv_activity$active[intv_activity$treatment == T])))

colnames(pvalue_table)[which(colnames(pvalue_table) %in% c(0,1))] <- c('not_active.treatment', 'active.treatment')

pvalue_table <- cbind(pvalue_table, as.data.frame.matrix(table(intv_activity$tranche[intv_activity$treatment == F], 
	intv_activity$active[intv_activity$treatment == F])))

colnames(pvalue_table)[which(colnames(pvalue_table) %in% c(0,1))] <- c('not_active.control', 'active.control')

pvalue_table$`%active.treatment` <- pvalue_table$active.treatment/(pvalue_table$active.treatment + pvalue_table$not_active.treatment)
pvalue_table$`%active.control` <- pvalue_table$active.control/(pvalue_table$active.control + pvalue_table$not_active.control)
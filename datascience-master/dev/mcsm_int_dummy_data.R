
database_url <- 'postgres://u9njark9n9e7hr:p7184bf4af82fa5f484814d4ec48d406c470b129c84c4487491f74295ac9bf750@ec2-3-217-149-68.compute-1.amazonaws.com:5432/df9hckvjk1jikn'
user_id <- 123583 #default - 89, merged - 99
user_id <- 123062 #default - 39, merged - 41
user_id <- 123077 #default - 2, merged - 3
user_id <- 116263 #default - 12, merged - 13
user_id <- 113761 # 
dyad_threshold <- 0.03
max_n_dyads <- 100

parts <- parse_url(database_url)

# loads the PostgreSQL driver
drv <- dbDriver("PostgreSQL")

#Establish local connection
connection <- dbConnect(drv, 
         host     = ifelse(is.null(parts$hostname), '', parts$hostname),
         port     = ifelse(is.null(parts$port), '', parts$port),
         user     = ifelse(is.null(parts$username), '', parts$username),
         password = ifelse(is.null(parts$password), '', parts$password),
         dbname   = gsub('^/', '', parts$path)
)

dyads <- dbGetQuery(connection, paste("SELECT oauth_account_id, alter_1, alter_2, predicted_value as pred 
                                  FROM public.users_dyads_input_agg_covariates_",user_id,
                                  " WHERE predicted_value > ", dyad_threshold,
                                  " ORDER BY predicted_value DESC
                                  LIMIT ", max_n_dyads, sep = ''))

colnames(dyads)[which(colnames(dyads) %in% c('alter_1', 'alter_2'))] <- c('alter_string_1', 'alter_string_2')

alters <- c(dyads$alter_string_1, dyads$alter_string_2)
alters <- unique(alters)
alters <- sort(alters)
alters <- data.frame(alter = as.character(alters), alt_id = 1:length(alters))

dyads <- merge(dyads, alters, by.x = 'alter_string_1', by.y = 'alter', all.x = T)
colnames(dyads)[which(colnames(dyads) == 'alt_id')] <- 'alter_1'
dyads <- merge(dyads, alters, by.x = 'alter_string_2', by.y = 'alter', all.x = T)
colnames(dyads)[which(colnames(dyads) == 'alt_id')] <- 'alter_2'

## DEFAULT ##
# create comparison results
default_results_string <- multi_contact_grouping(user_id, dyad_threshold, max_n_dyads, database_url)

# replace strings with integers and compare
results_string <- ldply(lapply(seq_along(default_results_string[[2]]), function(x) {
	data.frame(group = x, alter = paste(alters$alt_id[alters$alter %in% default_results_string[[2]][[x]]$group], collapse = ''),
		avg_pred = default_results_string[[2]][[x]]$avg_pred)}
	), rbind)

results_int <- ldply(lapply(seq_along(default_results_int[[2]]), function(x) {
	data.frame(group = x, alter = paste(default_results_int[[2]][[x]]$group, collapse = ''), avg_pred = default_results_int[[2]][[x]]$avg_pred)}
	), rbind)

nrow(merge(results_int, results_string, by = 'alter', all = T))

## MERGING ##
# create comparison results
merging_results_string <- multi_contact_grouping(user_id, dyad_threshold, max_n_dyads, database_url, merge_threshold = 0.66)

# replace strings with integers and compare
results_string <- ldply(lapply(seq_along(merging_results_string[[2]]), function(x) {
	data.frame(group = x, alter = paste(alters$alt_id[alters$alter %in% merging_results_string[[2]][[x]][[1]]], collapse = ''),
		avg_pred = merging_results_string[[2]][[x]]$avg_pred)}
	), rbind)

results_int <- ldply(lapply(seq_along(merging_results_int[[2]]), function(x) {
	data.frame(group = x, alter = paste(merging_results_int[[2]][[x]][[1]], collapse = ''), avg_pred = merging_results_int[[2]][[x]]$avg_pred)}
	), rbind)

nrow(merge(results_int, results_string, by = 'alter', all = T))

## DOMAINS ##
#create domain_id table from whitelist, existing dyads, and user email
alt_domains <- sapply(alters$alter, function(x) {strsplit(x, '@')[[1]][2]})
domain_whitelist <- sapply(c("@mail.mil", "@centurylink.net", "@rocketmail.com", "@twc.com", "@mac.com", "@netzero.net", "@hotmail.com", "@yahoo.com", "@aim.com", "@ymail.com", "@att.ne", "@suddenlink.net", "@juno.com", "@live.com", "@roadrunner.com", "@q.com", "@icloud.com", "@embarqmail.com", "@me.com", "@charter.net", "@frontier.com", "@gmail.com", "@mail.com", "@verizon.net", "@msn.com", "@comcast.net", "@pacbell.net", "@prodigy.net", "@windstream.net", "@sbcglobal.net", "@aol.com", "@ptd.net", "@outlook.com", "@bellsouth.net", "@tampabay.rr.com", "@nc.rr.com", "@earthlink.net", "@cableone.net", "@rochester.rr.com", "@cfl.rr.com", "@cox.net"),
	function(x) {strsplit(x, '@')[[1]][2]})
user_domain <- dbGetQuery(connection, paste("SELECT user_mail_list FROM public.email_account_aliases WHERE oauth_account_id = ", user_id, sep = ''))
user_domain <- gsub("[{}]", "", user_domain)
    if (grepl(',', user_domain) == TRUE) {
      user_domain <- sapply(strsplit(user_domain, ',')[[1]], function(x) {strsplit(x, '@')[[1]][2]})
      } else {user_domain <- strsplit(user_domain, '@')[[1]][2]}

domains <- c(unname(alt_domains), unname(domain_whitelist), unname(user_domain))
domains <- domains[!duplicated(domains)]
domains <- data.frame(domain = domains, domain_id = 1:length(domains))

domain_id <- merge(data.frame(id = 1:length(alt_domains), domain = alt_domains), domains, by = 'domain')
alters <- cbind(alters, domain_id = domain_id$domain_id[order(domain_id$id)])

domain_ids <- alters[, c(2,3)]
colnames(domain_ids) <- c('alter', 'domain_id')

user_domain_id <- domains$domain_id[which(domains$domain == user_domain)]
whitelist_domain_ids <- domains$domain_id[domains$domain %in% domain_whitelist]




## script uses functions/reactivation_tranches.R, deviates before '# separating into tranches'

tmp <- last_month[last_month$active == 1 & last_month$subscribed == 0,]
tmp <- tmp[order(-tmp$fitted), c('external_account_id', 'fitted', 'start_date')]
tmp <- tmp[c(1:2000), ]
tmp$treatment <- sample(0:1, nrow(tmp), replace = T)
tmp <- merge(tmp, user_ids[!duplicated(user_ids$external_account_id) & !duplicated(user_ids$user_id),], by = 'external_account_id', all.x = T)

emails <- dbGetQuery(connection, "select id, email from public.users")
emails <- emails[!is.na(emails$email),]
tmp <- merge(tmp, emails, by.x = 'user_id', by.y = 'id', all.x = T)

tmp$model_date <- today()

dbWriteTable(connection, c('datasci_research','subscription_nudgees'), value = tmp, overwrite = FALSE, append = TRUE, row.names = FALSE)

setwd('~/Public/')

write.csv(tmp[tmp$treatment == 1, c('user_id', 'email', 'start_date')], 'users_for_subscription_marketing.csv')
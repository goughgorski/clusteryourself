#Function to process monthly market coverage data
process_market_coverage <- function(connection) {
	
	print(noquote("Processing market coverage data"))
	
	#Query to get data from market coverage view
	market_coverage <- dbGetQuery(connection, 'SELECT *
	                                           FROM datasci_projects.vw_market_coverage_inputs')

	market_coverage$created_month <- format(as.Date(market_coverage$created_at), "%Y-%m")
	market_coverage$closing_month <- format(as.Date(market_coverage$closing_date), "%Y-%m")
	market_coverage$has_closing <- !is.na(market_coverage$closing_date)
	market_coverage$confirmed_or_created <- market_coverage$type == 'PropertyTransactionConfirmedEvent' | market_coverage$type == 'PropertyTransactionManuallyCreatedEvent' 
	market_coverage$confirmed_or_rejected <- ifelse(market_coverage$type == 'PropertyTransactionConfirmedEvent',TRUE,ifelse(market_coverage$type == 'PropertyTransactionRejectedEvent',FALSE,NA))
	market_coverage$post_closing <- market_coverage$created_month > market_coverage$closing_month

	#No clean up for long closings eg > 5 mos or back-closings eg closing date < created_at
	counts <- table(market_coverage$created_month[market_coverage$confirmed_or_created == TRUE], market_coverage$closing_month[market_coverage$confirmed_or_created == TRUE])
	rperc <- prop.table(counts,1)
	closing_counts <- apply(counts,2,sum)
	counts_no_close <- table(market_coverage$created_month[market_coverage$has_closing == FALSE & market_coverage$confirmed_or_created == TRUE])
	est_counts <- apply(rperc,2,function(x){x*counts_no_close})
	est_closing_counts <- apply(est_counts,2,sum)
	active_closings <- closing_counts + est_closing_counts
	active_closings <- as.data.frame(cbind(created_month = names(active_closings),active_closings),row.names = FALSE)

	unconfirmed <- market_coverage[market_coverage$type == 'PropertyTransactionAutomaticallyCreatedEvent' & market_coverage$has_closing == FALSE,]
	unconfirmed$one <- 1
	confirmation_rate <- aggregate(confirmed_or_rejected~created_month, data = market_coverage, FUN = mean, na.rm = T)
	unconfirmed_counts <- aggregate(one~created_month, data = unconfirmed, FUN = sum, na.rm = T)
	unconfirmed_counts <- unconfirmed_counts[unconfirmed_counts$created_month %in% rownames(rperc),'one']
	est_likely_counts <- apply(rperc,2,function(x){x*unconfirmed_counts})
	likely_counts <- apply(est_likely_counts,2,sum)
	likely_counts <- as.data.frame(cbind(created_month = names(likely_counts), likely_counts = as.numeric(as.character(likely_counts))),row.names = FALSE)	
	likely_closings <- merge(confirmation_rate,likely_counts, by = 'created_month', all.x = T)
	likely_closings$likely_closings <- as.numeric(as.character(likely_closings$likely_counts)) * likely_closings$confirmed_or_rejected

	closings <- merge(active_closings, likely_closings, by = 'created_month', all = TRUE)
	closings$total_closings <- as.numeric(as.character(closings$active_closings)) + as.numeric(as.character(closings$likely_closings))
	closings$computed_at <- Sys.time()

	dbWriteTable(connection, c('datasci_projects','market_coverage'), value = closings, overwrite = F, append = T, row.names = FALSE)
}
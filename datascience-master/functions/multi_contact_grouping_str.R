multi_contact_grouping_str <- function(user_id, dyad_threshold, max_n_dyads, database_url = NULL, domain_grouping = FALSE,
                                   domain_whitelist = NULL, merge_threshold = NULL){

  print(noquote("Connecting to database"))
  if (is.null(database_url)) {connection <- db_connect()} else {

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

  }

  print(noquote(paste("Extracting dyads for oauth_account_id", user_id, collapse = '')))
  dyads <- dbGetQuery(connection, paste("SELECT oauth_account_id, alter_1, alter_2, predicted_value as pred 
                                  FROM public.users_dyads_input_agg_covariates_",user_id,
                                  " WHERE predicted_value > ", dyad_threshold,
                                  " ORDER BY predicted_value DESC
                                  LIMIT ", max_n_dyads, sep = ''))

  if (nrow(dyads) == 0) {
    print(noquote("No qualifying dyads found"))
    return(list(oauth_account_id = user_id, sfgroups = c()))
  }
  print(noquote(paste(nrow(dyads), "dyads found", collapse = '')))

  dyads$pred <- as.numeric(dyads$pred)
  dyad_threshold <- as.numeric(dyad_threshold)
  max_n_dyads <- as.numeric(max_n_dyads)

  print(noquote("Analyzing dyad data"))
  expected_columns <- c('oauth_account_id', 'pred', 'alter_1', 'alter_2')
  missing_columns <- expected_columns[!(expected_columns %in% colnames(dyads))]
  if (length(missing_columns) > 0) {stop(noquote(paste('Columns missing: ', paste(missing_columns, collapse = ', '))))}
  
  suggested_dyads <- dyads
  
  if (domain_grouping == TRUE) {
    user_domain <- dbGetQuery(connection, paste("SELECT user_mail_list FROM public.external_account_aliases WHERE external_account_id = ", user_id, sep = ''))
    user_domain <- gsub("[{}]", "", user_domain)
    if (grepl(',', user_domain) == TRUE) {
      user_domain <- sapply(strsplit(user_domain, ',')[[1]], function(x) {paste('@', strsplit(x, '@')[[1]][2], sep = '')})}
    # domain whitelist hardcoded - should connect directly to amitree/config/prospect_folders/whitelisted_domains.json
    domain_whitelist <- c("@mail.mil", "@centurylink.net", "@rocketmail.com", "@twc.com", "@mac.com", "@netzero.net", "@hotmail.com", "@yahoo.com", "@aim.com", "@ymail.com", "@att.ne", "@suddenlink.net", "@juno.com", "@live.com", "@roadrunner.com", "@q.com", "@icloud.com", "@embarqmail.com", "@me.com", "@charter.net", "@frontier.com", "@gmail.com", "@mail.com", "@verizon.net", "@msn.com", "@comcast.net", "@pacbell.net", "@prodigy.net", "@windstream.net", "@sbcglobal.net", "@aol.com", "@ptd.net", "@outlook.com", "@bellsouth.net", "@tampabay.rr.com", "@nc.rr.com", "@earthlink.net", "@cableone.net", "@rochester.rr.com", "@cfl.rr.com", "@cox.net")
    alts <- c(as.character(suggested_dyads$a1), as.character(suggested_dyads$alter_2))
    alts <- alts[!duplicated(alts)]
    alts <- data.frame(alter = alts, stringsAsFactors = FALSE)
    alts$domain <- apply(alts, 1, function(x) {paste('@', strsplit(x['alter'], '@')[[1]][2], sep = '')})
    domains <- alts[!alts$domain %in% c(domain_whitelist, user_domain),]
    alts <- merge(domains, domains, by = 'domain')
    alts <- alts[alts$alter.x != alts$alter.y,]
    alts[, c('alter_1', 'alter_2')] <- ldply(apply(alts, 1, function(x) {data.frame(
      'alter_1' = c(c(x['alter.x']), c(x['alter.y']))[order(c(c(x['alter.x']), c(x['alter.y'])))][1],
      'alter_2' = c(c(x['alter.x']), c(x['alter.y']))[order(c(c(x['alter.x']), c(x['alter.y'])))][2])}),
      rbind)[, c('alter_1', 'alter_2')]
    alts <- alts[!duplicated(alts[, c('alter_1', 'alter_2')]),]
    domain_dyads <- merge(suggested_dyads, alts[, c('alter_1', 'alter_2')], by = c('alter_1', 'alter_2'), all = T)
    domain_dyads <- domain_dyads[is.na(domain_dyads$pred),]
    domain_dyads$pred[is.na(domain_dyads$pred)] <- 0
    domain_dyads$oauth_account_id[is.na(domain_dyads$oauth_account_id)] <- unique(dyads$oauth_account_id)
    
    if(domain_grouping == 'local') {
      print(noquote("Domain grouping set to 'local' : synthetic relationship will be created between all members of a domain family"))
      suggested_dyads <- rbind(suggested_dyads, domain_dyads)
    }
    
    if(domain_grouping == 'global') {
      print(noquote("Domain grouping set to 'global' : domain families will be considered as single alter"))
      domains <- merge(domains, as.data.frame(table(domains$domain)), by.x = 'domain', by.y = 'Var1')
      suggested_dyads <- merge(suggested_dyads, domains[domains$Freq > 1,], by.x = 'alter_1', by.y = 'alter', all.x = T)
      suggested_dyads <- merge(suggested_dyads, domains[domains$Freq > 1,], by.x = 'alter_2', by.y = 'alter', all.x = T)
      suggested_dyads$alter_1 <- ifelse(is.na(suggested_dyads$domain.x), as.character(suggested_dyads$alter_1), suggested_dyads$domain.x)
      suggested_dyads$alter_2 <- ifelse(is.na(suggested_dyads$domain.y), as.character(suggested_dyads$alter_2), suggested_dyads$domain.y)
    }
    
    if(domain_grouping != 'local' & domain_grouping != 'global') {
      stop(noquote("Domain grouping argument must be set to either 'local' or 'global.'"))
    }
  }
  
  suggested_dyads <- suggested_dyads[order(as.numeric(as.character(suggested_dyads$pred)), decreasing = T),]
  if (nrow(suggested_dyads) > max_n_dyads) {suggested_dyads <- suggested_dyads[1:max_n_dyads,]}
  
  suggested_dyads$alter_1 <- as.character(suggested_dyads$alter_1)
  suggested_dyads$alter_2 <- as.character(suggested_dyads$alter_2)
  suggested_dyads$pred <- as.numeric(suggested_dyads$pred)
  
  print(noquote("Determining multi-contact group suggestions"))
  dyads_graph <- graph_from_data_frame(suggested_dyads[,c('alter_1', 'alter_2')])
  sfgroups <- suppressWarnings(max_cliques(dyads_graph))
  
  if (!is.null(domain_grouping) && domain_grouping == 'global') {
    sfgroups <- lapply(sfgroups, function(x){
      alts <- names(x)[order(names(x))]
      alts <- c(alts, domains$alter[domains$domain %in% alts[regexpr('^@', alts) == 1]])
      doms <- alts[regexpr('^@', alts) == 1]
      alts <- alts[regexpr('^@', alts) == -1]
      group <- paste(alts, collapse = ', ')
      avg_pred <- as.data.frame(t(combn(alts, 2)))
      colnames(avg_pred) <- c('alter_1', 'alter_2')
      # here calls to un-subsetted list of dyads
      avg_pred <- merge(avg_pred, dyads[, c('alter_1', 'alter_2', 'pred')], by = c('alter_1', 'alter_2'))
      avg_pred <- mean(avg_pred$pred)
      list(group = alts, avg_pred = avg_pred, domain = doms)
    })
  } else {
  sfgroups <- lapply(sfgroups, function(x){
    alts <- names(x)[order(names(x))]
    #group <- paste(alts, collapse = ', ')
    group_pred <- as.data.frame(t(combn(alts, 2)))
    colnames(group_pred) <- c('alter_1', 'alter_2')
    group_pred <- merge(group_pred, suggested_dyads[, c('alter_1', 'alter_2', 'pred')])
    avg_pred <- mean(group_pred$pred)
    list(group = alts, avg_pred = avg_pred)
  })
  }
  
  names(sfgroups) <- order(order(ldply(lapply(sfgroups, function(x) {x$avg_pred}), rbind), decreasing = T))
  sfgroups <- sfgroups[order(as.numeric(names(sfgroups)))]
  
  if (!is.null(merge_threshold)) {
    if (!is.numeric(merge_threshold) | merge_threshold > 1 | merge_threshold <= 0.5) {
      stop(noquote('Merge threshold must be a numeric between 0.5 and 1'))}
    print(noquote(paste("Merge threshold set to ", merge_threshold)))
    overlap <- as.matrix(ldply(lapply(sfgroups, function(x) {
      unlist(lapply(sfgroups, function(y) {
        length(intersect(x$group, y$group))/length(x$group)
      }))
    }), rbind))
    overlap <- overlap[, which(!colnames(overlap) == '.id')]
    diag(overlap) <- 0
    if (all(overlap <= merge_threshold)) {return(list(oauth_account_id = user_id, sfgroups = sfgroups))}
    ol_over <- (overlap > merge_threshold)
    ol_over <- as.data.frame(which(ol_over == TRUE, arr.ind = TRUE))
    ol_over[, c('p1', 'p2')] <- t(apply(ol_over, 1, function(x) {
      c(x['row'], x['col'])[order(c(x['row'], x['col']))]}))
    ol_graph <- graph_from_data_frame(ol_over)
    olgroups <- suppressWarnings(max_cliques(ol_graph, min = 2))
    mgroups <- as.numeric(unlist(lapply(olgroups, function(x) {names(x)[order(names(x))]})))
    mgroups <- as.data.frame(table(mgroups))
    mgroup_dupes <- as.numeric(as.character(mgroups$mgroups[mgroups$Freq > 1]))
    mgroups <- as.numeric(as.character(mgroups$mgroups))
    msfgroups <- lapply(olgroups, function(x){
      if (all(!names(x) %in% mgroup_dupes)) {
        gs <- (names(x))
        gs <- gs[order(gs)]
        alts <- unlist(strsplit(unlist(lapply(sfgroups[which(names(sfgroups) %in% gs)], function(x) {x$group})), ', '))
        alts <- alts[!duplicated(alts)]
        alts <- alts[order(alts)]
        group <- paste(alts, collapse = ', ')
        avg_pred <- as.data.frame(t(combn(alts, 2)))
        colnames(avg_pred) <- c('alter_1', 'alter_2')
        avg_pred <- merge(avg_pred, suggested_dyads[, c('alter_1', 'alter_2', 'pred')])
        avg_pred <- mean(avg_pred$pred)
        list(merged_groups = group, avg_pred = avg_pred, merged_sfgs = gs)}
    })
    if (length(mgroup_dupes) > 0) {
      dupes_groups <- lapply(mgroup_dupes, function(x) {
        over <- rbind(ol_over[ol_over$row == as.numeric(x),], ol_over[ol_over$col == as.numeric(x),])
        absorb <- over[over$col == as.numeric(x), ]
        join <- over[over$row == as.numeric(x), ]
        gs <- c()
        if (nrow(absorb) > 0) {
          gs <- c(gs, as.numeric(absorb$row))}
        if (nrow(join) == 1) {gs <- c(gs, as.numeric(join$col))}
        if (nrow(join) > 1) {
          join$ol <- apply(join, 1, function(x) {overlap[as.numeric(x['row']), as.numeric(x['col'])]})
          join <- join[join$ol == unique(join$ol)[order(unique(join$ol), decreasing = T)][1],]
          if (nrow(join) > 1) {
            join$inverse_ol <- apply(join, 1, function(x) {overlap[as.numeric(x['col']), as.numeric(x['row'])]})
            gs <- c(gs, as.numeric(join$col[which.max(join$inverse_ol)]))
          } else {gs <- c(gs, as.numeric((join$col)))}
          
        }
        gs <- c(gs, as.numeric(x))
        gs <- gs[!duplicated(gs)]
        gs <- gs[order(gs)]
        alts <- unlist(strsplit(unlist(lapply(sfgroups[which(names(sfgroups) %in% gs)], function(x) {x$group})), ', '))
        alts <- alts[!duplicated(alts)]
        alts <- alts[order(alts)]
        group <- paste(alts, collapse = ', ')
        avg_pred <- as.data.frame(t(combn(alts, 2)))
        colnames(avg_pred) <- c('alter_1', 'alter_2')
        avg_pred <- merge(avg_pred, suggested_dyads[, c('alter_1', 'alter_2', 'pred')])
        avg_pred <- mean(avg_pred$pred)
        list(merged_groups = group, avg_pred = avg_pred, merged_sfgs = gs)
      })
    }
    msfgroups <- c(compact(msfgroups), dupes_groups)
    names(msfgroups) <- order(order(ldply(lapply(msfgroups, function(x) {x$avg_pred}), rbind), decreasing = T))
    msfgroups <- msfgroups[order(as.numeric(names(msfgroups)))]
    names(msfgroups) <- (max(as.numeric(names(sfgroups))) + 1):(length(msfgroups) + max(as.numeric(names(sfgroups))))
    sfgroups <- c(sfgroups, msfgroups)
  }
  
  return(list(oauth_account_id = user_id, sfgroups = sfgroups))
}

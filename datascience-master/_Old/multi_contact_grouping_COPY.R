multi_contact_grouping <- function(dyads, dyad_threshold, max_n_dyads, domain_grouping = NULL,
                                   domain_whitelist = NULL, user_domain = NULL, merge_threshold = NULL){
  
  print(noquote("Analyzing dyad data"))
  expected_columns <- c('oauth_account_id', 'pred', 'a1', 'a2')
  missing_columns <- expected_columns[!(expected_columns %in% colnames(dyads))]
  if (length(missing_columns) > 0) {stop(noquote(paste('Columns missing: ', paste(missing_columns, collapse = ', '))))}
  
  print(noquote("Subsetting dyads"))
  suggested_dyads <- dyads[dyads$pred > dyad_threshold,]
  
  if (!is.null(domain_grouping)) {
    if (is.null(domain_whitelist) | is.null(user_domain)) {stop(noquote('Cannot apply domain grouping without a domain whitelist'))}
    alts <- c(as.character(suggested_dyads$a1), as.character(suggested_dyads$a2))
    alts <- alts[!duplicated(alts)]
    alts <- data.frame(alter = alts, stringsAsFactors = FALSE)
    alts$domain <- apply(alts, 1, function(x) {paste('@', strsplit(x['alter'], '@')[[1]][2], sep = '')})
    domains <- alts[!alts$domain %in% c(domain_whitelist, user_domain),]
    alts <- merge(domains, domains, by = 'domain')
    alts <- alts[alts$alter.x != alts$alter.y,]
    alts[, c('a1', 'a2')] <- ldply(apply(alts, 1, function(x) {data.frame(
      'a1' = c(c(x['alter.x']), c(x['alter.y']))[order(c(c(x['alter.x']), c(x['alter.y'])))][1],
      'a2' = c(c(x['alter.x']), c(x['alter.y']))[order(c(c(x['alter.x']), c(x['alter.y'])))][2])}),
      rbind)[, c('a1', 'a2')]
    alts <- alts[!duplicated(alts[, c('a1', 'a2')]),]
    domain_dyads <- merge(suggested_dyads, alts[, c('a1', 'a2')], by = c('a1', 'a2'), all = T)
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
      suggested_dyads <- merge(suggested_dyads, domains[domains$Freq > 1,], by.x = 'a1', by.y = 'alter', all.x = T)
      suggested_dyads <- merge(suggested_dyads, domains[domains$Freq > 1,], by.x = 'a2', by.y = 'alter', all.x = T)
      suggested_dyads$a1 <- ifelse(is.na(suggested_dyads$domain.x), as.character(suggested_dyads$a1), suggested_dyads$domain.x)
      suggested_dyads$a2 <- ifelse(is.na(suggested_dyads$domain.y), as.character(suggested_dyads$a2), suggested_dyads$domain.y)
    }
    
    if(domain_grouping != 'local' & domain_grouping != 'global') {
      stop(noquote("Domain grouping argument must be set to either 'local' or 'global.'"))
    }
  }
  
  suggested_dyads <- suggested_dyads[order(as.numeric(as.character(suggested_dyads$pred)), decreasing = T),]
  if (nrow(suggested_dyads) > max_n_dyads) {suggested_dyads <- suggested_dyads[1:max_n_dyads,]}
  
  suggested_dyads$a1 <- as.character(suggested_dyads$a1)
  suggested_dyads$a2 <- as.character(suggested_dyads$a2)
  suggested_dyads$pred <- as.numeric(suggested_dyads$pred)
  
  print(noquote("Determining multi-contact group suggestions"))
  dyads_graph <- graph_from_data_frame(suggested_dyads[,c('a1', 'a2')])
  sfgroups <- suppressWarnings(max_cliques(dyads_graph))
  
  if (domain_grouping == 'global') {
    sfgroups <- ldply(lapply(sfgroups, function(x){
      alts <- names(x)[order(names(x))]
      alts <- c(alts, domains$alter[domains$domain %in% alts[regexpr('^@', alts) == 1]])
      doms <- alts[regexpr('^@', alts) == 1]
      alts <- alts[regexpr('^@', alts) == -1]
      group <- paste(alts, collapse = ', ')
      avg_pred <- as.data.frame(t(combn(alts, 2)))
      colnames(avg_pred) <- c('a1', 'a2')
      avg_pred <- merge(avg_pred, dyads[, c('a1', 'a2', 'pred')], by = c('a1', 'a2'))
      avg_pred <- mean(avg_pred$pred)
      data.frame(group, avg_pred, domain = paste(doms, collapse = ', '))
    }), rbind)
  } else {
  sfgroups <- ldply(lapply(sfgroups, function(x){
    alts <- names(x)[order(names(x))]
    group <- paste(alts, collapse = ', ')
    avg_pred <- as.data.frame(t(combn(alts, 2)))
    colnames(avg_pred) <- c('a1', 'a2')
    avg_pred <- merge(avg_pred, suggested_dyads[, c('a1', 'a2', 'pred')])
    avg_pred <- mean(avg_pred$pred)
    data.frame(group, avg_pred)
  }), rbind)
  }
  
  sfgroups <- sfgroups[order(sfgroups$avg_pred, decreasing = T),]
  sfgroups$id <- 1:nrow(sfgroups)
  sfgroups <- sfgroups[, c(colnames(sfgroups)[colnames(sfgroups) == 'id'], colnames(sfgroups)[colnames(sfgroups) != 'id'])]
  sfgroups$group <- as.character(sfgroups$group)
  
  if (!is.null(merge_threshold)) {
    if (!is.numeric(merge_threshold) | merge_threshold > 1 | merge_threshold <= 0.5) {
      stop(noquote('Merge threshold must be a numeric between 0.5 and 1'))}
    print(noquote(paste("Merge threshold set to ", merge_threshold)))
    overlap <- apply(sfgroups, 1, function(x) {
      apply(sfgroups, 1, function(y) {
        length(intersect(strsplit(as.character(y['group']), ', ')[[1]], strsplit(as.character(x['group']), ', ')[[1]]))/
          length(strsplit(as.character(y['group']), ', ')[[1]])
      })
    })
    diag(overlap) <- 0
    if (all(overlap <= merge_threshold)) {return(sfgroups)}
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
    msfgroups <- ldply(lapply(olgroups, function(x){
      if (all(!names(x) %in% mgroup_dupes)) {
        gs <- as.numeric(names(x))
        alts <- unlist(strsplit(sfgroups$group[gs], ', '))
        alts <- alts[!duplicated(alts)]
        alts <- alts[order(alts)]
        group <- paste(alts, collapse = ', ')
        avg_pred <- as.data.frame(t(combn(alts, 2)))
        colnames(avg_pred) <- c('a1', 'a2')
        avg_pred <- merge(avg_pred, suggested_dyads[, c('a1', 'a2', 'pred')])
        avg_pred <- mean(avg_pred$pred)
        data.frame(group, avg_pred, merged_sfgs = paste(gs, collapse = ', '))}
    }), rbind)
    if (length(mgroup_dupes) > 0) {
      dupes_groups <- ldply(lapply(mgroup_dupes, function(x) {
        over <- rbind(ol_over[ol_over$row == as.numeric(x),], ol_over[ol_over$col == as.numeric(x),])
        absorb <- over[over$col == as.numeric(x), ]
        join <- over[over$row == as.numeric(x), ]
        gs <- c()
        if (nrow(absorb) > 0) {
          gs <- c(gs, as.numeric(absorb$row))}
        if (nrow(join) == 1) {gs <- c(gs, as.numeric(join$col))}
        if (nrow(join) > 1) {
          join$ol <- apply(join, 1, function(x) {overlap[x['row'], x['col']]})
          join <- join[join$ol == unique(join$ol)[order(unique(join$ol), decreasing = T)][1],]
          if (nrow(join) > 1) {
            join$inverse_ol <- apply(join, 1, function(x) {overlap[x['col'], x['row']]})
            gs <- c(gs, as.numeric(join$col[which.max(join$inverse_ol)]))
          } else {gs <- c(gs, as.numeric((join$col)))}
          
        }
        gs <- c(gs, as.numeric(x))
        alts <- unlist(strsplit(sfgroups$group[gs], ', '))
        alts <- alts[!duplicated(alts)]
        alts <- alts[order(alts)]
        group <- paste(alts, collapse = ', ')
        avg_pred <- as.data.frame(t(combn(alts, 2)))
        colnames(avg_pred) <- c('a1', 'a2')
        avg_pred <- merge(avg_pred, suggested_dyads[, c('a1', 'a2', 'pred')])
        avg_pred <- mean(avg_pred$pred)
        data.frame(group, avg_pred, merged_sfgs = paste(gs, collapse = ', '))
      }), rbind)
    }
    msfgroups <- rbind(msfgroups, dupes_groups)
    msfgroups <- msfgroups[order(msfgroups$avg_pred, decreasing = T),]
    msfgroups$id <- max(sfgroups$id):(nrow(msfgroups) + max(sfgroups$id) - 1)
    sfgroups <- rbind.fill(sfgroups[, c('id', 'group', 'avg_pred', 'domain')], msfgroups)
  }
  
  return(sfgroups)
}

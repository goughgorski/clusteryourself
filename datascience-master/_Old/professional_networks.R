#Clear Workspace
rm(list=ls())

#Load libraries

source('~/workspace/datascience/functions/load_libraries.R')
load_libraries('~/workspace/datascience/functions/')

#Set Seed
set.seed(123456789)

#Connect to Local PSQL Database
local_connection <- local_db_conect('development')
prod_connection <- heroku_connect_app_db('amitree')

aff_mat_query <- "select gt.id
    , gt.email as agent_email
    , p.company
    , gc.email as contact_email
    , gc.role as contact_role
    , pc.company as contact_company
    , count(property_transaction_id) as tx_count
from gmail_tokens gt
left join property_transactions pt on gt.id = pt.external_account_id
left join contacts gc on gc.property_transaction_id = pt.id
left join people p on gt.email = p.email
left join users u on p.user_id = u.id
left join people pc on pc.email = gc.email
where (p.company ilike '%re%max%' OR p.email ilike '%re%max%') and u.type = 'AgentUser'
group by gt.id, gt.email, p.company, gc.email, gc.role, pc.company"

aff_mat_remax <- dbGetQuery(prod_connection, aff_mat_query)
kill_db_connections()
aff_mat_remax$role <- 'Agent'

i <- sapply(aff_mat_remax,is.factor)
aff_mat_remax[i] <- lapply(aff_mat_remax[i], as.character)

write.csv(aff_mat_remax, file = '~/Development/Data/aff_mat_remax.csv', row.names = F)

#Subset to role here
aff_mat_remax_lenders <- aff_mat_remax[aff_mat_remax$contact_role == 2 & !is.na(aff_mat_remax$contact_role),]
aff_mat_remax_lenders$contact_email_domain <- regexpr('@',aff_mat_remax_lenders$contact_email)
aff_mat_remax_lenders$contact_email_domain <- substr(aff_mat_remax_lenders$contact_email,aff_mat_remax_lenders$contact_email_domain,nchar(aff_mat_remax_lenders$contact_email))


d <- cbind(aff_mat_remax_lenders$agent_email, aff_mat_remax_lenders$contact_email, aff_mat_remax_lenders$tx_count)
d <- d[!is.na(d[,1]) & !is.na(d[,2]),]
colnames(d) <- c('Agent','Contact','tx_count')
#write.csv(d, file = '~/Development/Data/remax_edges.csv', row.names = F)
vertices1 <- aff_mat_remax_lenders[!duplicated(aff_mat_remax_lenders[,c('agent_email', 'company', 'role')]),c('agent_email', 'company', 'role')]
vertices2 <- aff_mat_remax_lenders[!duplicated(aff_mat_remax_lenders[,c('contact_email', 'contact_company', 'contact_role')]) & !is.na(aff_mat_remax_lenders$contact_email),c('contact_email', 'contact_company', 'contact_role')]
colnames(vertices1) <- c('email', 'company', 'role')
colnames(vertices2) <- c('email', 'company', 'role')
vertices <- rbind(vertices1, vertices2)
vertices <- vertices[order(vertices$email, vertices$company, vertices$role),]
vertices$obsnum <- sequence(rle(vertices$email)$lengths)
vertices1 <- dcast(vertices, email + company ~ paste('role',obsnum,sep = '.'), value.var = 'role')
vertices1$obsnum <- sequence(rle(vertices1$email)$lengths)
vertices <- vertices1[vertices1$obsnum == 1,]
vertices2 <- dcast(vertices1, email ~ paste('company',obsnum,sep = '.'), value.var = 'company')
vertices <- merge(vertices, vertices2, by = 'email', all = T)
vertices$type <- vertices$role.1 == 'Agent'
max(sequence(rle(vertices$email)$lengths))
sum(is.na(vertices$email))
#write.csv(d, file = '~/Development/Data/remax_nodes.csv', row.names = F)

remax_network_lenders <- graph_from_data_frame(d=d, vertices=vertices, directed=T) 
remax_network_lenders <- simplify(remax_network_lenders, remove.multiple = F, remove.loops = T) 

V(remax_network_lenders)$color <- ifelse(V(remax_network_lenders)$role.1 == 2, "green", "gold")
V(remax_network_lenders)$label <- NA
V(remax_network_lenders)$size <- 3
V(remax_network_lenders)$frame.color <- "white"
V(remax_network_lenders)$type <- V(remax_network_lenders)$role.1 == 'Agent'
E(remax_network_lenders)$arrow.size <- 0
E(remax_network_lenders)$edge.color <- "gray50"
E(remax_network_lenders)$width <- (E(remax_network_lenders)$tx_count)
E(remax_network_lenders)$arrow.mode <- 0

l <- layout_on_sphere(remax_network_lenders)
l <- layout_with_fr(remax_network_lenders)

l <- layout_with_kk(remax_network_lenders)
l <- layout_with_lgl(remax_network_lenders)
l <- layout_with_mds(remax_network_lenders)
l <- layout_with_drl(remax_network_lenders)
l <- layout_nicely(remax_network_lenders)
l <- layout.reingold.tilford(remax_network_lenders, circular=T)
l <- layout_on_grid(remax_network_lenders)
remax_network_lenders.bp <- bipartite.projection(remax_network_lenders)
plot(remax_network_lenders.bp$proj1, laoyout = layout_with_lgl)
plot(remax_network_lenders.bp$proj2, laoyout = layout_with_lgl)
plot(remax_network_lenders, layout=l)


d <- cbind(aff_mat_remax_lenders$agent_email, aff_mat_remax_lenders$contact_email_domain, aff_mat_remax_lenders$tx_count)
d <- d[!is.na(d[,1]) & !is.na(d[,2]),]
colnames(d) <- c('Agent','Contact','tx_count')
#write.csv(d, file = '~/Development/Data/remax_edges.csv', row.names = F)
vertices1 <- aff_mat_remax_lenders[!duplicated(aff_mat_remax_lenders[,c('agent_email', 'company', 'role')]),c('agent_email', 'company', 'role')]
vertices2 <- aff_mat_remax_lenders[!duplicated(aff_mat_remax_lenders[,c('contact_email_domain', 'contact_company', 'contact_role')]) & !is.na(aff_mat_remax_lenders$contact_email),c('contact_email_domain', 'contact_company', 'contact_role')]
colnames(vertices1) <- c('email', 'company', 'role')
colnames(vertices2) <- c('email', 'company', 'role')
vertices <- rbind(vertices1, vertices2)
vertices <- vertices[order(vertices$email, vertices$company, vertices$role),]
vertices$obsnum <- sequence(rle(vertices$email)$lengths)
vertices1 <- dcast(vertices, email + company ~ paste('role',obsnum,sep = '.'), value.var = 'role')
vertices1$obsnum <- sequence(rle(vertices1$email)$lengths)
vertices <- vertices1[vertices1$obsnum == 1,]
vertices2 <- dcast(vertices1, email ~ paste('company',obsnum,sep = '.'), value.var = 'company')
vertices <- merge(vertices, vertices2, by = 'email', all = T)
vertices$type <- vertices$role.1 == 'Agent'
max(sequence(rle(vertices$email)$lengths))
sum(is.na(vertices$email))

remax_network_lenders_d <- graph_from_data_frame(d=d, vertices=vertices, directed=T) 
remax_network_lenders_d <- simplify(remax_network_lenders_d, remove.multiple = F, remove.loops = T) 
V(remax_network_lenders_d)$color <- ifelse(V(remax_network_lenders_d)$role.1 == 2, "green", "gold")
V(remax_network_lenders_d)$label <- NA
V(remax_network_lenders_d)$size <- 3
V(remax_network_lenders_d)$frame.color <- "white"
V(remax_network_lenders_d)$type <- V(remax_network_lenders_d)$role.1 == 'Agent'
E(remax_network_lenders_d)$arrow.size <- 0
E(remax_network_lenders_d)$edge.color <- "gray50"
E(remax_network_lenders_d)$width <- (E(remax_network_lenders_d)$tx_count)
E(remax_network_lenders_d)$arrow.mode <- 0

l <- layout_on_sphere(remax_network_lenders_d)
l <- layout_with_lgl(remax_network_lenders_d)
l <- layout_with_drl(remax_network_lenders_d)
l <- layout_on_grid(remax_network_lenders_d)
l <- layout.reingold.tilford(remax_network_lenders_d, circular=F)
l <- layout_nicely(remax_network_lenders_d)

l <- layout_with_mds(remax_network_lenders_d)
l <- layout_with_fr(remax_network_lenders_d)
l <- layout_with_kk(remax_network_lenders_d)
plot(remax_network_lenders_d, layout=l)

top_lenders <- c('@eaglehm.com'
    , '@rate.com'
    , '@usaa.com'
    , '@yahoo.com'
    , '@bankofamerica.com'
    , '@lendtheway.com'
    , '@financeofamerica.com'
    , '@navyfederal.org'
    , '@caliberhomeloans.com'
    , '@primeres.com'
    , '@supremelending.com'
    , '@chase.com'
    , '@guildmortgage.net'
    , '@primelending.com'
    , '@veteransunited.com'
    , '@fairwaymc.com'
    , '@movement.com'
    , '@gmail.com'
    , '@quickenloans.com'
    , '@wellsfargo.com')

#Subset to role here
aff_mat_remax_lenders_top20 <- aff_mat_remax_lenders[aff_mat_remax_lenders$contact_email_domain %in% top_lenders,]
d <- cbind(aff_mat_remax_lenders_top20$agent_email, aff_mat_remax_lenders_top20$contact_email_domain, aff_mat_remax_lenders_top20$tx_count)
d <- d[!is.na(d[,1]) & !is.na(d[,2]),]
colnames(d) <- c('Agent','Contact','tx_count')
#write.csv(d, file = '~/Development/Data/remax_edges.csv', row.names = F)
vertices1 <- aff_mat_remax_lenders_top20[!duplicated(aff_mat_remax_lenders_top20[,c('agent_email', 'company', 'role')]),c('agent_email', 'company', 'role')]
vertices2 <- aff_mat_remax_lenders_top20[!duplicated(aff_mat_remax_lenders_top20[,c('contact_email_domain', 'contact_company', 'contact_role')]) & !is.na(aff_mat_remax_lenders_top20$contact_email),c('contact_email_domain', 'contact_company', 'contact_role')]
colnames(vertices1) <- c('email', 'company', 'role')
colnames(vertices2) <- c('email', 'company', 'role')
vertices <- rbind(vertices1, vertices2)
vertices <- vertices[order(vertices$email, vertices$company, vertices$role),]
vertices$obsnum <- sequence(rle(vertices$email)$lengths)
vertices1 <- dcast(vertices, email + company ~ paste('role',obsnum,sep = '.'), value.var = 'role')
vertices1$obsnum <- sequence(rle(vertices1$email)$lengths)
vertices <- vertices1[vertices1$obsnum == 1,]
vertices2 <- dcast(vertices1, email ~ paste('company',obsnum,sep = '.'), value.var = 'company')
vertices <- merge(vertices, vertices2, by = 'email', all = T)
vertices$type <- vertices$role.1 == 'Agent'
max(sequence(rle(vertices$email)$lengths))
sum(is.na(vertices$email))

remax_network_lenders_d20 <- graph_from_data_frame(d=d, vertices=vertices, directed=T) 
remax_network_lenders_d20 <- simplify(remax_network_lenders_d20, remove.multiple = F, remove.loops = T) 
V(remax_network_lenders_d20)$color <- ifelse(V(remax_network_lenders_d20)$role.1 == 2, "green", "gold")
V(remax_network_lenders_d20)$label <- NA
V(remax_network_lenders_d20)$size <- 3
V(remax_network_lenders_d20)$frame.color <- "white"
V(remax_network_lenders_d20)$type <- V(remax_network_lenders_d20)$role.1 == 'Agent'
E(remax_network_lenders_d20)$arrow.size <- 0
E(remax_network_lenders_d20)$edge.color <- "gray50"
E(remax_network_lenders_d20)$width <- (E(remax_network_lenders_d20)$tx_count)
E(remax_network_lenders_d20)$arrow.mode <- 0

V(remax_network_lenders_d20)$label <- ""
V(remax_network_lenders_d20)$label[V(remax_network_lenders_d20)$type==F] <- vertices$email[V(remax_network_lenders_d20)$type==F] 
V(remax_network_lenders_d20)$label.cex=.6
V(remax_network_lenders_d20)$label.font=2

l <- layout_with_kk(remax_network_lenders_d20)
plot(remax_network_lenders_d20, layout=l)


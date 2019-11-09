#Clear Workspace
rm(list=ls())

#Load libraries
source('~/workspace/datascience/functions/load_libraries.R')
load_libraries('~/workspace/datascience/functions/')

#Set Seed
set.seed(123456789)

#Connect to local PSQL database and extract data
local_connection <- local_db_conect('development')

affiliation_matrix <- dbGetQuery(local_connection, 
                        "SELECT gt.id
                            , gt.email as agent_email
                            , p.company
                            , gc.email as contact_email
                            , gc.role as contact_role
                            , pc.company as contact_company
                            , count(property_transaction_id) as tx_count
                        FROM public.gmail_tokens gt
                        LEFT JOIN public.property_transactions pt on gt.id = pt.external_account_id
                        LEFT JOIN prod.contacts gc on gc.property_transaction_id = pt.id
                        LEFT JOIN public.people p on gt.email = p.email
                        LEFT JOIN public.users u on p.user_id = u.id
                        LEFT JOIN public.people pc on pc.email = gc.email
                        WHERE u.type = 'AgentUser'
                        GROUP BY gt.id, gt.email, p.company, gc.email, gc.role, pc.company"
                        )

kill_db_connections()

#Set left hand role to agent
affiliation_matrix$role <- 'Agent'

#Convert factors to characters
i <- sapply(affiliation_matrix,is.factor)
affiliation_matrix[i] <- lapply(affiliation_matrix[i], as.character)

#Publish Affiliation Matrix
dbWriteTable(local_connection, c('datasci_projects','professional_networks_affiliation_matrix'), value = affiliation_matrix, overwrite = T, append = F, row.names = FALSE)

#Subset to contacts between agents and lenders
affiliation_matrix_lenders <- affiliation_matrix[affiliation_matrix$contact_role == 2 & !is.na(affiliation_matrix$contact_role),]
#Get email domains
affiliation_matrix_lenders$contact_email_domain <- regexpr('@',affiliation_matrix_lenders$contact_email)
affiliation_matrix_lenders$contact_email_domain <- substr(affiliation_matrix_lenders$contact_email,affiliation_matrix_lenders$contact_email_domain,nchar(affiliation_matrix_lenders$contact_email))
affiliation_matrix_lenders$contact_email_domain <- tolower(affiliation_matrix_lenders$contact_email_domain)

#Get list of lender domains
affiliation_matrix_lenders$one <- 1
lender_domains <- aggregate(cbind(one,tx_count)~contact_email_domain, data = affiliation_matrix_lenders, FUN = sum, na.rm = T)
lender_domains <- lender_domains[!(lender_domains$contact_email_domain %in% c('gmail.com','yahoo.com')),]
nrow(lender_domains)
lender_domains <- lender_domains[lender_domains$tx_count > 4,]
lender_domains <- lender_domains[lender_domains$one > 4,]


#Publish Affiliation Matrix - Lenders
dbWriteTable(local_connection, c('datasci_projects','professional_networks_affiliation_matrix_lenders'), value = affiliation_matrix_lenders, overwrite = T, append = F, row.names = FALSE)
#affiliation_matrix_lenders <- 

#Sample?
thresh <- .1
lender_domains_rand <- runif(nrow(lender_domains))
affiliation_matrix_lenders_samp <- affiliation_matrix_lenders[affiliation_matrix_lenders$contact_email_domain %in% lender_domains$contact_email_domain[lender_domains_rand<thresh],]

affiliation_matrix_lenders_samp_agg <- aggregate(tx_count~agent_email + contact_email_domain + role + contact_role, data = affiliation_matrix_lenders_samp, FUN = sum, na.rm = T)

#Create edge list d
d <- cbind(affiliation_matrix_lenders_samp_agg$agent_email, affiliation_matrix_lenders_samp_agg$contact_email_domain, affiliation_matrix_lenders_samp_agg$tx_count)
d <- d[!is.na(d[,1]) & !is.na(d[,2]),]
colnames(d) <- c('Agent','Lender','tx_count')

#Create vertices (Agents (1) and contacts (2))
vertices1 <- as.data.frame(affiliation_matrix_lenders_samp_agg[!duplicated(affiliation_matrix_lenders_samp_agg[,c('agent_email')]),c('agent_email', 'role')])
vertices2 <- as.data.frame(affiliation_matrix_lenders_samp_agg[!duplicated(affiliation_matrix_lenders_samp_agg[,c('contact_email_domain', 'contact_role')]) & !is.na(affiliation_matrix_lenders_samp_agg$contact_email_domain),c('contact_email_domain','contact_role')])
colnames(vertices1) <- c('node', 'role')
colnames(vertices2) <- c('node', 'role')

#Combine vertices 
vertices <- rbind(vertices1, vertices2)
vertices <- vertices[order(vertices$node),]

#Cast Wide for contacts with multiple roles
#vertices$obsnum <- sequence(rle(vertices$node)$lengths)
#vertices1 <- dcast(vertices, node ~ paste('role',obsnum,sep = '.'), value.var = 'role')
#vertices1$obsnum <- sequence(rle(vertices1$email)$lengths)
#vertices <- vertices1[vertices1$obsnum == 1,]
#vertices2 <- dcast(vertices1, email ~ paste('company',obsnum,sep = '.'), value.var = 'company')
#vertices <- merge(vertices, vertices2, by = 'email', all = T)
max(sequence(rle(vertices$email)$lengths))
sum(is.na(vertices$email))
nrow(vertices)

#Create professional network graph (agents / lenders)
pn_agents_lenders <- graph_from_data_frame(d=d, vertices=vertices, directed=T) 
pn_agents_lenders <- simplify(pn_agents_lenders, remove.multiple = F, remove.loops = T) 
V(pn_agents_lenders)$color <- ifelse(V(pn_agents_lenders)$role == 2, "green", "gold")
V(pn_agents_lenders)$label <- NA
V(pn_agents_lenders)$size <- 3
V(pn_agents_lenders)$frame.color <- "white"
V(pn_agents_lenders)$type <- V(pn_agents_lenders)$role == 'Agent'
E(pn_agents_lenders)$arrow.size <- 0
E(pn_agents_lenders)$edge.color <- "gray50"
E(pn_agents_lenders)$width <- (E(pn_agents_lenders)$tx_count)
E(pn_agents_lenders)$arrow.mode <- 0

#l <- layout_on_sphere(pn_agents_lenders)
#plot(pn_agents_lenders, layout=l, title = nrow(vertices))
#l <- layout_with_mds(pn_agents_lenders)
#plot(pn_agents_lenders, layout=l)

#l <- layout_with_fr(pn_agents_lenders)
#plot(pn_agents_lenders, layout=l)

#l <- layout_with_drl(pn_agents_lenders)
#plot(pn_agents_lenders, layout=l)
l <- layout_with_kk(pn_agents_lenders)
plot(pn_agents_lenders, layout=l)
l <- layout_with_lgl(pn_agents_lenders)
plot(pn_agents_lenders, layout=l)
l <- layout_nicely(pn_agents_lenders)
plot(pn_agents_lenders, layout=l)
legend(x=-1, y=-.35,c("Agent","Lender"), pch=21, pt.bg=c("gold","green"))

l <- layout.reingold.tilford(pn_agents_lenders, circular=T)
plot(pn_agents_lenders, layout=l)
l <- layout_on_grid(pn_agents_lenders)
plot(pn_agents_lenders, layout=l)

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


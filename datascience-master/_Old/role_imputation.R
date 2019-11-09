rm(list=ls())

#Load libraries
source('~/workspace/datascience/functions/load_libraries.R')
load_libraries('~/workspace/datascience/functions/', include_bundles = c('utility'))

#Set Seed
set.seed(123456789)
connection_target <- db_connect()
connection_source <- db_connect('AMITREE_READ_ONLY_URL')

#Role definitions
#buyer: 1,
#lender: 2,
#inspector: 3,
#attorney: 4,
#buyer_agent: 5,
#contractor: 6,
#escrow_officer: 7,
#insurance_agent: 8,
#mover: 9,
#seller: 10,
#seller_agent: 11,
#title_officer: 12,
#other: 13,
#assistant: 14,
#broker: 15,
#listing_agent: 16,
#partner_agent: 17,
#transaction_coordinator: 18

#Pull contact data
contacts <- dbGetQuery(connection_target, "SELECT * FROM datasci.lk_contacts_to_business_objects;")

#Convert NULL role value to zero
contacts$role[is.na(contacts$role)] <- 0

#Aggregate roles by email_address_id and merge back into contacts
tmp <- aggregate(contacts$role, by = list(contacts$email_address_id), sum)
tmp$x <- ifelse(tmp$x == 0, 0, 1)
contacts <- merge(contacts, tmp, by.x = 'email_address_id', by.y = 'Group.1')
colnames(contacts)[which(colnames(contacts) == 'x')] <- 'assigned_role'

#Find contact-object_type frequency and merge
contacts_prop <- as.data.frame(table(contacts$email_address_id[contacts$business_object_identifier == 'property_transaction_id']))
contacts_prop <- merge(contacts[contacts$business_object_identifier == 'property_transaction_id',], 
	contacts_prop, by.x = 'email_address_id', by.y = 'Var1')
contacts_cli <- as.data.frame(table(contacts$email_address_id[contacts$business_object_identifier == 'prospect_folder_id']))
contacts_cli <- merge(contacts[contacts$business_object_identifier == 'prospect_folder_id',], 
	contacts_cli, by.x = 'email_address_id', by.y = 'Var1')
contacts <- rbind(contacts_prop, contacts_cli)

role.ids <- c(0:18)

multi_cont <- contacts[contacts$Freq > 1 & contacts$business_object_identifier == 'property_transaction_id',]

mat_gnar2 <- tapply(multi_cont$role[multi_cont$email_address_id == 22364470],
 multi_cont$email_address_id[multi_cont$email_address_id == 22364470], function(x){
  if(length(x) < 100000){
  #print(x)
  count_mat <- matrix(0L,ncol = length(role.ids), nrow = length(role.ids), dimnames = list(role.ids, role.ids))
  count_mat2 <- count_mat
  mat <- as.data.frame.matrix(table(data.frame(t(combn(x, 2))))) #fix needed here for conversion to numeric
  mat2 <- t(mat)
  #mat2 <- as.data.frame.matrix(t(table(data.frame(t(combn(x, 2))))))
  count_mat[row.names(mat), colnames(mat)]<-as.matrix(mat)
  count_mat2[row.names(mat2), colnames(mat2)]<-as.matrix(mat2)
  return((count_mat + count_mat2))}
})

mats_clean <- Filter(Negate(is.null), mats)

mats_clean <- lapply(mats_clean, function(x){
	tmp <- as.numeric(x)
	tmp <- matrix(tmp, ncol = length(colnames(x)), nrow = length(row.names(x)), dimnames = list(row.names(x), colnames(x)))
	return(tmp)
	})

adj_mat_prop_tx <- Reduce('+', mats_clean)

tmp <- lapply(seq_along(mats_clean), function(x) {
	print(paste(x, ':', mats_clean[[x]][1,1]))
	return(data.frame(e = x, n = mats_clean[[x]][1,1]))})
tmp <- ldply(tmp, rbind)

relations <- dbGetQuery(connection_target, "SELECT dslcbo.*, dspt.external_account_id 
											FROM datasci.lk_contacts_to_business_objects dslcbo
											LEFT JOIN datasci.property_transactions dspt ON dspt.property_transaction_id = dslcbo.business_object_id_value
											WHERE dslcbo.business_object_identifier = 'property_transaction_id'
											UNION
											SELECT dslcbo.*, dsc.external_account_id 
											FROM datasci.lk_contacts_to_business_objects dslcbo
											LEFT JOIN datasci.clients dsc ON dsc.prospect_folder_id = dslcbo.business_object_id_value
											WHERE dslcbo.business_object_identifier = 'prospect_folder_id'")

relations$role[is.na(relations$role)] <- 0

tmp <- aggregate(relations$role, by = list(relations$email_address_id), sum)
tmp$x <- ifelse(tmp$x == 0, 0, 1)
relations <- merge(relations, tmp, by.x = 'email_address_id', by.y = 'Group.1')
colnames(relations)[which(colnames(relations) == 'x')] <- 'assigned_role'

relations_prop <- as.data.frame(table(relations$email_address_id[relations$business_object_identifier == 'property_transaction_id']))
relations_prop <- merge(relations[relations$business_object_identifier == 'property_transaction_id',], 
	relations_prop, by.x = 'email_address_id', by.y = 'Var1')
relations_cli <- as.data.frame(table(relations$email_address_id[relations$business_object_identifier == 'prospect_folder_id']))
relations_cli <- merge(relations[relations$business_object_identifier == 'prospect_folder_id',], 
	relations_cli, by.x = 'email_address_id', by.y = 'Var1')
relations <- rbind(relations_prop, relations_cli)

tmp <- dcast(relations, email_address_id + external_account_id ~ role, fun.aggregate = length, value.var = 'id')

dyads <- data.frame()

dyad_cols <- c('email_address_id', 'business_object_identifier.x', 'business_object_id_value.x', 'role.x', 'business_object_id_value.y', 'role.y')

seg <- 1000

#Breaking dyads into chunks for merging
for(i in 1:ceiling(length(unique(multi_cont$email_address_id))/seg)) {
	if(i == 1){tmp <- multi_cont[multi_cont$email_address_id > sort(unique(multi_cont$email_address_id), decreasing = T)[i * seg],]
		} else if (i == ceiling(length(unique(multi_cont$email_address_id))/seg)){
			tmp <- multi_cont[multi_cont$email_address_id <= sort(unique(multi_cont$email_address_id), decreasing = T)[(i-1) * seg],]
		}
		else {
			tmp <- multi_cont[multi_cont$email_address_id > sort(unique(multi_cont$email_address_id), decreasing = T)[i * seg] &
			multi_cont$email_address_id <= sort(unique(multi_cont$email_address_id), decreasing = T)[(i-1) * seg],]
		}
	print(i)
	print('merging')
	tmp <- merge(tmp, tmp, by = 'email_address_id')
	tmp <- tmp[tmp$business_object_id_value.x != tmp$business_object_id_value.y, ]
	print('mutating')
	tmp <- mutate(tmp, id = f(tmp$business_object_id_value.x, tmp$business_object_id_value.y))
	tmp <- tmp[!duplicated(tmp$id), dyad_cols]
	print('rbinding')
	dyads <- rbind(dyads, tmp)
	print(i)
}

dyads <- merge(dyads, dyads, by = 'email_address_id')

dyads <- ldply(lapply(split(contacts[contacts$Freq > 1,], contacts[contacts$Freq > 1, 'email_address_id']), function(x) {
	tmp <- merge(x, x, by = 'email_address_id')
	tmp <- tmp[tmp$business_object_id_value.x != tmp$business_object_id_value.y, ]
	tmp <- mutate(tmp, id = f(tmp$business_object_id_value.x, tmp$business_object_id_value.y))
	tmp <- tmp[!duplicated(tmp$id),]
	return(tmp)
	}), rbind)
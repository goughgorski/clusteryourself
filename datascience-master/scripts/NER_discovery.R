#Clear Environment
rm(list=ls())

#Load libraries
source('~/workspace/datascience/functions/load_libraries.R')
load_libraries('~/workspace/datascience/functions/', include_bundles = c('utility', 'text'))
library(NLP)
library(openNLP) # needed to install Java fix beforehand: 'https://cran.r-project.org/src/contrib/Archive/rJava/rJava_0.9-9.tar.gz'
library(openNLPmodels.en) # installed from 'https://datacube.wu.ac.at/src/contrib/openNLPmodels.en_1.5-1.tar.gz'
library(tidystringdist)
library(igraph)
library(magrittr)

#Set Seed
set.seed(123456789)

#Connect to Local PSQL Database AND Pull Data
connection <- db_connect()

messages_full <- dbGetQuery(connection, "SELECT m.*, d.*, sf.smart_folder_id, sf.user_id 
										FROM datasci_projects.transaction_messages m
										LEFT JOIN datasci_projects.transaction_documents d
										ON m.message_id = d.message_id
										LEFT JOIN datasci.smart_folders sf
										ON sf.business_object_id = m.property_transaction_id
										WHERE m.message_id IS NOT NULL
										AND d.message_id IS NOT NULL")

messages <- messages_full[, c('message_id', 'smart_folder_id', 'user_id', 'from', 'to', 'sender', 'cc', 'subject','body_text', 'body_html', 
								'attachment_name', 'document_content', 'date')]

sets <- data.frame(user_id = unique(messages$user_id), rand = runif(length(unique(messages$user_id))))
sets$set <- ifelse(sets$rand < 0.7, 'train', ifelse(sets$rand >= 0.9, 'validation', 'test'))

messages <- merge(messages, sets[, c('user_id', 'set')], by = 'user_id')

#Entity extraction
text <- messages[1, 'body_text']
text <- "This is a message about Sir Edmund Hillary climbing Mount Everest but not Salt Lake City"

#functions from Apache OpenNLP models
sent_annot <- Maxent_Sent_Token_Annotator()
word_annot <- Maxent_Word_Token_Annotator()
loc_annot <-  Maxent_Entity_Annotator(kind = "location") #annotate location
people_annot <- Maxent_Entity_Annotator(kind = "person") #annotate person

annot.l1 <- NLP::annotate(text, list(sent_annot,word_annot,loc_annot,people_annot))

k <- sapply(annot.l1$features, `[[`, "kind")
locations <- sapply(annot.l1[k == "location"], function(x) {substr(text, x$start, x$end)})
people <- sapply(annot.l1[k == "person"], function(x) {substr(text, x$start, x$end)})

entity_grouping <- function(found_entities) {
	# user 377365 example of merging groups
	# user 255884 not correctly grouping entities

	# remove duplicated found entities
	unik <- found_entities[!duplicated(found_entities)]

	# create pre-process function
	text_pre_process <- function(text) {
		sapply(text, function(x) {gsub('^[:print:]','', x)}) %>% sapply(tolower) %>% 
		sapply(function(x) {gsub("[^\001-\177]",'', x, perl = TRUE)}) %>%
		sapply(function (x) {removePunctuation(x)}) %>%
		sapply(function(x) {stripWhitespace(x)}) %>% unname()
	}

	# pre-process entities, remove emptysets, and find fuzzy matches
	ent <- text_pre_process(unik) %>% subset(. != "") %>% lapply(function(x) {agrep(x, ., max.distance = 0.2)})

	ent <- lapply(seq_along(ent), function(e) {strsplit(text_pre_process(unik[ent[[e]]]), " ") %>% 
		lapply(function(x) {intersect(strsplit(text_pre_process(unik[e]), " ")[[1]], x)}) %>% lapply(function(x) {length(x) > 0}) %>%
		unlist() %>% subset(ent[[e]], .)})

	if (all(lapply(ent, length) <= 1)) {

		matches <- merge(data.frame(id = 1:length(found_entities), found_entities = found_entities, stringsAsFactors = FALSE), 
		data.frame(found_entities = unik, grp_name = text_pre_process(unik), stringsAsFactors = FALSE), by = 'found_entities')

		return(matches[order(matches$id),][, c('id', 'found_entities', 'grp_name')])}

	# group entities based on fuzzy matches
	ent <- suppressWarnings(lapply(ent, function(x) {if(length(x) > 1) {as.data.frame(t(combn(x, 2)))}}) %>%
		ldply(rbind) %>% subset(select = c('V1', 'V2')) %>% 
		graph_from_data_frame() %>% max_cliques(min = 2) %>% 
		lapply(function(x) {as.numeric(names(x))}))

	# for each group of entities matched, find the overlapping string(s)
	unients <- ldply(lapply(ent, function(x) {

		tmp <- unik[x] %>% text_pre_process() %>%
		sapply(function(x) {strsplit(x," ")})

		for (i in 1:(length(tmp))) 
			{if (i == 1) {unient <- c()
				unient <- intersect(tmp[[i]], tmp[[i + 1]])}
			else {intersect(unient, tmp[[i]])}
			}

		return(paste(unient, collapse = ' '))
		
		}), rbind)

	# assign each entity a group
	unient_groups <- suppressWarnings(ldply(lapply(seq_along(ent), function(x) 
		{data.frame(ent = x, grp_name = unients[x, 1], grp = ent[[x]])}), rbind))

	# find any entities that belong to more than one group for merging
	freq <- as.data.frame(table(unient_groups$grp)) %>% subset(Freq > 1)

	unient_groups_to_merge <- merge(unient_groups, freq, by.x = 'grp', by.y = 'Var1')[, c('grp', 'ent')] %>%
		split(.[, 'grp']) %>% lapply(function(x) {c(x[, 'ent'])}) %>%
		lapply(function(x) {unique(unlist(ent[x]))})

	# merge group names
	unient_groups$grp_name <- as.character(unient_groups$grp_name)
	if (length(unient_groups_to_merge) > 0) {
		for (i in 1:length(unient_groups_to_merge)){
		unient_groups$grp_name[unient_groups$grp %in% unient_groups_to_merge[[i]]] <- 
		paste(sort(unique(unient_groups$grp_name[unient_groups$grp %in% unient_groups_to_merge[[i]]])), collapse = ' ')
	}}

	unient_groups <- unient_groups[!duplicated(unient_groups$grp),]
	unik <- merge(data.frame(id = 1:length(unik), found_entities = unik, stringsAsFactors = FALSE),
	 unient_groups[, c('grp_name', 'grp')], by.x = 'id', by.y = 'grp', all.x = TRUE)
	unik$grp_name[is.na(unik$grp_name)] <- text_pre_process(unik$found_entities[is.na(unik$grp_name)])

	matches <- merge(data.frame(id = 1:length(found_entities), found_entities = found_entities, stringsAsFactors = FALSE), 
		unik[, c('found_entities', 'grp_name')], by = 'found_entities')

	return(matches[order(matches$id),][, c('id', 'found_entities', 'grp_name')])

}

#Split messages by user and test entity recognition and grouping for kind = 'person'
	#User_id 2757 returns no person entities despite several visually observable persons
user_ent <- lapply(split(messages[messages$set == 'train', ], messages[messages$set == 'train', 'user_id'], drop = TRUE),
 function(x) {
print(unique(x[, 'user_id']))
	ent_person <- lapply(x[, 'body_text'], function(y) {
	if (length(annotate(y, Maxent_Sent_Token_Annotator(probs = TRUE))) > 0) {
		tmp <- NLP::annotate(y, list(sent_annot,word_annot,loc_annot,people_annot))
		k <- sapply(tmp$features, `[[`, "kind")
		sapply(tmp[k == "person"], function(z) {substr(y, z$start, z$end)})
	}
	}) %>% unlist() %>% unname()

if(is.null(ent_person)) {return(NULL)}

entity_grouping(ent_person)

	})

#Group entities using fuzzy matching
compare <- tidy_comb_all(people[!duplicated(people)], comp)

comparisons <- tidy_stringdist(compare) %>% #osa, lv, dl are optimal string alignment distances
  mutate(sim = 1-jaccard) %>% 
  select(V1, V2, sim)

recommendation <- comparisons %>% 
  group_by(V1) %>% 
  summarise(max_sim = max(sim)) %>% 
  ungroup()

comparisons %>% 
  inner_join(recommendation, by = c("V1" = "V1", "sim" = "max_sim"))







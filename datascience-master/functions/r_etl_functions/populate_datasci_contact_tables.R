
populate_datasci_contact_tables <- function(connection_source, connection_target) {

    #Check to see if Tables Exist
    required_tables <- c('contacts', 'lk_contacts_to_business_objects')
    for (i in 1:length(required_tables)) {
      table_check <- dbExistsTable(connection_target, c("datasci",required_tables[i]))
      if (!table_check) {stop(print(paste(required_tables[i], " table is missing")))}
    }

    #Check to see if Tables Exist
    required_tables <- c('contacts')
    for (i in 1:length(required_tables)) {
      table_check <- dbExistsTable(connection_target, c("public",required_tables[i]))
      if (!table_check) {stop(print(paste(required_tables[i], " table is missing")))}
    }

    print(noquote("Populating Contacts table in datasci."))

    last_insterted_contact <- dbGetQuery(connection_target, "SELECT MAX(TIMEZONE('UTC', created_at)) FROM datasci.contacts")

    if (is.na(last_insterted_contact[1,1])){last_insterted_contact <- as.data.frame('1-1-1900')}

    process_contacts <- dbGetQuery(connection_source, paste("SELECT DISTINCT LOWER(email) FROM public.contacts WHERE created_at >=",paste("'",last_insterted_contact[1,1],"'", sep = ''), "OR updated_at >=",paste("'",last_insterted_contact[1,1],"'", sep = '')))
    
    new_contacts <- data.frame(email = process_contacts[!is.na(process_contacts$email),])

    new_contacts$email <- tolower(as.character(new_contacts$email))

    existing_contacts <- dbGetQuery(connection_target, "SELECT email FROM datasci.contacts")

    new_contacts <- new_contacts[!new_contacts[,'email'] %in% tolower(existing_contacts[,'email']),'email']

    dbWriteTable(connection_target, c('datasci','contacts'), value = data.frame(email = new_contacts), overwrite = FALSE, append = TRUE, row.names = FALSE)

    existing_contacts <- dbGetQuery(connection_target, "SELECT * FROM datasci.contacts")
    
    print(noquote("Populating Contacts lookup table in datasci."))

    last_insterted_lk_contact <- dbGetQuery(connection_target, "SELECT MAX(created_at) FROM datasci.lk_contacts_to_business_objects")

    if (is.na(last_insterted_lk_contact[1,1])){last_insterted_lk_contact <- as.data.frame('1-1-1900')}

    process_lk_contacts <- dbGetQuery(connection_source, paste("SELECT * FROM public.contacts WHERE created_at >=",paste("'",last_insterted_contact[1,1],"'", sep = ''), "OR updated_at >=",paste("'",last_insterted_contact[1,1],"'", sep = '')))

    lk_cols <- unlist(dbGetQuery(connection_target, "SELECT column_name 
                         FROM information_schema.columns 
                         WHERE table_name = 'lk_contacts_to_business_objects'
                         and table_schema = 'datasci'
                         AND column_default IS NULL"))

    lk_object_to_id <- rbind(cbind(business_object_type = 'PropertyTransaction', business_object_identifier = 'property_transaction_id')
                        , cbind(business_object_type = 'ProspectFolder', business_object_identifier = 'prospect_folder_id'))

    process_lk_contacts <- merge(process_lk_contacts, lk_object_to_id, by = 'business_object_type', sort = FALSE)
    colnames(process_lk_contacts)[colnames(process_lk_contacts) == 'business_object_id'] <- 'business_object_id_value'

    lk_bo_contacts <- merge(process_lk_contacts, existing_contacts, by = 'email', all.x = TRUE, sort = FALSE)
    colnames(lk_bo_contacts)[colnames(lk_bo_contacts) == 'id.y'] <- 'email_address_id'
    lk_bo_contacts <- lk_bo_contacts[!is.na(lk_bo_contacts$email_address_id),]        
    lk_bo_contacts <- lk_bo_contacts[!duplicated(lk_bo_contacts[,c('business_object_identifier', 'business_object_id_value', 'role', 'email_address_id')]),]
    
    i <- sapply(lk_bo_contacts,is.factor)
    lk_bo_contacts[i] <- lapply(lk_bo_contacts[i], as.character)

    existing_lk_bo_contacts <- dbGetQuery(connection_target, "SELECT * FROM datasci.lk_contacts_to_business_objects")    
    
    existing_lk_bo_contacts$exists <- TRUE
    lk_bo_contacts <- merge(lk_bo_contacts, existing_lk_bo_contacts, by = lk_cols, all.x = TRUE, sort = FALSE)
    lk_bo_contacts$exists <- ifelse(is.na(lk_bo_contacts$exists),FALSE,lk_bo_contacts$exists)
    lk_bo_contacts <- lk_bo_contacts[!lk_bo_contacts$exists,lk_cols]

    dbWriteTable(connection_target, c('datasci','lk_contacts_to_business_objects'), value = lk_bo_contacts[,lk_cols], overwrite = FALSE, append = TRUE, row.names = FALSE)
    
    } 


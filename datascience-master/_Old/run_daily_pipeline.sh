#!/bin/bash
 date
 #Set Environment Variables
 cd ~/workspace/datascience && git checkout master
 source ./.envrc
 
 #Close active ruby data collection processes
 echo "Shutting down active Ruby data collection processes at $(date)"
 pgrep -o ruby | xargs kill -USR1  
 while pgrep -o ruby > /dev/null; do sleep 5; done
 echo "Completed data collection shutdown at $(date)"
 
 #####Optional Backup TIGER PostGIS data and ACS data####
 #${PGPATH}/pg_dump -d ${DATABASE} -h ${HOST} -p ${PORT} -U ${PGUSER} -v -x --serializable-deferrable -n '(tiger|topology)*' -Fc > ${BACKUPDIR}/census/TIGER_`date +%s`.dump 
 #${PGPATH}/pg_dump -d ${DATABASE} -h ${HOST} -p ${PORT} -U ${PGUSER} -v -x --serializable-deferrable -n 'acs*' -Fc > ${BACKUPDIR}/census/ACS_`date +%s`.dump 
 
 #Optional Restore TIGER PostGIS data and ACS data
 #${PGPATH}/pg_restore -d ${DATABASE} -h ${HOST} -p ${PORT} -U ${PGUSER} --no-owner --role=${PGUSER} -v -j5 --clean -Fc "/Users/luke/Development/Data/TIGER_1062017.dump"
 #${PGPATH}/pg_restore -d ${DATABASE} -h ${HOST} -p ${PORT} -U ${PGUSER} --no-owner --role=${PGUSER} -v -j5 -Fc ${BACKUPDIR}/census/ACS_`date +%s`.dump 
 
 
 ####Basic DB Processing####
 ${PGPATH}/pg_ctl -D /usr/local/var/postgres stop -s -m fast
 while pgrep -o pg_ctl > /dev/null; do sleep 5; done
 ${PGPATH}/pg_ctl -D /usr/local/var/postgres start &
 while pgrep -o pg_ctl > /dev/null; do sleep 5; done
 
 #Drop Data-Sci Views to enable sucessful PG DUMP
 ${PGPATH}/psql -d ${DATABASE} -h ${HOST} -p ${PORT} -U ${PGUSER} -w -t -f ${PSQLDIR}/vw_drop.sql > ~/tmp_vw_drop.sql
 ${PGPATH}/psql -d ${DATABASE} -h ${HOST} -p ${PORT} -U ${PGUSER}  -w -f ~/tmp_vw_drop.sql
 rm ~/tmp_vw_drop.sql
 
 #Create Nightly Data Science Backup
 schema='(datasci|datasci_projects|datasci_mining|datasci_modeling|datasci_research)'
DSFILE=${DATABASE}_${schema}_`date +%s`
${PGPATH}/pg_dump -d ${DATABASE} -h ${HOST} -p ${PORT} -U ${PGUSER} -v -x --serializable-deferrable -n ${schema} -Fc > ${BACKUPDIR}/${DSFILE}.dump 
 #Upload this to S3 with a timestamp prefix
#Upload this to S3 with a timestamp prefix on first and 15th
#TODO ADD if Statement with date here: date "+%d"
${AWSPATH}/aws s3 cp ${BACKUPDIR}/${DSFILE}.dump s3://amitree-datascience/db/ --acl authenticated-read
 #Pull Production Copy
 ~/workspace/datascience/data_pipeline/pg_run `${HEROKUPATH}/heroku config:get DATABASE_URL -r prod --app amitree` ${PGPATH}/pg_dump -v -x -Fc --serializable-deferrable -T sent_emails -T emails -T proxied_emails -T delayed_jobs -T delayed_job_events -T client_statuses -T contacts -f ${BACKUPDIR}/${DBNAME}.dump
 
 #Restore Production to local development
 schema=public
 ${PGPATH}/pg_restore -d ${DATABASE} -h ${HOST} -p ${PORT} -U ${PGUSER} --no-owner --role=${PGUSER} --schema=${schema} --clean -v -j5 -Fc ${BACKUPDIR}/${DBNAME}.dump
 
 #Optional Dump and Restore of contacts Table
 #~/workspace/datascience/data_pipeline/pg_run `${HEROKUPATH}/heroku config:get DATABASE_URL -r prod --app amitree` ${PGPATH}/pg_dump -v -x -Fc --serializable-deferrable -t contacts -f ${BACKUPDIR}/${DBNAME}_contacts.dump
 #${PGPATH}/pg_restore -d ${DATABASE} -h ${HOST} -p ${PORT} -U ${PGUSER} --no-owner --role=${PGUSER} --schema=${schema} -v -j5 -Fc ${BACKUPDIR}/${DBNAME}_contacts.dump
 
 #Recreate Views
 ${PGPATH}/psql -d ${DATABASE} -h ${HOST} -p ${PORT} -U ${PGUSER}  -w -f ${PSQLDIR}/vw_create.sql
 
 ###Data Science ETL####
 #R ETL Scripts
 ${RPATH}/Rscript ${RDIR}/pipeline_daily_etl_r.R
 
 #Census Geocoding for Property Transactions
 ${PGPATH}/psql -d ${DATABASE} -h ${HOST} -p ${PORT} -U ${PGUSER} -w -c "SELECT datasci.geocode_properties()"
 
 #Drop and Recreate Foriegn Schema connection to Production
 ${PGPATH}/psql -d ${DATABASE} -h ${HOST} -p ${PORT} -U ${PGUSER} -w -f ${PSQLDIR}/ForiegnSchema.sql
 
 #Pull external data and load
 sh ${SHDIR}/get_external_data.sh
 
 #Dump DataSci Modeling, Push to s3, load into Heroku, delete from s3
 schema='(datasci_modeling)'
 DSFILE=hjf_`date +%s`
 ${PGPATH}/pg_dump -d ${DATABASE} -h ${HOST} -p ${PORT} -U ${PGUSER} -v -x --serializable-deferrable -n ${schema} -Fc > ${BACKUPDIR}/${DSFILE}.dump 
 ${AWSPATH}/aws s3 cp ${BACKUPDIR}/${DSFILE}.dump s3://amitree-datascience/db/ --grants read=uri=http://acs.amazonaws.com/groups/global/AllUsers
 export DBURL=`${HEROKUPATH}/heroku config:get DATABASE_URL -a pa-r-abox`
 ${HEROKUPATH}/heroku pg:backups:restore -a pa-r-abox "https://s3.amazonaws.com/amitree-datascience/db/${DSFILE}.dump" DATABASE_URL --confirm pa-r-abox
 ${AWSPATH}/aws s3 rm s3://amitree-datascience/db/${DSFILE}.dump
 
 #Reset Server
 ${PGPATH}/pg_ctl -D /usr/local/var/postgres stop -s -m fast
 while pgrep -o pg_ctl > /dev/null; do sleep 5; done
 ${PGPATH}/pg_ctl -D /usr/local/var/postgres start &
 while pgrep -o pg_ctl > /dev/null; do sleep 5; done
 
 # ####Restart Data Collection
 # echo "Starting Ruby data collection processes at $(date)"
 # ##Process Proposed Txs for modeling set extraction####
 # #Populate New Tx Data in Proposed Tx Table
 # #TXLOG=~/Development/Data/ETL_Logging/tx_processing_`date +\%Y\%m\%d_\%H\%M\%S`.log
 # ${PGPATH}/psql -d ${DATABASE} -h ${HOST} -p ${PORT} -U ${PGUSER} -w -f ${PSQLDIR}/populate_proposed_transactions.sql #&> ${TXLOG}
 
 # #Run Tx Query Comparision and Data Gathering on new Txs
 # #cd ${APPPATH}
 # #source ~/.rvm/scripts/rvm
 # #${APPPATH}/bin/remcon_runner -g prod "load '../datascience/data_pipeline/ruby_pipeline_functions/process_transaction_events.rb'; process_transaction_events 'datasci_projects', 'proposed_transaction_events', 'event_id', true, 4, '(\"accepted offer\" OR \"ratified offer\" OR \"purchase agreement\" OR (filename:contract filename:pdf -lease) OR filename:\"listing agreement\" OR (from:skyslope subject:\"all signed\") OR (from:dotloop subject:\"has signed\") OR (from:zillow subject:\"Your listing is posted\") OR (from:ctmecontracts OR to:ctmecontracts))'" #>> ${TXLOG} 2>&1
 
 # #Populate New Tx Data in Offer Inputs Table
 # #POLOG=~/Development/Data/ETL_Logging/offer_processing_`date +\%Y\%m\%d_\%H\%M\%S`.log
 # ${PGPATH}/psql -d ${DATABASE} -h ${HOST} -p ${PORT} -U ${PGUSER} -w -f ${PSQLDIR}/qa_offer_clean.sql #&> ${POLOG}
 # ${PGPATH}/psql -d ${DATABASE} -h ${HOST} -p ${PORT} -U ${PGUSER} -w -f ${PSQLDIR}/populate_proposed_offers.sql #&> ${POLOG}
 
 # #Run Offer Scan
 # cd ${APPPATH}
 # source ~/.rvm/scripts/rvm
 # ${APPPATH}/bin/remcon_runner -g prod "load '../datascience/data-mining/process_parse_scan.rb'; process_parse_scan(schema: 'datasci_mining', tablename: 'test', eventidcol: 'event_id', processes: 4, message_thresh: 100, query: 'has:attachment', store_doc_dir: '', try_doc_parse: true, form_type: ['Purchase Offer', 'Counter Offer'])"
 
 
 
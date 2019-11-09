#!/bin/bash
date
#Set Environment Variables
source ~/workspace/datascience/ds_pipeline_env.sh
source ~/.rvm/scripts/rvm

if ! screen -ls | grep "TxProcessing"; then screen -DR -S TxProcessing; fi
screen -S TxProcessing -X stuff "${SHDIR}process_transaction_events.sh"$(echo '\015')


screen -S TxProcessing -X stuff "source ~/.rvm/scripts/rvm"$(echo '\015')
screen -S TxProcessing -X stuff 


{PGPATH}/psql -d ${DATABASE} -h ${HOST} -p ${PORT} -U ${PGUSER} -w -f ${PSQLDIR}/populate_proposed_transactions.sql

screen -S TxProcessing -X stuff "cd ${APPPATH}"$(echo '\015')
screen -S TxProcessing -X stuff ${APPPATH}/bin/remcon_runner -g prod load '../datascience/data_pipeline/ruby_pipeline_functions/process_transaction_events.rb'| process_transaction_events 'datasci_projects', 'proposed_transaction_events', 'event_id', true, 4, '(\"accepted offer\" OR \"ratified offer\" OR \"purchase agreement\" OR (filename:contract filename:pdf -lease) OR filename:\"listing agreement\" OR (from:skyslope subject:\"all signed\") OR (from:dotloop subject:\"has signed\") OR (from:zillow subject:\"Your listing is posted\") OR (from:ctmecontracts OR to:ctmecontracts))'$(echo '\015')
${APPPATH}

#Populate New Tx Data in Proposed Tx Table

#Run Tx Query Comparision and Data Gathering on new Txs
cd ${APPPATH}
"${APPPATH}/bin/remcon_runner -g prod"+"load '../datascience/data_pipeline/ruby_pipeline_functions/process_transaction_events.rb'; process_transaction_events 'datasci_projects', 'proposed_transaction_events', 'event_id', true, 4, '(\"accepted offer\" OR \"ratified offer\" OR \"purchase agreement\" OR (filename:contract filename:pdf -lease) OR filename:\"listing agreement\" OR (from:skyslope subject:\"all signed\") OR (from:dotloop subject:\"has signed\") OR (from:zillow subject:\"Your listing is posted\") OR (from:ctmecontracts OR to:ctmecontracts))'"



${APPPATH}/bin/remcon_runner -g prod "load '../datascience/data_pipeline/ruby_pipeline_functions/process_transaction_events.rb'; process_transaction_events 'datasci_projects', 'proposed_transaction_events', 'event_id', true, 4, '(\"accepted offer\" OR \"ratified offer\" OR \"purchase agreement\" OR (filename:contract filename:pdf -lease) OR filename:\"listing agreement\" OR (from:skyslope subject:\"all signed\") OR (from:dotloop subject:\"has signed\") OR (from:zillow subject:\"Your listing is posted\") OR (from:ctmecontracts OR to:ctmecontracts))'"$(echo '\015')


#!/bin/bash -l
# sudo RAILS_ENV="development" HOME="/Users/pivotal" DATABASE_URL="postgres://u2cs0k36gtfce2:p32cqm3hnno00a3do4909qdhl8o@ec2-54-204-13-35.compute-1.amazonaws.com:5432/dfpa89c6j8adi3" nohup /Users/pivotal/workspace/amitree/script/adhoc/data_sci_sync.sh

PURPLE='\033[0;35m'
echo
echo
echo
echo -e "${PURPLE}---------------------------------------------------------------------------"
echo `date`

if [[ -s "/Users/pivotal/.rvm/scripts/rvm" ]] ; then
  source "/Users/pivotal/.rvm/scripts/rvm"
else
  printf "ERROR: An RVM installation was not found.\n"
fi

cd /Users/pivotal/workspace/amitree

rvm use ruby-2.4.1@amitree

echo "Starting data sync - pid $$"
sleep 1

# download data
bundle exec rails runner 'load "script/adhoc/data_science_eric.rb"; populate_annotated_transactions'

export DEV_DATABASE=development
export DEV_USER=pivotal
export DEV_PASSWORD=
export DEV_HOST=localhost
export DEV_PORT=5432

cd /Users/pivotal/workspace/email_classification/pipeline

# run luigi pipeline

source activate dev
luigid --background

python -m luigi --module pipeline PipelineTask

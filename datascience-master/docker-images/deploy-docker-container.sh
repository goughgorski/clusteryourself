#!/bin/bash

#this script build and deploys docker containers to heroku
#arg 1 - dirrectory for the container build
#arg 2 - heroku app to release container to
#arg 3:@ - directories needed in the tmp folder in the container
# deploy-docker-container.sh ./parabox_RE pa-r-abox-staging ~/workspace/datascience/functions/*.* 


cd "${1}"
mkdir tmp

for i in "${@:3}"
do
   echo $i
   cp $i ./tmp
done


#Start docker
if [[ $(ps aux | grep Docker| wc -l) -eq 1 ]]
	set -x
	then open -a Docker && sleep 45
fi


#Log in to heroku container registry
heroku container:login

#Build and push Docker image to heroku container registry
heroku container:push web -a "${2}"

#Release image to target heroku application
heroku container:release web -a "${2}"

#Stop docker
osascript -e 'quit app "Docker"'

#Clean copied files
rm -r ./tmp


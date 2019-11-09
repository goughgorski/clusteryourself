#!/bin/bash
FILENAME=Backup_`date +\%Y\%m\%d_\%H\%M\%S`.tar.gz

#Archvie Pipeline Backup / Restores
tar -czf ~/Development/Data/Archives/${FILENAME} ~/Development/Data/Backups/

#If archive created and looks ok, clear directory
if [ -s ~/Development/Data/Archives/${FILENAME} ]
then
	find ~/Development/Data/Backups -delete
else
     echo 'WARNING: No archive found.'
fi


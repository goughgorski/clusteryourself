#!/bin/bash

SCRIPT=$(dirname "$0")

function cleanup()
{
	rm -f "$TEMPFILE"
	rm -f "$TEMPFILE2"
}

function usage()
{
	echo "Usage: $0 [--include-delayed-jobs] [--include-sent-emails] [--include-gmail-tokens]" >&2
	exit 1
}

function log()
{
	local msg="$1"

	echo -n $'\e[0;32m' >&2
	echo -n "$msg" >&2
	echo $'\e[0m' >&2
}

function warn()
{
	echo -n $'\e[0;33m' >&2
	echo -n "$1" >&2
	echo -n $'\e[0m' >&2
}

function pull()
{
	ARGS=()
	for table in "$@"
	do
		ARGS+=("--exclude-table-data" "$table")
	done
	if ! ${HEROKUPATH}/heroku auth:whoami 2>/dev/null
	then
		${HEROKUPATH}/heroku auth:login
	fi
	export DBNAME="${DATABASE}_`date +%s`"
	$SCRIPT/pg_run `${HEROKUPATH}/heroku config:get DATABASE_URL -r prod --app amitree` ${PGPATH}/pg_dump -v -x -Fc --serializable-deferrable "${ARGS[@]}" -f ${BACKUPDIR}/$DBNAME.dump
}

#skip some tables 
SKIP_TABLES='sent_emails
emails
proxied_emails
delayed_jobs
gmail_contacts
client_statuses
delayed_jobs
'

for arg in $*
do
	case $arg in
	--include-delayed-jobs)
		SKIP_TABLES=$(echo "$SKIP_TABLES" | grep -vx delayed_jobs)
		;;
	--include-sent-emails)
		SKIP_TABLES=$(echo "$SKIP_TABLES" | grep -vx sent_emails)
		;;
	--include-gmail-tokens)
		SKIP_TABLES=$(echo "$SKIP_TABLES" | grep -vx gmail_tokens)
		;;
	*)
		echo "Unknown option: $arg" >&2
		usage
	esac
done

TEMPFILE=$(mktemp $TMPDIR/database.XXXXXX)
TEMPFILE2=$(mktemp $TMPDIR/database.XXXXXX)
trap cleanup EXIT

set -e

PATH=$PATH:/Applications/Postgres.app/Contents/Versions/latest/bin:/Applications/Postgres.app/Contents/Versions/9.4/bin:/Applications/Postgres.app/Contents/Versions/9.3/bin:/Applications/Postgres93.app/Contents/MacOS/bin

log "* Dumping production to $DBNAME"
pull $SKIP_TABLES



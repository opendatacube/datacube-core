#!/usr/bin/env bash
set -e

# usage: file_env VAR [DEFAULT]
#    ie: file_env 'XYZ_DB_PASSWORD' 'example'
# (will allow for "$XYZ_DB_PASSWORD_FILE" to fill in the value of
#  "$XYZ_DB_PASSWORD" from a file, especially for Docker's secrets feature)
# Copied from: https://github.com/docker-library/postgres/blob/master/10/docker-entrypoint.sh
file_env() {
	local var="$1"
	local fileVar="${var}_FILE"
	local def="${2:-}"
	if [ "${!var:-}" ] && [ "${!fileVar:-}" ]; then
		echo >&2 "error: both $var and $fileVar are set (but are exclusive)"
		exit 1
	fi
	local val="$def"
	if [ "${!var:-}" ]; then
		val="${!var}"
	elif [ "${!fileVar:-}" ]; then
		val="$(< "${!fileVar}")"
	fi
	export "$var"="$val"
	unset "$fileVar"
}

# First set up the CONF_FILE environment variable
file_env 'DATACUBE_CONFIG_PATH'
if [ "$DATACUBE_CONFIG_PATH" ]; then
    export CONF_FILE="$DATACUBE_CONFIG_PATH"
else 
    export CONF_FILE="$HOME/.datacube.conf"
fi

# Build Config file
echo "[datacube]" > $CONF_FILE 

file_env 'DB_DATABASE'
if [ "$DB_DATABASE" ]; then
    echo "db_database: $DB_DATABASE" >> $CONF_FILE 
else
    echo >&2
    echo >&2 'Warning: missing DB_DATABASE environment variable'
    echo >&2 '*** Using "datacube" as fallback. ***'
    echo >&2
    echo "db_database: datacube" >> $CONF_FILE 
fi

file_env 'DB_HOSTNAME'
if [ "$DB_HOSTNAME" ]; then
    echo "db_hostname: $DB_HOSTNAME" >> $CONF_FILE 
fi

file_env 'DB_USERNAME'
if [ "$DB_USERNAME" ]; then
    echo "db_username: $DB_USERNAME" >> $CONF_FILE 
fi

file_env 'DB_PASSWORD'
if [ "$DB_PASSWORD" ]; then
    echo "db_password: $DB_PASSWORD" >> $CONF_FILE
fi

file_env 'DB_PORT'
if [ "$DB_PORT" ]; then
    echo "db_port: $DB_PORT" >> $CONF_FILE
fi

exec "$@"

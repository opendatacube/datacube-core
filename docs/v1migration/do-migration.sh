#!/usr/bin/env bash

set -eu

# Make sure this matches your database in datacube.conf
db='imported_agdc1'

dropdb ${db}
createdb ${db}

psql -d ${db} -c 'create extension if not exists "uuid-ossp";'
psql -d ${db} -c 'create extension if not exists dblink;'

agdc-config -v database init
agdc-config -v storage add ../config_samples/25m_bands_geotif_storage_type.yaml

psql -d ${db} -f index_v1_data.sql

psql -d ${db} -c 'vacuum analyze;'


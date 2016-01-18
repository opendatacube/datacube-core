#!/usr/bin/env bash

set -eu

# Make sure this matches your database in datacube.conf
db='imported_agdc1'
foreign_host='localhost'
foreign_db='hypercube_v0'
foreign_user='cube_user'

PYTHONPATH=/Users/jeremyhooke/agdc-v2
agdc_config="python /Users/jeremyhooke/agdc-v2/datacube/scripts/config_tool.py"

dropdb ${db}
createdb ${db}

psql -d ${db} -c 'create extension if not exists "uuid-ossp";'
psql -d ${db} -c 'create extension if not exists dblink;'

${agdc_config} -v database init
${agdc_config} -v storage add ../config_samples/25m_bands_geotif_storage_type.yaml

psql -d ${db} -v ON_ERROR_STOP=1\
              -v "foreign_host='${foreign_host}'" \
              -v "foreign_db='${foreign_db}'" \
              -v "foreign_user='${foreign_user}'" \
              -f index_v1_data.sql

psql -d ${db} -c 'vacuum analyze;'


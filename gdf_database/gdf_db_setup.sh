#!/bin/bash
# Script to setup an empty GDF database on a freshly installed local PostGIS database

dbname=gdf_empty
db_backup_file=gdf_empty.backup

# Create default GDF groups and users
psql -U postgres -c "
CREATE ROLE cube_admin_group
  NOSUPERUSER INHERIT NOCREATEDB NOCREATEROLE NOREPLICATION;

CREATE ROLE cube_user_group
  NOSUPERUSER INHERIT NOCREATEDB NOCREATEROLE NOREPLICATION;

CREATE ROLE cube_admin LOGIN
  ENCRYPTED PASSWORD 'md5bef0c3c7aadc8744bc3fa174c5e80f6b'
  SUPERUSER INHERIT CREATEDB CREATEROLE REPLICATION;
GRANT cube_admin_group TO cube_admin;
GRANT cube_user_group TO cube_admin;

CREATE ROLE cube_user LOGIN
  ENCRYPTED PASSWORD 'md57c93896ee15147e58d639d52196e092a'
  NOSUPERUSER INHERIT NOCREATEDB NOCREATEROLE NOREPLICATION;
GRANT cube_user_group TO cube_user;
"

./create_db_from_backup.sh $dbname $db_backup_file
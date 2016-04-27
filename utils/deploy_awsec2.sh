#!/bin/bash

######################################################################################################
# Purpose: One script installation of agdc-v2 into a baseline AWS ec2 instance with ubuntu-14.04 OS 64bit.
# Minimal hardware Spec: 2CPU, 8GB memory, 200GB diskspace.
# Author: Fei.Zhang@ga.gov.au
# DateCreated: 2016-04-08

# Main Steps:
#
# 1. Install git, gdal hdf netcdf binaries and ananconda Python and libraries 
# 2. clone and install agdc-v2 into conda python
# 3. install and setup postgres 9.4
# 4. create and initialize the datacube schema +config
# 5. ingest test datasts
# 6. Run test suite.
###################################################################################################### 

function install_postgres94:  # Install postgres-9.4 etc
{
    sudo  apt-get install git g++

    sudo sh -c 'echo "deb http://apt.postgresql.org/pub/repos/apt/ $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list'
    wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | sudo apt-key add -

    sudo apt-get update
    sudo apt-get upgrade
    sudo apt-get install postgresql-9.4 postgresql-client-9.4 postgresql-contrib-9.4
    sudo apt-get install postgresql-doc-9.4

    sudo apt-get install pgadmin3

}

function setup_db:
{

/etc/init.d/postgresql status
#make sure postgres servewr up and running
# 9.4/main (port 5432): online


# Create and initise a database
#ubuntu@ip-10-0-0-164:/xdisk$ sudo su - postgres -c "createdb testdc"
#ubuntu@ip-10-0-0-164:/xdisk$ datacube-config database init
#Initialising database...
#Done.

#postgres@ip-10-0-0-164:~$ createdb datacube

# create a admin db users

postgres@ip-10-0-0-164:~$ psql  -d datacube
psql (9.4.7)
Type "help" for help.

datacube=# create user test_user login superuser  password 'test_user';
CREATE ROLE

# next step is create schema

#as ubuntu user  config the database login for sqlalc
ubuntu@ip-10-0-0-164:~$ cat  ~/.datacube.conf
[datacube]
db_database: datacube

# A blank host will use a local socket. Specify a hostname to use TCP.
db_hostname: localhost

# Credentials are optional: you might have other Postgres authentication configured.
# The default username otherwise is the current user id.
db_username: test_user


[locations]
# Where to reach storage locations from the current machine.
#  -> Location names are arbitrary, but correspond to names used in the
#     storage type files.
#  -> We may eventually support remote protocols (http, S3, etc) to lazily fetch remote data.
#eotiles: file:///short/public/democube/
eotiles: file:///xdisk/democube/


# create schema
ubuntu@ip-10-0-0-164:~$  datacube-config -v database init
Initialising database...
Done.

# import data storage type definition

datacube-config storage add /xdisk/fzhang/agdc-v2/docs/config_samples/ga_landsat_5/ls5_albers.yaml

datacube-config storage add /xdisk/fzhang/agdc-v2/docs/config_samples/ga_landsat_7/ls7_albers.yaml

datacube-config storage add /xdisk/fzhang/agdc-v2/docs/config_samples/ga_landsat_8/ls8_albers.yaml

# ingest datasets
datacube-ingest -v ingest /xdisk/IngestInputs/ls5/LS5_TM_NBAR_P54_GANBAR01-002_095_083_20060129/ga-metadata.yaml

}

# other stuff

sudo apt-get install libhdf5-serial-dev libnetcdf-dev
sudo apt-get install libgdal1-dev
sudo apt-get install hdf5-tools netcdf-bin gdal-bin
sudo apt-get installlibhdf5-doc netcdf-doc libgdal1-doc

# firefox for http://localhost:8888 test jupyter notebook
sudo apt-get install firefox



# Install anaconda python as user ubuntu:
wget https://3230d63b5fc54e62148e-c95ac804525aac4b6dba79b00b39d1d3.ssl.cf1.rackcdn.com/Anaconda2-4.0.0-Linux-x86_64.sh

# install anaconda

bash Anaconda2-4.0.0-Linux-x86_64.sh

#add to .bashrc after anaconda path
export GDAL_DATA=/xdisk/anaconda2/share/gdal/

# re-login or export PATH=/anaconda/bin:$PATH; to make sure python -V showing the anaconda's installation version
conda install psycopg2 gdal libgdal hdf5 rasterio netcdf4 libnetcdf pandas


# Clone agdc-v2 git repo and install it as ubuntu:
git clone https://github.com/data-cube/agdc-v2.git
cd adgc-v2
git checkout develop # use the right branch
python setup.py install  # will install into anaconda version

# conda list

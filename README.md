# Australian Geoscience Data Cube v2

Master -
[![Build Status - Master](https://travis-ci.org/data-cube/agdc-v2.svg?branch=master)](https://travis-ci.org/data-cube/agdc-v2)
[![Coverage Status](https://coveralls.io/repos/data-cube/agdc-v2/badge.svg?branch=master&service=github)](https://coveralls.io/github/data-cube/agdc-v2?branch=master)
[![Documentation Status - Master](https://readthedocs.org/projects/agdc-v2/badge/?version=latest)](https://readthedocs.org/projects/agdc-v2/?badge=latest)

Develop -
[![Build Status - Develop](https://travis-ci.org/data-cube/agdc-v2.svg?branch=develop)](https://travis-ci.org/data-cube/agdc-v2)
[![Coverage Status](https://coveralls.io/repos/data-cube/agdc-v2/badge.svg?branch=develop&service=github)](https://coveralls.io/github/data-cube/agdc-v2?branch=develop)
[![Documentation Status - Develop](https://readthedocs.org/projects/agdc-v2/badge/?version=develop)](https://readthedocs.org/projects/agdc-v2/?badge=develop)

Overview
========

The Australian Geoscience Data Cube provides an integrated gridded data analysis environment for decades of analysis ready earth observation satellite and related data from multiple satellite and other acquisition systems.

In 2014, Geoscience Australia, CSIRO and the NCI established the Australian Geoscience Data Cube, building on earlier work of Geoscience Australia and expanding it to include additional earth observation satellite and other gridded data collections (e.g. MODIS, DEM) in order to expand the range of integrated data analysis capabilities that were available. The complete software stack and petabytes of EO are deployed at the NCI petascale computing facility for use by NCI users.

__The current AGDC v2 implementation is intended as a working prototype__ for a cohesive, sustainable framework for large-scale multidimensional data management for geoscientific data. This public development release is intended to foster broader collaboration on the design and implementation. It is not intended for operational use.

Requirements
============

### System
* PostgreSQL 9.4 or greater
* Python 2.7

See [requirements.txt](requirements.txt) for required python modules.

Installation
============

1. Clone repository from GitHub
    * `git clone https://github.com/data-cube/agdc-v2.git`
2. Install python requirements
    * `pip install -r requirements.txt`
3. Install AGDC-v2
    * `python setup.py install`

Setup
=====

### User configuration

Create a basic user configuration in ~/.datacube.conf; For example:

    [datacube]
    db_hostname: localhost
    db_database: datacube
    db_username:
    db_password:
    
    [locations]
    gdata: file:///gdata/datacube


### Create the datacube database in PostgreSQL

    createdb datacube
    agdc-config database init

### Load sample **Storage Type** configuration documents

    datacube-config storage add docs/config_samples/*_type.yaml
    
### Load sample **Storage Mapping** configuration documents

    datacube-config mappings add docs/config_samples/*/*_mapping.yaml


### Ingest some data

    $ datacube-ingest -h
    Usage: datacube-ingest-script.py [OPTIONS] [DATASET]...
    
      Ingest datasets into the Data Cube.
    
    Options:
      -v, --verbose  Use multiple times for more verbosity
      --log-queries  Print database queries.
      -h, --help     Show this message and exit.
      
    $ datacube-ingest dataset-description.yaml



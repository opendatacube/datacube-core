======================
Ubuntu Developer Setup
======================

Base OS: Ubuntu 16.04 LTS

This guide will setup an ODC core development environment and includes:

 - Anaconda python using conda environments to isolate the odc development environment
 - installation of required software and useful developer manuals for those libraries
 - Postgres database installation with a local user configuration
 - Integration tests to confirm both successful development setup and for ongoing testing
 - Build configuration for local ODC documentation


Required software
-----------------

GDAL, HDF5, and netCDF4::

    sudo apt-get install libgdal1-dev libhdf5-serial-dev libnetcdf-dev

Postgres::

    sudo apt-get install postgresql-9.5 postgresql-client-9.5 postgresql-contrib-9.5

Optional packages (useful utilities, docs)::

    sudo apt-get install postgresql-doc-9.5 libhdf5-doc netcdf-doc libgdal-doc
    sudo apt-get install hdf5-tools netcdf-bin gdal-bin pgadmin3


.. include:: common_install.rst

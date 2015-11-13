Installation
============

These installation instructions are tested on:

* Ubuntu 14.04 or variant
* Redhat/Centos

Required software
-----------------

PostgreSQL and PostGIS::

    apt-get install postgresql-9.3 postgresql-client-9.3 postgresql-contrib-9.3 postgresql-9.3-postgis-2.1 postgresql-9.3-postgis-2.1-scripts

HDF4, HDF5, and netCDF4::

   apt-get install libhdf4-alt-dev libhdf5-serial-dev libnetcdf-dev

.. note::

    Using libhdf4-alt-dev for compatibility with GDAL

GDAL::

    apt-get install libgdal-dev

Optional packages (useful utilities, docs)::

    apt-get install postgresql-doc-9.3 libhdf4-doc  libhdf5-doc netcdf-doc libgdal-doc
    apt-get install hdf4-tools hdf5-tools netcdf-bin gdal-bin pgadmin3



Python and packages
-------------------

.. attention::

    Python 2.7 is required.

Anaconda Python 2.7
^^^^^^^^^^^^^^^^^^^

Download `MiniConda <https://repo.continuum.io/miniconda/Miniconda-latest-Linux-x86_64.sh>`_ and install it::

    bash Miniconda-latest-Linux-x86_64.sh

.. note::

    You can use the full Anaconda installation if you prefer, it will have some packages by default.

Use conda to install the required Python packages::

    conda install nose pip sphinx numpy scipy matplotlib gdal netCDF4 numexpr psycopg2


Installation
============

These installation instructions are tested on:

* Ubuntu 14.04 or variant
* Redhat/Centos

Required software
-----------------

.. attention::

    These dependencies should be trimmed further. There's an excess of utilities listed used by some developers and not others.

PostgreSQL and PostGIS::

    apt-get install postgresql-9.3 postgresql-client-9.3 postgresql-contrib-9.3 postgresql-9.3-postgis-2.1 postgresql-9.3-postgis-2.1-scripts

HDF4, HDF5, and netCDF4::

   apt-get install zlib1g zlib1g-dev zlib1g-dbg zlibc zlib-bin libhdf4-alt-dev libhdf4-doc hdf4-tools libhdf5-doc libhdf5-serial-dev hdf5-tools netcdf-bin netcdf-dbg netcdf-doc libnetcdf-dev

.. note::

    Using libhdf4-alt-dev for compatibility with GDAL

GEOS, PROJ4 and GDAL::

    apt-get install libgeos-dev libgeos-dbg libgeos-doc proj-bin proj-data gdal-bin libgdal-dev libgdal-doc

Python and packages
-------------------

.. attention::

    Python 2.7 is required.

Anaconda Python 2.7
^^^^^^^^^^^^^^^^^^^

Download `MiniConda <https://repo.continuum.io/miniconda/Miniconda-latest-Linux-x86_64.sh>`_ and install it::

    bash Miniconda-latest-Linux-x86_64.sh

Use conda to install the Python packages::

    conda install nose pip sphinx numpy scipy h5py imaging matplotlib shapely  gdal netCDF4


Installation
============

These installation instructions are tested on:

* Ubuntu 14.04 or variant
* Redhat/Centos

Required software
-----------------

PostgreSQL::

    apt-get install postgresql-9.4 postgresql-client-9.4 postgresql-contrib-9.4

HDF5, and netCDF4::

   apt-get install libhdf5-serial-dev libnetcdf-dev

GDAL::

    apt-get install libgdal1-dev

Optional packages (useful utilities, docs)::

    apt-get install postgresql-doc-9.4 libhdf5-doc netcdf-doc libgdal1-doc
    apt-get install hdf5-tools netcdf-bin gdal-bin pgadmin3



Python and packages
-------------------

Python 2.7 and 3.4 are supported.

Download the latest version of the software from the `repository <https://github.com/data-cube/agdc-v2>`_ and install it::

    python setup.py install

.. note::

    Usage of virtual environment is recommended

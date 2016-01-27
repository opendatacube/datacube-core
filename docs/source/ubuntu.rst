======
Ubuntu
======

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

Python 2.7 and 3.4+ are supported.

Download the latest version of the software from the `repository <https://github.com/data-cube/agdc-v2>`_ and install it::

    git clone https://github.com/data-cube/agdc-v2
    cd agdc-v2
    git checkout develop
    python setup.py install


It may be useful to use conda to install binary packages::

    conda install psycopg2 gdal libgdal hdf5 rasterio netcdf4 libnetcdf pandas

.. note::

    Usage of virtual environments is recommended

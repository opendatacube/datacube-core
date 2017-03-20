========
Mac OS X
========

.. note::

    This section was typed up from memory. Verification and input would be appreciated.

Required software
-----------------
Homebrew::

    /usr/bin/ruby -e "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install)"

Postgres.app::

    http://postgresapp.com/

HDF5, netCDF4, and GDAL::

   brew install hdf5 netcdf gdal

Python and packages
-------------------
Python 2.7 and 3.5+ are supported.

Download the latest version of the software from the `repository <https://github.com/opendatacube/datacube-core>`_ and install it::

    git clone https://github.com/opendatacube/datacube-core
    cd datacube-core
    git checkout develop
    python setup.py install

It may be useful to use conda to install binary packages::

    conda install psycopg2 gdal libgdal hdf5 rasterio netcdf4 libnetcdf pandas

.. note::

    Usage of virtual environments is recommended

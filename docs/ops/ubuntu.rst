======
Ubuntu
======

Miniconda
~~~~~~~~~
1.  Download and install Miniconda using the following instructions https://conda.io/docs/user-guide/install/linux.html

2. Open your favourite terminal to execute the following commands.

.. include:: conda_base.rst 

Datacube is now installed and can be used in a terminal by activating the `cubeenv` environment.

Manual Installation (Fallback)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Only follow these steps if the Miniconda installation does not suit your needs.


Required software
-----------------

HDF5, and netCDF4::

   apt-get install libhdf5-serial-dev libnetcdf-dev

GDAL::

    apt-get install libgdal1-dev

Optional packages (useful utilities, docs)::

    apt-get install postgresql-doc-9.5 libhdf5-doc netcdf-doc libgdal1-doc
    apt-get install hdf5-tools netcdf-bin gdal-bin pgadmin3



Python and packages
-------------------

Python 3.5+ is required. Python 3.6 is recommended.

Download the latest version of the software from the `repository <https://github.com/opendatacube/datacube-core>`_ and install it::

    git clone https://github.com/opendatacube/datacube-core
    cd datacube-core
    git checkout develop
    python setup.py install


It may be useful to use conda to install binary packages::

    conda install psycopg2 gdal libgdal hdf5 rasterio netcdf4 libnetcdf pandas

.. note::

    Usage of virtual environments is recommended

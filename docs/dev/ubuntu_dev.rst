======================
Ubuntu Developer Setup
======================

Base OS: Ubuntu 16.04 LTS

Required software
-----------------

GDAL, HDF5, and netCDF4::

   sudo apt-get install libgdal1-dev libhdf5-serial-dev libnetcdf-dev

Postgres::

   sudo apt-get install postgresql-9.5 postgresql-client-9.5 postgresql-contrib-9.5

Optional packages (useful utilities, docs)::

    apt-get install postgresql-doc-9.5 libhdf5-doc netcdf-doc libgdal-doc
    apt-get install hdf5-tools netcdf-bin gdal-bin pgadmin3

Python and packages
-------------------

Python 2.7 and 3.5+ are supported.

Anaconda Python
~~~~~~~~~~~~~~~

`Install Anaconda Python <https://www.continuum.io/downloads#linux>`_

Add conda-forge to package channels::

    conda config --add channels conda-forge

Install required python packages and create an odc conda environment.

Python 3.5::

    conda env create -n odc --file .travis/environment_py35.yaml sphinx

Python 2.7::

    conda env create -n odc --file .travis/environment_py27.yaml sphinx

Activate odc python environment::

    source activate odc

Open Data Cube source
---------------------

Download the latest version of the software from the `repository <https://github.com/opendatacube/datacube-core>`_ and install it::

    git clone https://github.com/opendatacube/datacube-core
    cd datacube-core

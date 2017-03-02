=======================
Miniconda (recommended)
=======================

Install Postgres
----------------

Download and install PostgreSQL for your platform https://www.postgresql.org/download


Install Miniconda
-----------------

Follow conda installation guide for your platform: https://conda.io/docs/install/quick.html

Configure Miniconda
-------------------

Add conda-forge channel

.. code::

    conda config --add channels conda-forge

conda-forge channel provides multitude of community maintained packages.
Find out more about it here https://conda-forge.github.io

Create the environment
----------------------

.. code::

    conda create --name datacube python=3.5 cachetools dask gdal jsonschema netcdf4 numexpr numpy pathlib psycopg2 python-dateutil pyyaml rasterio singledispatch sqlalchemy xarray

Activate the environment on **Linux** and **OS X**

.. code::

    source activate datacube

Activate the environment on **Windows**

.. code::

    activate datacube

Find out more about managing virtual environments here https://conda.io/docs/using/envs.html


Install datacube
----------------

.. code::

    pip install datacube

Install other packages
----------------------

.. code::

    conda install jupyter matplotlib scipy

Find out more about managing packages here https://conda.io/docs/using/pkgs.html

========
 Ubuntu
========

Python venv Installation
========================

Ubuntu 20.04 includes fairly recent geospatial packages, so it is much more
practical to create "native" Python virtual environments for running Datacube.
One no longer needs to rely on conda.


Required Software
-----------------

Many Python modules are now shipped with pre-compiled binaries, but some still
require compilation during installation. The library for parsing YAML documents
(``libyaml-dev``), and the library for to talking to a PostgreSQL database (``libpq-dev``) are
such examples.

.. code:: bash

   apt-get install -y \
     build-essential \
     python3-dev \
     python3-pip \
     python3-venv \
     libyaml-dev \
     libpq-dev

The Datacube uses the ``rasterio``, ``shapely`` and ``pyproj`` geospatial libraries.
Those can be installed in binary form, however it is possible that binary
versions of those libraries are incompatible with each other as they might ship
slightly different versions of ``GDAL`` or other libraries. It is safest to
compile those libraries during installation instead. For that we need to install
geospatial and netcdf libraries and tools. Include fortran, chances are some
numeric lib will need it.

.. code:: bash

   apt-get install -y \
     libproj-dev \
     proj-bin \
     libgdal-dev \
     libgeos-dev \
     libgeos++-dev \
     libudunits2-dev \
     libnetcdf-dev \
     libhdf4-alt-dev \
     libhdf5-serial-dev \
     gfortran


Optional packages (useful utilities, docs)

.. code:: bash

    apt-get install postgresql-doc libhdf5-doc netcdf-doc libgdal-doc
    apt-get install hdf5-tools netcdf-bin gdal-bin pgadmin3


Creating Python Virtual Environment
-----------------------------------

This example uses a `virtual environment`_, as installation into system python is definitely not
recommended. First we create a new virtual environment called ``odc`` and update
some foundational packages.

.. code:: bash

   python3 -m venv odc
   ./odc/bin/python3 -m pip install -U pip setuptools
   ./odc/bin/python3 -m pip install -U wheel 'setuptools_scm[toml]' cython


Install ``datacube``, making sure that important dependencies are compiled
locally to ensure binary compatibility. Version 3 of ``pyproj`` requires a more
recent version of the ``PROJ`` C library than what is available in Ubuntu
repositories, so we limit ``pyproj`` to 2.x.x series.

.. code:: bash

   ./odc/bin/python3 -m pip install -U \
     'pyproj==2.*' \
     'datacube[all]' \
     --no-binary=rasterio,pyproj,shapely,fiona,psycopg2,netCDF4,h5py

If you omit the ``--no-binary=...`` flag you will get a pre-compiled version of
geospatial libs. Installation will be quicker, but the Python environment will be
somewhat larger due to duplicate copies of some C libraries. More importantly
you might get random segfaults if ``rasterio`` and ``pyproj`` include
incompatible binary dependencies.

Run some basic checks:

.. code:: bash

   ./odc/bin/datacube --help
   ./odc/bin/rio --help

Datacube no longer depends on GDAL Python bindings, but if your code needs them,
they can be easily installed like so

.. code:: bash

   ./odc/bin/python -m pip install GDAL==$(gdal-config --version)

It is important to install exactly the right version of python bindings, it must
match the version of the system GDAL, hence ``GDAL==$(gdal-config --version)``.

.. _`virtual environment`: https://docs.python.org/3.8/tutorial/venv.html

Miniconda
=========

Datacube is also available via the ``conda-forge`` channel for installation in a
Conda environment. So if you prefer or need to use Conda rather than system
Python, follow the instructions below:

1. Download and install Miniconda using the following instructions
   https://docs.conda.io/projects/conda/en/latest/user-guide/install/linux.html

2. Open your favourite terminal to execute the following commands.

.. include:: conda_base.rst

Datacube is now installed and can be used in a terminal by activating the `cubeenv` environment.

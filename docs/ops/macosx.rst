==========
 Mac OS X
==========

Miniconda
=========
1. Download and install Miniconda using the following instructions https://docs.conda.io/projects/conda/en/latest/user-guide/install/macos.html

2. Open Terminal to execute the following commands.

.. include:: conda_base.rst 

Datacube is now installed and can be used in Terminal by activating the `cubeenv` environment.

Manual Installation (Fallback)
==============================
Only follow these steps if the Miniconda installation does not suit your needs.

Required software
-----------------
Homebrew::

  /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"


HDF5, netCDF4, and GDAL::

  brew install hdf5 netcdf gdal postgres

Python and packages
-------------------
Python 3.6+ is required (3.8 is recommended)

Install the latest version of the ODC from PyPI:

  pip install -U \
    'pyproj==2.*' \
    'datacube[all]' \
    --no-binary=rasterio,pyproj,shapely,fiona,psycopg2,netCDF4,h5py

.. note::

    Usage of Docker and if not then Python virtual environments is recommended.

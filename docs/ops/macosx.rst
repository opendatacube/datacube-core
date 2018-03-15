========
Mac OS X
========
Miniconda
~~~~~~~~~
1. Download and install Miniconda using the following instructions https://conda.io/docs/user-guide/install/macos.html

2. Open Terminal to execute the following commands.

.. include:: conda_base.rst 

Datacube is now installed and can be used in Terminal by activating the `cubeenv` environment.

Manual Installation (Fallback)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Only follow these steps if the Miniconda installation does not suit your needs.

.. note::

    This section was typed up from memory. Verification and input would be appreciated.
    
    See also `Mac and Homebrew <https://github.com/opendatacube/documentation/blob/master/Build%20install%20instructions/Mac%20and%20Homebrew.md>`_ install instructions.

Required software
-----------------
Homebrew::

    /usr/bin/ruby -e "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install)"

HDF5, netCDF4, and GDAL::

   brew install hdf5 netcdf gdal

Python and packages
-------------------
Python 3.5+ is required (3.6 is recommended)

Download the latest version of the software from the `repository <https://github.com/opendatacube/datacube-core>`_ and install it::

    git clone https://github.com/opendatacube/datacube-core
    cd datacube-core
    git checkout develop
    python setup.py install

It may be useful to use conda to install binary packages::

    conda install psycopg2 gdal libgdal hdf5 rasterio netcdf4 libnetcdf pandas

.. note::

    Usage of virtual environments is recommended

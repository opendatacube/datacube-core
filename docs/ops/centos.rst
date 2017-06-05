========
CentOS 7
========

Required software
-----------------
                 

Postgresql 9.5 -> get newest version from `RedhatGuide <https://www.postgresql.org/download/linux/redhat/>`__::

    sudo yum install http://yum.postgresql.org/9.5/redhat/rhel-7-x86_64/pgdg-redhat95-9.5-2.noarch.rpm
    sudo yum install postgresql95 postgresql95-contrib postgresql95-devel postgresql95-server

GDAL & gdal_devel::

    sudo yum install gdal gdal-devel

HDF5, and netCDF4::

    sudo yum install netcdf netcdf-devel hdf5 hdf5-devel

Python (using Conda)
--------------------
      
Download and install `Miniconda2 <http://conda.pydata.org/docs/install/quick.html>`__ for managing python
versions and dependencies in a user environment::

    sudo yum install bzip2 wget
    wget https://repo.continuum.io/miniconda/Miniconda2-latest-Linux-x86_64.sh
    bash Miniconda2-latest-Linux-x86_64.sh

Install a new virtual python 3.5 environment including all necessary
packages (from the conda-forge channel) with the provided YAML ->  conda_env_datacube.yaml::

    conda config --prepend channels conda-forge --system
    conda update --all -y
    conda env create --file conda_env_datacube.yaml

Download the latest version of agdc-v2 from the repository and install it:;

    source activate datacube # activate the conda datacube environment

    git clone https://github.com/data-cube/agdc-v2
    cd agdc-v2
    git checkout develop
    python setup.py install

Database Setup
--------------

All the dependencies and code are now installed, setup the database database as follows::


    sudo -u postgres createuser --superuser $USER

    sudo -u postgres psql
    postgres=# \password $USER

    sudo -u postgres createdb $USER

    psql createdb datacube

    datacube -v system init #initialize db



For more information see :ref:`database_setup`




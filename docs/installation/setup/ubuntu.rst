
Ubuntu Developer Setup
**********************

Base OS: Ubuntu 20.04 LTS

This guide will setup an ODC core development environment and includes:

 - Anaconda python using conda environments to isolate the odc development environment
 - installation of required software and useful developer manuals for those libraries
 - Postgres database installation with a local user configuration
 - Integration tests to confirm both successful development setup and for ongoing testing
 - Build configuration for local ODC documentation


Required software
=================

GDAL, HDF5, and netCDF4::

    sudo apt-get install libgdal-dev libhdf5-serial-dev libnetcdf-dev

Install the latest Postgres version `available <https://packages.ubuntu.com/search?keywords=postgresql>` for your
Ubuntu distribution, eg::

    sudo apt-get install postgresql-12

    # Optionally, Postgis too
    sudo apt-get install postgresql-12-postgis-3

Ubuntu's official repositories usually ship older versions of Postgres. You can alternatively get the most recent version from
`the official PostgreSQL repository <https://wiki.postgresql.org/wiki/Apt>`_.

Optional packages (useful utilities, docs)::

    sudo apt-get install libhdf5-doc netcdf-doc libgdal-doc
    sudo apt-get install hdf5-tools netcdf-bin gdal-bin pgadmin3

.. include:: common_install.rst

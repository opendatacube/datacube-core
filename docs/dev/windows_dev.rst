======================
Windows Developer Setup
======================

**Under construction**

Base OS: Windows 10

This guide will setup an ODC core development environment and includes:

 - Anaconda python using conda environments to isolate the odc development environment
 - installation of required software and useful developer manuals for those libraries
 - Postgres database installation with a local user configuration
 - Integration tests to confirm both successful development setup and for ongoing testing
 - Build configuration for local ODC documentation

Required software
-----------------

GDAL, HDF5, and netCDF4::

   
Postgres::

    
Optional packages (useful utilities, docs)::

    

Python and packages
-------------------

Python 2.7 and 3.5+ are supported.

Anaconda Python
~~~~~~~~~~~~~~~

`Install Anaconda Python <https://www.continuum.io/downloads#linux>`_

Add conda-forge to package channels::

    conda config --add channels conda-forge

Conda Environments are recommended for use in isolating your ODC development environment from your system installation and other python evironments.

Install required python packages and create an odc conda environment.

Python 3.5::


Python 2.7::


Activate odc python environment::

    source activate odc

Postgres database configuration
-------------------------------

This configuration supports local development using your login name.


Open Data Cube source and development configuration
---------------------------------------------------

Download the latest version of the software from the `repository <https://github.com/opendatacube/datacube-core>`_ ::

    git clone https://github.com/opendatacube/datacube-core
    cd datacube-core

We need to specify the database user and password for the ODC integration testing. To do this::

    

Then edit the ~/.datacube_integration.conf with a text editor and add the following lines replacing <foo> with your username and <foobar> with the database user password you set above (not the postgres one, your <foo> one)::

    [datacube]
    db_hostname: localhost
    db_database: agdcintegration
    db_username: <foo>
    db_password: <foobar>

Verify it all works
-------------------

Run the integration tests::


Build the documentation::





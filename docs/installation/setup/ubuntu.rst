
Ubuntu Developer Setup
**********************

Base OS: Ubuntu 20.04 LTS

This guide will setup an ODC core development environment and includes:

 - Mambaforge using conda environments to isolate the odc development environment
 - installation of required software and useful developer manuals for those libraries
 - Postgres database installation with a local user configuration
 - Integration tests to confirm both successful development setup and for ongoing testing
 - Build configuration for local ODC documentation


Required software
=================

GDAL, HDF5, and netCDF4::

    sudo apt-get install libgdal-dev libhdf5-serial-dev libnetcdf-dev

Install the latest Postgres version `available <https://packages.ubuntu.com/search?keywords=postgresql>`_ for your
Ubuntu distribution, eg::

    sudo apt-get install postgresql-14

    # Optionally, Postgis too
    sudo apt-get install postgresql-14-postgis-3

Ubuntu's official repositories usually ship older versions of Postgres. You can alternatively get the most recent version from
`the official PostgreSQL repository <https://wiki.postgresql.org/wiki/Apt>`_.

Optional packages (useful utilities, docs)::

    sudo apt-get install libhdf5-doc netcdf-doc libgdal-doc
    sudo apt-get install hdf5-tools netcdf-bin gdal-bin pgadmin3

.. include:: common_install.rst


If createdb or psql cannot connect to server, check which postgresql installation is being run::

    which psql

If it is running the mambaforge installation, you may need to run the global installation::

    /usr/bin/psql -d agdcintegration


You can now specify the database user and password for ODC integration testing. To do this::

    cp integration_tests/agdcintegration.conf ~/.datacube_integration.conf

Then edit the ``~/.datacube_integration.conf`` with a text editor and add the following lines, replacing ``<foo>`` with your username and ``<foobar>`` with the database user password you set above (not the postgres one, your ``<foo>`` one)::

    [datacube]
    db_hostname: /var/run/postgresql
    db_database: agdcintegration
    index_driver: default
    db_username: <foo>
    db_password: <foobar>

    [experimental]
    db_hostname: /var/run/postgresql
    db_database: odcintegration
    index_driver: postgis
    db_username: <foo>
    db_password: <foobar>

Verify it all works
===================

Install additional test dependencies::
    
    cd datacube-core
    pip install --upgrade -e '.[test]'
    
Run the integration tests::

    ./check-code.sh integration_tests

Note: if moto-based AWS-mock tests fail, you may need to unset all AWS environment variables.

Build the documentation::

    pip install --upgrade -e '.[doc]'
    cd docs
    pip install -r requirements.txt
    sudo apt install make
    sudo apt install pandoc
    make html

Then open :file:`_build/html/index.html` in your browser to view the Documentation.

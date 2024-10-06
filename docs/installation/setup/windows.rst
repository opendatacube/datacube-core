
Windows Developer Setup
***********************

Base OS: Windows 10

This guide will setup an ODC core development environment and includes:

 - Anaconda python using conda environments to isolate the odc development environment
 - installation of required software and useful developer manuals for those libraries
 - Postgres database installation with a local user configuration
 - Integration tests to confirm both successful development setup and for ongoing testing
 - Build configuration for local ODC documentation

Required software
=================

Postgres:

    Download and install from `here <https://www.enterprisedb.com/downloads/postgres-postgresql-downloads>`_.

Optionally, download and install Postgis as well (required for the postgis/experimental index driver)

    Refer to the instructions detailed `here <https://postgis.net/documentation/getting_started/install_windows/released_versions/>`_.


Python and packages
===================

Python 3.9+ is required.

Conda environment setup
-----------------------

Conda environments are recommended for use in isolating your ODC development environment from your system installation and other Python environments.

We recommend you use Mambaforge to set up your conda virtual environment, as all the required packages are obtained from the conda-forge channel.
Download and install it from `here <https://github.com/conda-forge/miniforge#mambaforge>`_.

Download the latest version of the Open Data Cube from the `repository <https://github.com/opendatacube/datacube-core>`_::

    git clone https://github.com/opendatacube/datacube-core
    cd datacube-core

Create a conda environment named ``cubeenv``::

    mamba env create -f conda-environment.yml

If you didn't add miniforge to the Path on installation, you will need to use the full path of the executable::

    <install location>\miniforge3\condabin\mamba env create -f conda-environment.yml

Activate the ``cubeenv`` conda environment::

    conda activate cubeenv

Find out more about conda environments `here <https://docs.conda.io/projects/conda/en/latest/user-guide/concepts/environments.html>`_.


Postgres database configuration
===============================

This configuration supports local development using your login name.

If this is a new installation of Postgres on your system it is probably wise to set the postgres user password. As the local “postgres” user, we are allowed to connect and manipulate the server using the psql command.

In a terminal, type::

    psql -U postgres

Set a password for the "postgres" database role using the command::

    \password postgres

and set the password when prompted. The password text will be hidden from the console for security purposes.

Type **Control+D** or **\\q** to exit the posgreSQL prompt.

By default, Postgresql is configured to use ``ident sameuser`` authentication for any connections from the same machine, which is useful for development. Check out the excellent Postgresql documentation for more information, but essentially this means that if your system username is ``foo`` and you add ``foo`` as a Postgresql user then you can connect to a database without requiring a password for many functions.

Since the only user who can connect to a fresh install is the postgres user, here is how to create yourself a database account (which is in this case also a database superuser) with the same name as your login name and then create a password for the user::

    createuser -U postgres --superuser %USERNAME%
    psql - U postgres

    postgres=# \password <foo>

Now we can create databases for integration testing. You will need 2 databases - one for the Postgres driver and one for the PostGIS driver.
By default, these databases are called ``pgintegration`` and ``pgisintegration``, but you can name them however you want::

    postgres=# create database pgintegration;
    postgres=# create database pgisintegration;
    
Or, directly from the bash terminal::

    createdb pgintegration
    createdb pgisintegration

Connecting to your own database to try out some SQL should now be as easy as::

    psql -d pgintegration

You can now specify the database user and password for the ODC integration testing. To do this::

    copy integration_tests\integration.conf %HOMEDRIVE%%HOMEPATH%\.datacube_integration.conf

Then edit ``%HOMEDRIVE%%HOMEPATH%\.datacube_integration.conf`` with a text editor and add the following lines, replacing ``<foo>`` with your username and ``<foobar>`` with the database user password you set above (not the postgres one, your ``<foo>`` one)::

    [datacube]
    db_hostname: localhost
    db_database: pgintegration
    index_driver: default
    db_username: <foo>
    db_password: <foobar>

    [experimental]
    db_hostname: localhost
    db_database: pgisintegration
    index_driver: postgis
    db_username: <foo>
    db_password: <foobar>

Verify it all works
===================

Run the integration tests::

    cd datacube-core
    pytest


Build the documentation::

    cd datacube-core/docs
    pip install -r requirements.txt
    make html
    open _build/html/index.html

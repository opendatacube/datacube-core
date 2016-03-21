AGDC Database Setup
===================

.. attention::

    You must have a properly configured Postgres installation for this to work. If you have a fresh install of Postgres
    on Ubuntu then you may want to configure the ``postgres`` user password to `complete the postgres setup <https://help.ubuntu.com/community/PostgreSQL>`


Create Database
---------------

If you have existing Postgres authentication:
::

    createdb datacube

or specify connection details manually:

::

    createdb -h <hostname> -U <username> datacube

Create Configuration File
-------------------------
Datacube looks for configuration file in ~/.datacube.conf::

    [datacube]
    # Blank implies localhost
    db_hostname:
    db_database: datacube

    # Credentials are optional: you might have other PG authentication configured.
    # db_username:
    # db_password:

    [locations]
    # Where to reach storage locations from the current machine.
    #  -> Location names are arbitrary, but correspond to names used in the
    #     storage type files.
    #  -> We may eventually support remote protocols (http, S3, etc) to lazily fetch remote data.
    eotiles: file:///short/public/democube/

Change eotiles to point to the location where the datacube should store the storage units.
Note the URI syntax (file:// prefix is required).

See also :ref:`runtime-config-doc`

Create the Database Schema
--------------------------
:ref:`datacube-config-tool` can create and populate the datacube schema (agdc)::

    datacube-config -v database init

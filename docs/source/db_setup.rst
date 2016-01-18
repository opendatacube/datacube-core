AGDC Database Setup
===================

.. attention::

    You must have a properly configured postgres installation for this to work. If you have a fresh install of postgres on Ubuntu then you will need to configure the postgres user password to `complete the postgres setup <https://help.ubuntu.com/community/PostgreSQL>`_


Create Database
---------------
::

    createdb datacube

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
    #     storage mapping files.
    #  -> We may eventually support remote protocols (http, S3, etc) to lazily fetch remote data.
    eotiles: file:///short/public/democube/
    v1tiles: file:///g/data/rs0/tiles/EPSG4326_1deg_0.00025pixel/

See also :ref:`runtime-config-doc`

Create the Database Schema
--------------------------
:ref:`datacube-config-tool` can create and populate the datacube schema (agdc)::

    datacube-config -v database init

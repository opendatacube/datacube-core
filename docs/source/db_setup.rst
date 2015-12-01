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

    [locations]
    # Where to reach storage locations from the current machine.
    #  -> Location names are arbitrary, but correspond to names used in the
    #     storage mapping files.
    #  -> We may eventually support remote protocols (http, S3, etc) to lazily fetch remote data.
    swanky_tiles: file:///data/tiles/swanky

Create the Database Schema
--------------------------
:ref:`datacube-config-tool` can create and populate the datacube schema (agdc)::

    datacube-config -v database init

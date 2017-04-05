Database Setup
==============

.. attention::

    You must have a properly configured Postgres installation for this to work. If you have a fresh install of Postgres
    on Ubuntu then you may want to configure the ``postgres`` user password to `complete the postgres setup <https://help.ubuntu.com/community/PostgreSQL>`_


Create Database
---------------

If you have existing Postgres authentication:
::

    createdb datacube

or specify connection details manually:
::

    createdb -h <hostname> -U <username> datacube

.. note::

    You can also delete the database by running ``dropdb datacube``. This step is not reversible.

.. _create-configuration-file:

Create Configuration File
-------------------------
Datacube looks for a configuration file in ~/.datacube.conf or in the location specified by the ``DATACUBE_CONFIG_PATH`` environment variable. The file has this format::

    [datacube]
    db_database: datacube

    # A blank host will use a local socket. Specify a hostname (such as localhost) to use TCP.
    db_hostname:

    # Credentials are optional: you might have other Postgres authentication configured.
    # The default username otherwise is the current user id.
    # db_username:
    # db_password:

Uncomment and fill in lines as required.

See also :ref:`runtime-config-doc`

Initialise the Database Schema
------------------------------
:ref:`datacube-tool` can create and populate the Data Cube database schema
(agdc)::

    datacube -v system init

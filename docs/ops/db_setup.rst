Database Setup
**************

.. attention::

    You must have a properly configured Postgres installation for this to work. If you have a fresh install of Postgres
    on Ubuntu then you may want to configure the ``postgres`` user password to `complete the postgres setup <https://help.ubuntu.com/community/PostgreSQL>`_

Install PostgreSQL
==================

Data Cube is using `PostgreSQL <https://www.postgresql.org>`_


Ubuntu
------

Ubuntu 16.04 includes packages for PostgreSQL 9.5. On earlier versions of Ubuntu you can use the postgresql.org repo as
described on `their download page <http://www.postgresql.org/download/linux/ubuntu/>`_.


Install postgres using ``apt``::

    sudo apt install postgresql-9.5 postgresql-client-9.5 postgresql-contrib-9.5

Configure the ``postgres`` user password to `complete the postgres setup <https://help.ubuntu.com/community/PostgreSQL>`_


Windows
-------

An easy to install version of PostgreSQL can be downloaded from
http://sourceforge.net/projects/postgresqlportable/. It can install and run as
an unprivileged windows user.

After installing, launch ``PostgreSQLPortable.exe`` (and place a shortcut in the windows Startup menu).

To prepare the database for first use, enter the following commands in the PostgrSQL Portable window,
substituting "u12345" with your windows login user-ID::

    create role u12345 superuser login;
    create database datacube;


MacOS
-----

Install Postgres.app from http://postgresapp.com/


Create Database
===============

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
=========================

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
==============================

The ``datacube system init`` tool can create and populate the Data Cube database schema ::

    datacube -v system init

.. click:: datacube.scripts.system:database_init
   :prog: datacube system

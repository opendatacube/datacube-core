Database Setup
**************

These instructions assume a local PostgreSQL installation.  If you have access to an existing PostgreSQL installation,
the first part of these instructions may need to be adapted to your environment. Refer to your organisation's
friendly database administrator or your cloud provider's documentation for more information.

Install PostgreSQL
==================

Please refer to the `PostgreSQL <https://www.postgresql.org>`_ documentation on how to install and configure postgresql.
Note that the `PostGIS <https://postgis.net>`_ extension to PostgreSQL is required for the new ``postgis`` index driver.

On Ubuntu and other Debian-based Linux distributions, the following is usually sufficient::

   apt install postgresql-postgis

After a fresh install of Postgres on Ubuntu then you may want to configure the ``postgres`` user password
to `complete the postgres setup <https://help.ubuntu.com/community/PostgreSQL>`_

Create Database
===============

If you have existing Postgres authentication::

    createdb datacube

or specify connection details manually::

    createdb -h <hostname> -U <username> datacube

.. note::

    You can also delete the database by running ``dropdb datacube``. This step is not reversible.

.. _create-configuration-file:

Create Configuration File
=========================

Datacube looks for a configuration file in several locations, including ``./datacube.conf`` and ``~/.datacube.conf``,
or in a location specified by a user. For a detailed description of default locations and how to specify an alternate
location for the configuration file see :doc:`passing-configuration`.

The configuration file may be in ``.ini``, ``.yaml`` or ``.json`` format.  Here is an example configuration
file in ``.ini`` format::

    [production]
    # One config file may contain multiple named sections providing multiple configuration environments.

    # index_driver is optional and defaults to "default" (the default Postgres index driver)
    index_driver: default

    # The remaining configuration entries are for the default Postgres index driver and
    # may not apply to other index drivers.
    db_database: datacube

    # A blank host will use a local socket. Specify a hostname (such as localhost) to use TCP.
    db_hostname:

    # Port is optional. The default port is 5432.
    # db_port:

    # Credentials are optional: you might have other Postgres authentication configured.
    # The default username otherwise is the current user id.
    # db_username:
    # db_password:

    [default]
    # The section named "default" (or "datacube") is used if no environment is specified.
    #
    # An environment may be declared as an alias to another environment.
    # This section sets ``default`` as an alias to the ``production`` environment, ensuring that
    # the ``production`` environment will be used if no environment is specified.
    # No other configuration values may be used in an ``alias`` environment.
    alias: production

    [test]
    # A "test" environment that accesses a separate test database.
    index_driver: default
    db_database: datacube_test

    [migration]
    # An environment using the new postgis index driver
    index_driver: postgis
    # For both the postgis and postgres index drivers, database connection details may be configured in a single
    # db_url config entry, instead of the db_username, db_database, db_password, etc.
    db_url: postgresql://username:password@server.domain:5444/mydb

    # FOR DEVELOPERS OF OPENDATACUBE:
    # Using the experimental psycopg3 support can be done through the db_url,
    # or setting psycopg_version to 3.
    # db_url: postgresql+psycopg://username:password@server.domain:5444/mydb
    # psycopg_version: 3

    [null]
    # A "null" environment for working with no index.
    index_driver: null

    [local_memory]
    # A local non-persistent in-memory index.
    #   Compatible with the default index driver, but resides purely in memory with no persistent database.
    #   Note that each new invocation will receive a new, empty index.
    index_driver: memory


Alternately, you can configure the ODC connection to Postgres using environment variables::

    ODC_DEFAULT_DB_HOSTNAME
    ODC_DEFAULT_DB_PORT
    ODC_DEFAULT_DB_USERNAME
    ODC_DEFAULT_DB_PASSWORD
    ODC_DEFAULT_DB_DATABASE

To configure a database as a single connection url instead of individual environment variables::

    export ODC_DEFAULT_DB_URL=postgresql://[username]:[password]@[hostname]:[port]/[database]

Alternatively, for password-less access to a database on localhost::

    export ODC_DEFAULT_DB_URL=postgresql:///[database]

For further information on database configuration, see :doc:`configuration` and :doc:`passing-configuration`.

The desired environment can be specified:

1. in code, with the ``env`` argument to the ``datacube.Datacube`` constructor;
2. with the ``-E`` option to the command line ui;
3. with the ``$ODC_ENVIRONMENT`` environment variable.

Initialise the Database Schema
==============================

The ``datacube system init`` tool can create and populate the Data Cube database schema ::

    datacube -v system init

Or to initialise a database schema for an environment other than the default::

   datacube -v -E myenv system init

.. click:: datacube.scripts.system:database_init
   :prog: datacube system

.. _create-spatial-indexes-postgis-driver-only:

Create Spatial Indexes (Postgis Driver Only)
============================================

The new ``postgis`` index driver supports spatial indexes.  By default a spatial index is created for the CRS
``epsg:4326`` (i.e. WGS-84 lat/long coordinates).  Spatial indexes for other Coordinate Reference Systems can be
created and it is usually most efficient to create these spatial indexes up front.

Examples:

 - a database that will host mostly Australian EO data may benefit from an ``epsg:3577`` (Australian
   Albers) spatial index.
 - a database that intends to support web maps via `datacube-ows <https://github.com/opendatacube/datacube-ows>`_
   may benefit from an ``epsg:3857`` (Web Mercator) spatial index.
 - a database that will include data that crossed the anti-meridian will benefit from an ``epsg:3832``
   (Pacific Mercator) spatial index.
 - a database that will host Antarctic imagery will benefit from a ``epsg:3031`` (South Polar Stereographic)
   spatial index.

In deciding what spatial indexes to create for your database you should consider:

 - the native CRSes of the data you intend to index.
 - if you expect to have data in areas where ``epsg:4326`` is not suitable for searching (i.e. crossing the
   anti-meridian, or in the north or south polar regions.)
 - the native or preferred CRSes of any external systems you intend to interface or inter-operate with.
 - specialist CRSes in common use amongst your user base, or in the region you intend to focus on.

Adding and updating spatial indexes can be performed with the ``datacube spindex`` tool.  Spatial indexes
are identified by their postgres SRID.  For EPSG-registered CRSes, the SRID is EPSG number. Some ESRI-defined
non-EPSG SRIDs are also supported - refer to the PostGIS documentation for details.   Spatial indexes for
generalised non-EPSG CRSes (e.g. arbitrary WKT definitions) are not supported.

To add a spatial index for an srid user ``datacube spindex add srid``, e.g. for epsg:3577::

   datacube spindex add 3577

Note that adding a new spatial index to an existing database does NOT add existing datasets to the spatial index!

After adding a new spatial index to a non-empty database it is necessary to **update** the spatial index after
creation::

   datacube spindex update 3577


User and Permissions Management
===============================

When a database is initialised with ``datacube system init``, database roles
are created. These roles can serve as user groups to organise database permissions.

The user groups created are: "user" (providing read-only permissions for regular users
who only need to access existing data), "manage" (providing read-write users for data
management users who can add new data to the index), and "admin" (providing
schema ownership for admin users can update apply schema updates and drop and recreate
entire indexes).

In the newer ``postgis`` index driver, these roles have an ``odc_`` prefix (e.g.
``odc_user``, ``odc_manage``, etc.)

In the legacy ``postgres`` index driver, these roles have an ``agdc_`` prefix (e.g.
``agdc_user``, ``agdc_manage``, etc.)  (This refers to the old name for the project
before the name "Open Data Cube" was adopted. The legacy index driver also creates a
deprecated legacy ``agdc_ingest`` role which provides similar permissions to manage.)

ODC users are simply postgres users that have been granted one of the roles discussed
above.  Users can be created and role memberships maintained using conventional
Postgresql tools, but Datacube does provide some basic user management tools::

   datacube user create {user|manage|admin} USER

creates a new login user with the selected permissions - the password for the new user
is written to stdout.

Permissions for an existing user (or users) can be adjusted using::

   datacube user grant {user|manage|admin} USERS...

Note that this replaces any previously granted permissions.  E.g. granting "user" to
a user who already has the "admin" role will remove the "admin" role, leaving the user
with only the read-only "user" role.

Only a user with higher permissions can grant a particular role to a user.  This means
that granting the "admin" role can only be done by a user with superuser privileges on
the database.

As of the most recent releases, datacube-ows and datacube-explorer use the same roles
as datacube-core.  Previously they have maintained their own separate user permissions
frameworks.


.. py:currentmodule:: datacube

.. _runtime-config-doc:

ODC Configuration (Basics)
**************************

The Open Data Cube uses configuration files and/or environment variables to determine how to connect to databases.

Further functionality may be controlled by configuration in future releases.  (e.g. AWS/S3 access configuration,
rasterio configuration, etc.)

ODC Configuration Overview
==========================

When you first start a session with the Open Data Cube, you instantiate a
:py:class:`Datacube` object:

.. code-block:: python

   from datacube import Datacube

   dc = Datacube()

If you have access to many Open Data Cube databases, you may need to use several at once, e.g. to compare
the contents of dev and prod databases, or to combine data managed by different organisations.  In this
scenario, you instantiate a separate :py:class:`Datacube` object per environment:

.. code-block:: python

   from datacube import Datacube

   dc_prod = Datacube(env="prod")
   dc_dev  = Datacube(env="dev")

Environments can be read from a configuration file (e.g. an INI or YAML format file at :file:`~/.datacube.conf` -
refer to :doc:`passing-configuration` for a full description of how the location of the configuration file is
determined.)

A simple example configuration file:

.. code-block:: yaml

   # This is a YAML file and the # symbol marks comments
   default:
      # The 'default' environment is used if no environment is specified.
      # It is often convenient to define it as an alias to another environment
      alias: prod

   # You might have to copy configuration for system-wide environments from your system
   # configuration file.  (Probably at /etc/defaults/datacube.conf or /etc/datacube.conf)
   prod:
      # Production DB uses the legacy ODC index schema.
      index_driver: postgres
      # db_url is the easiest way to specify connection details
      db_url: postgresql://user:passwd@server.domain:5555/production_db
      db_connection_timeout: 30

   production:
      alias: prod

   dev:
      # Dev use the new PostGIS-based ODC index schema.
      index_driver: postgis
      db_url: postgresql://user:passwd@internal.server.domain:5555/development_db
      db_connection_timeout: 120

   development:
      alias: dev

   private:
      index_driver: postgis
      # Use OS ident authentication over a local named pipe.
      db_url: postgresql:///private

You can also override configuration file contents or even inject whole new environments dynamically with environment
variables, e.g.:

.. code-block:: python

   import os
   from datacube import Datacube

   # Environment dev exists in the configuration file, but use this db_url instead of the one in the config file
   os.environ["ODC_DEV_DB_URL"] = "postgresql:///user:B377erP@$$w0rd@internal.server.domain:5432/development_db"

   # Environment private does NOT exist in the configuration file, but we can create it dynamically
   os.environ["ODC_PRIVATE_INDEX_DRIVER"] = "postgis"
   os.environ["ODC_PRIVATE_DB_URL"] = "postgresql:///private"

   dc_private = Datacube(env="private")
   dc_private = Datacube(env="dev")


Configuration Files
===================

Format
------

Configuration files may be provided in either INI, JSON or YAML format.  YAML is preferred
for consistency with ODC metadata files.  INI files can only support one level of nesting,
which is sufficient for current functionality - INI format may be deprecated for
configuration files in future releases if deeper nesting of configuration becomes
desirable for future functionality.

INI format configuration files are parsed with the Python standard library
configparser module.  Features supplied by that library are supported in ODC for
INI format configuration files only.  (e.g. a ``DEFAULT`` section whose
options are applied to all other sections unless over-ridden, and interpolation.)
Refer to the :py:mod:`configparser` documentation in the Python standard library
for more information.

Configuration Environments
--------------------------

A valid configuration file consists of one or more named environment definition sections.

Environment names must start with a lowercase letter and can only include lowercase
letters and digits.  (This restriction it to support generic environment variable
overrides, as discussed below.)

.. code-block:: ini
   :caption: Full INI Configuration Example

    ; Comments in INI files start with a semi-colon
    ; This config file defines two environments: 'main' and 'aux'.
    [main]
    index_driver: default
    db_database: datacube
    db_hostname: server.domain.com
    db_username: cube
    db_password: this_is_a_big_secret

    [aux]
    index_driver: default
    db_database: mydb
    ; Leaving the hostname blank uses a local socket.
    db_hostname:


.. code-block:: yaml
   :caption: Full YAML Configuration Example

    # Comments in YAML files start with a hash.
    # This config file defines two environments: 'main' and 'aux'.
    main:
      index_driver: default
      db_database: datacube
      db_hostname: server.domain.com
      db_username: cube
      db_password: this_is_a_big_secret

    aux:
      index_driver: default
      db_database: mydb
      # Leaving the hostname blank uses a local socket.
      db_hostname:

Configuration Options
---------------------

All supported configuration options are described here.  Configuration options are
specified per-environment.

.. confval:: alias

   **Cannot be used in conjunction with any other configuration option.**

   Normally an environment section in a configuration file defines a new
   environment.  If the ``alias`` configuration option is used, the section
   instead defines an alias for an existing environment.  If the alias option
   is present in a section, no other configuration options are permitted in
   that section.


   .. code-block::

      [default]
      ; The default environment is an alias for the "main" section.
      ; The 'main' environment can be accessed as either 'main' or 'default'.
      alias: main

      [main]
       index_driver: default
       db_database: datacube
       db_hostname: server.domain.com
       db_username: cube
       db_password: this_is_a_big_secret

.. confval:: index_driver

   Defines which index driver should be used to access the database index for
   this environment.

   The Open Data Cube currently supports 4 index drivers:

   - ``postgres`` Postgres index driver (aka ``default``, ``legacy``).  This
     is the old-style index driver, fully compatible with datacube-1.8.  This
     is the default value used if index_driver is not specified in the
     configuration.

     This index driver will not be available in datacube-2.0.

   - ``postgis`` PostGIS index driver.  This is the new-style eo3-only index
     driver with support for spatial indexes.

   - ``memory`` In-memory index driver.  This index driver is currently
     compatible with the postgres driver, and stores all data temporarily in
     memory.  No persistent database is used.

   - ``null``  Null index driver.  If you are not using a database index at
     all, this might be an appropriate choice.

The ``null`` and ``memory`` index drivers take no further configuration. The
remaining configuration options only apply to the ``postgres`` and
``postgis`` index drivers:

.. confval:: db_connection_timeout

   **Only used for the 'postgres' and 'postgis' index drivers.**

   The database connection timeout, in seconds.

   Connections in the connection pool that are idle for more than the
   configured timeout are automatically closed.

   Defaults to 60.

.. confval:: db_url

   **Only used for the 'postgres' and 'postgis' index drivers.**

   Database connection details can be specified in a single option with the
   ``db_url`` field.  If a ``db_url`` is not provided, connection details can
   be specified with separate :confval:`db_hostname`, :confval:`db_port`, :confval:`db_database`,
   :confval:`db_username`, and :confval:`db_password` fields, as described below.

   If a `db_url` is provided, it takes precedence over the separate connection
   detail options.

   .. code-block:: ini
      :caption: INI Example showing :confval:`db_url`

      [default]
      index_driver: postgres
      ; Connect to database mydb on TCP port 5444 at server.domain, with username and password
      db_url: postgresql://username:password@server.domain:5444/mydb


   .. code-block:: yaml
      :caption: YAML Example showing :confval:`db_url`

      default:
        # Connect to database mydb over local socket with OS authentication.
        db_url: postgresql:///mydb

.. confval:: db_database

   **Only used for the 'postgres' and 'postgis' index drivers.**

   **Only used if :confval:`db_url` is not set.**

   The name of the database to connect to.  Defaults to ``"datacube"``.

.. confval:: db_hostname

   **Only used for the 'postgres' and 'postgis' index drivers.**

   **Only used if :confval:`db_url` is not set.**

   The hostname to connect to.  May be set to an empty string, in which case a
   local socket is used. Defaults to ``"localhost"`` if not set at all.

.. confval:: db_port

   **Only used for the 'postgres' and 'postgis' index drivers.**

   **Only used if :confval:`db_url` is not set.**

   The TCP port to connect to.  Defaults to 5432.  Not used when connecting over a local socket.

.. confval:: db_username

   **Only used for the 'postgres' and 'postgis' index drivers.**

   **Only used if :confval:`db_url` is not set.**

   The username to use when connecting to the database. Defaults to the
   username of the logged-in user on UNIX-like systems.

.. confval:: db_password

   .. admonition::
      Only used for the 'postgres' and 'postgis' index drivers.

      Only used if :confval:`db_url` is not set.

   The password to use when connecting to the database. Not used when
   connecting over a local socket.

.. confval:: db_iam_authentication

   **Only used for the 'postgres' and 'postgis' index drivers.**

   A boolean flag to indicate that IAM style authentication should be used
   instead of the supplied password.  (Recommended for cloud based database
   services like AWS RDS.)

   Defaults to False.

   .. code-block::
      :caption: Example showing :confval:`db_iam_authentication`

      [main]
      index_driver: postgis
      db_url: postgresql://user@server.domain:5432/main
      ; Use IAM authentication
      db_iam_authentication: yes

      [aux]
      index_driver: postgis
      db_url: postgresql:///aux
      db_iam_authentication: no

   YAML is a typed format and INI is not. Not all YAML boolean keywords will be
   recognised when they occur in INI files.  Using "yes" and "no" will work
   correctly for both formats.

   For IAM authentication to work, you must use the standard boto ``$AWS_*``
   environment variables to pass in your AWS identity and access key.

.. confval:: db_iam_timeout

   **Only used for the 'postgres' and 'postgis' index drivers.**

   **Only used when IAM authentication is activated.**

   How often (in seconds) a new IAM token should be generated.

   Defaults to 600 (10 minutes).

Need to know more?
==================
   This default config is only used after exhausting the default search path. If you have
   provided your own search path via any of the above methods and none of the paths exist, then an error is raised.


A full description of the ODC configuration engine can be found in :doc:`passing-configuration`.

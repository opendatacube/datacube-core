
.. py:currentmodule:: datacube

ODC Configuration
*****************

The Open Data Cube uses configuration files and/or environment variables to determine how to connect to databases.

Further functionality may be controlled by configuration in future releases.  (e.g. AWS/S3 access configuration,
rasterio configuration, etc.)

Overview
========

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

Environments can be read from a configuration file (e.g. an INI or YAML format file at :file:`~/.datacube.conf`) that
looks something like this:

.. code-block:: yaml

   # This is a YAML file and the # symbol marks comments
   default:
      # The 'default' environment is used if now environment is specified.
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

You can also inject new environments dynamically with environment variables, e.g.:

.. code-block:: python

   import os
   from datacube import Datacube
   os.environ["ODC_PRIVATE_INDEX_DRIVER"] = "postgis"
   os.environ["ODC_PRIVATE_DB_URL"] = "postgresql:///private"

   dc_private = Datacube(env="private")

Full details, including all recognised configuration options and defaults, is documented below.

Configuration Files
===================

Format
------

Configuration files may be provided in either INI or YAML format.  YAML is preferred
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

Evironment names must start with a lowercase letter and can only include lowercase
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

   - ``postgis`` Postgis index driver.  This is the new-style eo3-only index
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

   Connections in the connection pool that are idle for more than than the
   configured timeout are automatically closed.

   Defaults to 60.

.. confval:: db_url

   **Only used for the 'postgres' and 'postgis' index drivers.**

   Database connection details can be specified in a single option with the
   ``db_url`` field.  If a ``db_url`` is not provided, connection details can
   be specfied with separate :confval:`db_hostname`, :confval:`db_port`, :confval:`db_database`,
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
      :caption: Example showing :confval:`db_iam_authenticaion`

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

Passing in Configuration
========================

There are a number of different approaches for passing configuration into an Open Data Cube
session.  They are described here in priority order.

1. Explicit configuration
-------------------------

Configuration can be passed in explicitly, without ever reading from a configuration file on disk.

When explicit configuration is passed in, it takes precedence over configuration by environment variable.

1a. Via Python (str or dict)
++++++++++++++++++++++++++++

A valid configuration dictionary can be passed in directly to the
:py:class:`Datacube` constructor with the ``raw_config`` argument, without
serialising to a string:

.. code-block:: python

   dc = Datacube(raw_config={
      "default": {
         "index_driver": "postgres",
         "db_url": "postgresql:///mydb"
      }
   })

The ``raw_config`` argument can also be passed config as a string, in either INI or YAML format:

.. code-block:: python

   dc = Datacube(raw_config="""
   default:
     # Connect to database mydb over local socket with OS authentication.
     db_url: postgresql:///mydb
   """)

1b. As a string, via the datacube CLI
+++++++++++++++++++++++++++++++++++++

The contents of a configuration file can be passed into the ``datacube`` CLI via the ``-R`` or
``--raw-config`` command line option:

::

   datacube --raw-config "default: {db_database: this_db}"

Output from a script that generates a configuration file dynamically can be passed in using
a BASH backquote string:

::

   datacube --raw-config "`config_file_generator --option blah`"

1c. As a string, via an Environment Variable
++++++++++++++++++++++++++++++++++++++++++++

If raw configuration has not been passed in explicitly via methods 1a. or 1b.
above, then the contents of a configuration file can be written in full to the
:envvar:`ODC_CONFIG` environment variable:

.. code-block:: console

   $ ODC_CONFIG="default: {db_database: this_db}"
   $ datacube check    # will use the this_db database


2. Selecting a Configuration File
---------------------------------

.. highlight:: python

If explicit configuration has not been passed in, ODC attempts to find a configuration file.

Only one configuration file is read.

This is different to previous versions of the Open Data Cube,
where multiple configuration files could be merged.

If your previous practice was to extend a shared system configuration file with a local
user configuration file, then you will now need to take a copy of the system configuration file,
add your extensions to your copy, and ensure that the Open Data Cube reads from your
modified file.

2a. In Python
+++++++++++++

In Python, the ``config`` argument can take a path to a config file:

::

    dc = Datacube(config="/path/to/my/file.conf")

The ``config`` argument can also take a priority list of config paths.
The first path in the list that can be read (i.e. exists and has read permissions) is read.
If no configuration file can be found, a :py:class:`ConfigException` is raised:

::

     dc = Datacube(config=[
         "/first/path/checked",
         "/second/path/checked",
         "/last/path/checked",
     ])

The config argument can also take a :py:class:`cfg.ODCConfig` object.  Refer to
the API documentation for more information.

2b. Via the datacube CLI
++++++++++++++++++++++++

Configuration file paths can be passed using either the :option:`datacube -C`
or :option:`datacube --config`` option.

The option can be specified multiple times, with paths being searched in order, and an error being
raised if none can be read.

2c. Via an Environment Variable
+++++++++++++++++++++++++++++++

.. envvar:: ODC_CONFIG_PATH

   If config paths have not been passed in through methods 2a. or 2b. above,
   then they can be read from the :envvar:`ODC_CONFIG_PATH`` environment
   variable, in a UNIX Path-style colon separated list:

   ::

          ODC_CONFIG_PATH=/first/path/checked:/second/path/checked:/last/path/checked

2d. Default Search Paths
++++++++++++++++++++++++

If config file paths have not passed in through any of the above 2a. through
2c., then the Open Data Cube checks the following paths in order, with the
first readable file found being read:

1. :file:`./datacube.conf`    (in the current working directory)
2. :file:`~/.datacube.conf`   (in the user's home directory)
3. :file:`/etc/default/datacube.conf`
4. :file:`/etc/datacube.conf``

If none of the above exist then a basic default configuration is used, equivalent to:

.. code-block:: yaml

   default:
      db_hostname: ''
      db_database: datacube
      index_driver: default
      db_connection_timeout: 60

.. note:: Note
  This default config is only used after exhausting the default search path. If you have
  provided your own search path via any of the above methods and none of the paths exist, then an error is raised.

3. The Active Environment
-------------------------

3a. Specifying in Python
++++++++++++++++++++++++

The active environment can be selected in Python with the ``env`` argument to
the :py:class:`Datacube` constructor.

If you wish to work with multiple environments simultaneously, you can create
one :py:class`Datacube` object for each environment of interest and use them
side by side:

::

   dc_main    = Datacube(env="main")
   dc_aux     = Datacube(env="aux")
   dc_private = Datacube(env="private")

3b. Specifying in the CLI
+++++++++++++++++++++++++

The active environment can be selected in Python with the ``-E`` or ``--env`` option to the ``datacube``
CLI tool.

CLI commands that require more than one environment will have a second option for the second argument.
Refer to the ``--help`` text for more information.

3c. Via an Environment Variable
+++++++++++++++++++++++++++++++

.. envvar:: ODC_ENVIRONMENT

   If not explicitly specified via methods 3a. and 3b. above, the active
   environment can be specified with the ``$ODC_ENVIRONMENT`` environment
   variable.

3d. Default Environment
+++++++++++++++++++++++

If not specified by any of the methods 3a. to 3d. above, the ``default``
environment is used.  If no ``default`` environment is known, an error is
raised.  It is strongly recommended that a ``default`` environment be defined
in all configuration files - either explicitly, or as an alias to an explicitly
defined environment.

If no environment named ``default`` is known, but one named ``datacube`` **IS**
known, the ``datacube`` environment is used and a deprecation warning issued.
``datacube`` will be dropped as a legacy default environment name in a future
release.

4. Generic Environment Variable Overrides
-----------------------------------------

Configuration values in config files can be over-ridden by setting the appropriate environment variable.

The name of overriding environment variables are all upper-case and structured:

.. code-block:: bash

   $ODC_{environment name}_{option name}

E.g. to override the :confval:`db_password` field in the ``main`` environment,
set the ``$ODC_MAIN_DB_PASSWORD`` environment variable.

Environment variables overrides are **NOT** applied to environments defined in
configuration that was passed in explicitly as a string or dictionary.

4a. Dynamic Environments
++++++++++++++++++++++++

It is possible for environments to be defined dynamically purely in environment variables.

E.g. given the following active configuration file:

.. code-block::yaml

     default:
         alias: main
     main:
         index_driver: postgres
         db_url: postgresql://myuser:mypassword@server.domain/main

and the following defined environment variables:

.. code-block::bash

   ODC_AUX_INDEX_DRIVER=postgis
   ODC_AUX_DB_URL=postgres://auxuser:secret@backup.domain/aux

You can request the "aux" environment and it's configuration will be
dynamically read from the environment variables, even though it is not
mentioned in the configuration file at all.

Notes:

1. Environment variables are read when configuration is first read from that
   environment (i.e. when first connecting to the database.)

2. As all configuration options have default values, it is no longer possible
   to get an error by requesting an environment that does not exist.  Instead,
   an all-defaults environment with the requested name will be dynamically
   created.  The only exception is when a specific environment is not
   requested.  In this case, the ``default`` environment is only used if it is
   either defined in the active configuration file or has previously been
   explicitly requested from the same :py:class:`ODCConfig` object.

3. Although environment variable overrides are bypassed for configured
   environments by passing in explicit configuration, reading from environment
   variables to dynamically create new environments is still supported.

4b. Environment Variable Overrides and Environment Aliases
++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

To avoid troublesome and unpredictable corner carse, aliases can only be
defined in raw configuration or in config files - they cannot be defined
through environment variables.

i.e. defining ``ODC_ENV2_ALIAS=env1`` does NOT create an ``env2`` alias to the ``env1``
environment.

A configuration file may define an environment which is an alias to an environment that is to be loaded
dynamically and is NOT defined in the configuration file.

Aliases (created in raw config or a config file) **ARE** honoured when interpreting environment variables.

E.g.  Given config file:

.. code-block::yaml

     default:
          alias: main
     common:
          alias: main
     main:
          index_driver: postgis
          db_url: postgresql://uid:pwd@server.domain:5432/main

The "main" environment url can be over-ridden with **ANY** of the following environment variables:

.. code-block::bash

   $ODC_DEFAULT_DB_URL
   $ODC_COMMON_DB_URL
   $ODC_MAIN_DB_URL

The environment variable using the canonical environment name (``$ODC_MAIN_DB_URL`` in this case) always
takes precedence if it set. If more than one alias environment name is used (e.g. if both ``$ODC_DEFAULT_DB_URL``
**AND** ``$ODC_COMMON_DB_URL`` exist) then only one will be read and the implementation makes no guarantees
about which.  Therefore canonical environment names are strongly recommended for environment variable names where
possible.

4c. Deprecated Legacy Environment Variables
+++++++++++++++++++++++++++++++++++++++++++

Some legacy environment variable names are also read for backwards
compatibility reasons, however they may not work as expected where more than
one ODC environment is in use and will generate a deprecation warning if they
are read from.  The preferred new environment variable name will be included in
the text of the deprecation warning.

Most notably the old database connection environment variables:

.. code-block::bash

   $DB_DATABASE
   $DB_HOSTNAME
   $DB_PORT
   $DB_USERNAME
   $DB_PASSWORD

are strongly deprecated as they will be applied to ALL environments, which is probably not what you intended.

The new preferred configuration environment variable names all begin with ``ODC_``

Migrating from datacube-1.8
===========================

The new configuration engine introduced in datacube-1.9 is not fully backwards compatible with that used
previously.  This section notes the changes which administrators and maintainers should be aware of before
upgrading.

Merging multiple config files
-----------------------------

Previously, multiple config files could be read simultaneously and merged with "higher priority" files being
read later, and overriding the contents of "lower priority" files.

This is no longer supported.  Only one configuration file is read.

Where users previously created a local personal configuration file that supplemented a global system
configuration file, they should now make a copy of the global system configuration file, edit it with
their own personal extensions, and ensure that it is read in preference to the global file - or choose
one of the other methods for passing in configuration.

The special "user" section is also no longer supported as it doesn't make sense without merging of multiple
config files.

Legacy Environment Variables
----------------------------

Legacy environment variables are deprecated, but still read to assist with migration.  In all cases there is
a new preferred environment variable, as listed in the table below.


+------------------------------+-----------------------------------+---------------------------------------------+
| Legacy Environment Variable  | New Environment Variable(s)       |  Notes                                      |
+==============================+===================================+=============================================+
| DATACUBE_CONFIG_PATH         | :envvar:`ODC_CONFIG_PATH`         | Behaviour is slightly different, mostly due |
|                              |                                   | to only reading a single file.              |
+------------------------------+-----------------------------------+---------------------------------------------+
| DATACUBE_DB_URL              | ODC_<env_name>_DB_URL             | These legacy environment variables apply    |
|                              |                                   | to ALL environments - which is probably not |
+------------------------------+-----------------------------------+ what you want.                              |
| DB_DATABASE                  | ODC_<env_name>_DB_DATABASE        |                                             |
+------------------------------+-----------------------------------+                                             |
| DB_HOSTNAME                  | ODC_<env_name>_DB_HOSTNAME        |                                             |
+------------------------------+-----------------------------------+                                             |
| DB_PORT                      | ODC_<env_name>_DB_PORT            |                                             |
+------------------------------+-----------------------------------+                                             |
| DB_USERNAME                  | ODC_<env_name>_DB_USERNAME        |                                             |
+------------------------------+-----------------------------------+                                             |
| DB_PASSWORD                  | ODC_<env_name>_DB_PASSWORD        |                                             |
+------------------------------+-----------------------------------+---------------------------------------------+
| DATACUBE_ENVIRONMENT         | :envvar:`ODC_ENVIRONMENT`         | datacube-1.8 used this legacy environment   |
|                              |                                   | variable fairly inconsistently.  There are  |
|                              |                                   | several corner cases where it is now read   |
|                              |                                   | where it was not previously.                |
+------------------------------+-----------------------------------+---------------------------------------------+

The auto_config() function
--------------------------

There used to be an undocumentd ``auto_config()`` function (also available through ``python -m datacube``) that read
in the configuration (from multiple files and environment variables) and wrote it out as a single consolidated
configuration file.

As the new configuration engine is more clearly documented and more predictable in its behaviour, this functionality
is no longer required.

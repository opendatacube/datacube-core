
ODC Configuration (Details)
***************************

Open Data Cube configuration files, supported configuration options, and basic use cases and defaults
are described in :doc:`configuration`.

The Open Data Cube uses configuration files and/or environment variables to determine how to connect to databases.

Summary
=======

The default behaviour is to read in configuration from a `configuration file`_.

Alternatively, `Raw configuration`_ can be explicitly passed in.

Data in a configuration file can be supplemented or overridden by configuration from `environment variables`_.

One configuration can define multiple environments, so users must `choose one`_.

The configuration engine in 1.9 is not 100% compatible with the previous configuration engine.  Advanced
users and developers upgrading 1.8 systems should read the `migration notes`_.

.. _`configuration file`: #file-configuration
.. _`choose one`: #the-active-environment
.. _`environment variables`: #generic-environment-variable-overrides
.. _`migration notes`: #migrating-from-datacube-1-8

1. File configuration
---------------------

.. highlight:: python

If `Raw configuration`_ is not passed in, ODC attempts to find a configuration file in the file system.

`Only one`_ configuration file is read.

If your previous practice was to extend a shared system configuration file with a local
user configuration file, then you will now need to take a copy of the system configuration file,
add your extensions to your copy, and ensure that the Open Data Cube reads from your
modified file.

1a. Default Search Paths
++++++++++++++++++++++++

If no config file paths have been specified through any of the methods below (1b. through
1d.), the Open Data Cube will search for the following paths in order and use the
first readable file it finds:

1. :file:`./datacube.conf`    (in the current working directory)
2. :file:`~/.datacube.conf`   (in the user's home directory)
3. :file:`/etc/default/datacube.conf`
4. :file:`/etc/datacube.conf`

If none of the files in the default search path exist, then a basic default configuration is used, equivalent to:

.. code-block:: yaml

   default:
      db_hostname: ''
      db_database: datacube
      index_driver: default
      db_connection_timeout: 60

.. note:: Note
  This default config is only used after exhausting the default search path. If you have
  provided your own search path via any of the below methods and none of the paths exist, then an error is raised.

1b. In Python
+++++++++++++

In Python, the ``config`` argument can take a path to a config file::

    dc = Datacube(config="/path/to/my/file.conf")

The ``config`` argument can also take a priority list of config paths.
The first readable path in the list is used.
If none of the files in the list no configuration file can be found, a :py:class:`ConfigException` is raised:

::

     dc = Datacube(config=[
         "/first/path/checked",
         "/second/path/checked",
         "/last/path/checked",
     ])

The config argument can also take a :py:class:`cfg.ODCConfig` object.  Refer to
the API documentation for more information.

1c. Via the datacube CLI
++++++++++++++++++++++++

Configuration file paths can be passed using either the :option:`datacube -C`
or :option:`datacube --config` option.

The option can be specified multiple times, with paths being searched in order, and an error being
raised if none can be read.

1d. Via an Environment Variable
+++++++++++++++++++++++++++++++

.. envvar:: ODC_CONFIG_PATH

   If config paths have not been passed in through methods 2a. or 2b. above,
   then they can be read from the :envvar:`ODC_CONFIG_PATH` environment
   variable, in a UNIX Path-style colon separated list::

          ODC_CONFIG_PATH=/first/path/checked:/second/path/checked:/last/path/checked

.. _Raw configuration:

2. Raw configuration
--------------------

Raw configuration can be passed in explicitly, without ever reading from a configuration file on disk.

Attempting to pass in both raw configuration and a configuration file path simultaneously will raise a
:py:class:`ConfigException`.

`Environment variable overrides`_ do **NOT** apply to configuration environments defined in raw configuration.

However **new** `dynamic environments`_ that do not explicitly appear in raw configuration **CAN** still be defined by
environment variable.

.. _`Environment variable overrides`: #generic-environment-variable-overrides
.. _`dynamic environments`: #a-dynamic-environments

2a. Via Python (str or dict)
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

2b. As a string, via the datacube CLI
+++++++++++++++++++++++++++++++++++++

The contents of a configuration file can be passed into the ``datacube`` CLI via the ``-R`` or
``--raw-config`` command line option::

   datacube --raw-config "default: {db_database: this_db}"

Output from a script that generates a configuration file dynamically can be passed in using
a BASH backquote string::

   datacube --raw-config "`config_file_generator --option blah`"

2c. As a string, via an Environment Variable
++++++++++++++++++++++++++++++++++++++++++++

If raw configuration has not been passed in via methods 1a. or 1b.
above, then the contents of a configuration file can be written in full to the
:envvar:`ODC_CONFIG` environment variable:

.. code-block:: console

   $ ODC_CONFIG="default: {db_database: this_db}"
   $ datacube check    # will use the this_db database

3. The Active Environment
-------------------------

Each :py:class:`Datacube` object is associated with a particular environment. Multiple environments can be
accessed by instantiating multiple ``Datacube`` objects.  The environment associated with a particular
``Datacube`` object is determined when the object is first instantiated and cannot subsequently be changed.

3a. Default Environment
+++++++++++++++++++++++

If not specified by any of the methods 3b. to 3d. below, the ``default``
environment is used.  If no ``default`` environment is known, an error is
raised.  It is strongly recommended that a ``default`` environment be defined
in all configuration files - ideally as an alias to an explicitly
defined environment.

If no environment named ``default`` is known, but one named ``datacube`` **IS**
known, the ``datacube`` environment is used and a deprecation warning issued.
``datacube`` will be dropped as a legacy default environment name in a future
release.

An all-defaults environment is used when requesting an environment that does not exist.

3b. Specifying in Python
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

3c. Specifying in the CLI
+++++++++++++++++++++++++

The active environment can be selected in Python with the ``-E`` or ``--env`` option to the ``datacube``
CLI tool.

CLI commands that require more than one environment will have a second option for the second argument.
Refer to the ``--help`` text for more information.

3d. Via an Environment Variable
+++++++++++++++++++++++++++++++

.. envvar:: ODC_ENVIRONMENT

   If not explicitly specified via methods 3a. and 3b. above, the active
   environment can be specified with the ``$ODC_ENVIRONMENT`` environment
   variable.

.. _generic-environment-variables:

4. Generic Environment Variable Overrides
-----------------------------------------

Configuration values in config files can be overridden by setting the appropriate environment variable.

The name of overriding environment variables are all upper-case and structured:

.. code-block:: bash

   $ODC_{environment name}_{option name}

E.g. to override the :confval:`db_password` field in the ``main`` environment,
set the ``$ODC_MAIN_DB_PASSWORD`` environment variable.

Environment variable overrides are **NOT** applied to environments defined in
raw configuration that was passed in `explicitly as a string or dictionary <Raw configuration>`_.


4a. Dynamic Environments
++++++++++++++++++++++++

It is possible for environments to be defined dynamically purely in environment variables.

E.g. given the following active configuration file:

.. code-block:: yaml

     default:
         alias: main
     main:
         index_driver: postgres
         db_url: postgresql://myuser:mypassword@server.domain/main

and the following defined environment variables:

.. code-block:: bash

   ODC_AUX_INDEX_DRIVER=postgis
   ODC_AUX_DB_URL=postgres://auxuser:secret@backup.domain/aux

You can request the ``aux`` environment and its configuration will be
dynamically read from the environment variables, even though the "aux"
environment is not mentioned in the configuration file at all.

.. note::

   #. Environment variables are read when first accessing to a named environment (usually just before
      connecting to a database from that environment).  Dynamic changes to environment variables after
      first access have no effect.
   #. Environment variables cannot override values included in `Raw configuration`_, but can still
      be used to create `dynamic environments`_.

4b. Environment Variable Overrides and Environment Aliases
++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

Aliases can only be defined in raw configuration or in config files - they cannot be defined
through environment variables.

i.e. defining ``ODC_ENV2_ALIAS=env1`` does NOT create an ``env2`` alias to the ``env1``
environment.

A configuration file may define an environment which is an alias to an environment that is to be loaded
dynamically and is NOT defined in the configuration file.

Aliases (created in raw config or a config file) **ARE** honoured when interpreting environment variables.

E.g.  Given config file:

.. code-block:: yaml

     default:
          alias: main
     common:
          alias: main
     main:
          index_driver: postgis
          db_url: postgresql://uid:pwd@server.domain:5432/main

The "main" environment url can be overridden with **ANY** of the following environment variables:

.. code-block:: bash

   $ODC_DEFAULT_DB_URL
   $ODC_COMMON_DB_URL
   $ODC_MAIN_DB_URL

The environment variable using the canonical environment name (``$ODC_MAIN_DB_URL`` in this case) always
takes precedence if it is set. If more than one alias environment name is used (e.g. if both ``$ODC_DEFAULT_DB_URL``
**AND** ``$ODC_COMMON_DB_URL`` exist and ``$ODC_MAIN_DB_URL`` does not) then only one will be read and the
implementation makes no guarantees about which.  Therefore canonical environment names are strongly recommended
for environment variable names where possible.

4c. Deprecated Legacy Environment Variables
+++++++++++++++++++++++++++++++++++++++++++

Some legacy environment variable names are also read for backwards
compatibility reasons, however they may not work as expected where more than
one ODC environment is in use and will generate a deprecation warning if they
are read from.  The preferred new environment variable name will be included in
the text of the deprecation warning.

Most notably the old database connection environment variables:

.. code-block:: bash

   $DB_DATABASE
   $DB_HOSTNAME
   $DB_PORT
   $DB_USERNAME
   $DB_PASSWORD

apply to ALL environments, and are deprecated.

The new preferred configuration environment variable names all begin with ``ODC_``

Migrating from datacube-1.8
===========================

The new configuration engine introduced in datacube-1.9 is not fully backwards compatible with that used
previously.  This section notes the changes which administrators, maintainers and developers should be aware
of before upgrading.

.. _`Only one`:

Merging multiple config files
-----------------------------

Previously, multiple config files could be read simultaneously and merged with "higher priority" files being
read later, and overriding the contents of "lower priority" files.

This is no longer supported.  Only one configuration file is now read.

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
| DATACUBE_CONFIG_PATH         | :envvar:`ODC_CONFIG_PATH`         | Behaviour is different for lists of paths,  |
|                              |                                   | due to only reading a single file.          |
+------------------------------+-----------------------------------+---------------------------------------------+
| DATACUBE_DB_URL              | ODC_<env_name>_DB_URL             | These legacy environment variables apply    |
|                              |                                   | to ALL environments - which is probably not |
+------------------------------+-----------------------------------+ what you want in a multi-db scenario.       |
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

API changes
-----------

Details of the new API are described in :doc:`/api/cfg`.

The old ``datacube.config.LocalConfig`` class has been replaced by ``datacube.cfg.ODCConfig`` and ``datacube.cfg.ODCEnvironment`` classes.

For most users the only method you need is ``ODCConfig.get_environment()``

The auto_config() function
--------------------------

There used to be an undocumented ``auto_config()`` function (also available through ``python -m datacube``) that read
in the configuration (from multiple files and environment variables) and wrote it out as a single consolidated
configuration file.

As the new configuration engine is more clearly documented and more predictable in its behaviour, this functionality
is no longer required.

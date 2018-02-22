.. _user_config:

User Configuration
******************


It is possible to connect to multiple Data Cube indexes from within the one python process.
When initialising a :class:`~.Datacube` instance, it will load configuration options from one or more
config files. These configuration options define which indexes are available, and any parameters required to connect
to them.


Types of Indexes
================
At the moment, there are two types of indexes supported, but in the future we expect to support more. The two
indexes currently are the standard PostgreSQL backed index, and the other is an extension to the standard index, with
additional support for data stored in the ``S3 AIO`` format.

The type of index driver to use is defined by the `index_driver` option in each section of the user config file.


.. _runtime-config-doc:

Runtime Config
==============

The runtime config specifies configuration options for the current user, such as
available Data Cube instances and which to use by default.

This is loaded from the following locations in order, if they exist, with properties from latter files
overriding those in earlier ones:

 * ``/etc/datacube.conf``
 * ``$DATACUBE_CONFIG_PATH``
 * ``~/.datacube.conf``
 * ``datacube.conf``

Example:

.. code-block:: ini

    [default]
    db_database: datacube

    # A blank host will use a local socket. Specify a hostname (such as localhost) to use TCP.
    db_hostname:

    # Credentials are optional: you might have other Postgres authentication configured.
    # The default username is the current user id
    # db_username:
    # A blank password will fall back to default postgres driver authentication, such as reading your ~/.pgpass file.
    # db_password:
    index_driver: pg


    ## Development environment ##
    [dev]
    # These fields are all the defaults, so they could be omitted, but are here for reference

    db_database: datacube

    # A blank host will use a local socket. Specify a hostname (such as localhost) to use TCP.
    db_hostname:

    # Credentials are optional: you might have other Postgres authentication configured.
    # The default username is the current user id
    # db_username:
    # A blank password will fall back to default postgres driver authentication, such as reading your ~/.pgpass file.
    # db_password:

    ## Staging environment ##
    [staging]
    db_hostname: staging.dea.ga.gov.au

    [s3_test]
    db_hostname: staging.dea.ga.gov.au
    index_driver: s3aio

Note that the staging environment only specifies the hostname, all other fields will use default values (dbname
datacube, current username, password loaded from ``~/.pgpass``)

When using the datacube, it will use your default environment unless you specify one explicitly

eg.

.. code-block:: python

    with Datacube(env='staging') as dc:
        ...

or for cli commmands ``-E <name>``::

    datacube -E staging system check

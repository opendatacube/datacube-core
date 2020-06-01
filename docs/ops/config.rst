.. _environment_config:

Environment Configuration
*************************


It is possible to connect to multiple Data Cube indexes from within the one
python process.  When creating a :class:`~.Datacube` instance, it will load
configuration options from files or environment variables. This determine which
database will be connected to, and thus which Products will be available.


.. _runtime-config-doc:

Runtime Configuration
=====================

The runtime config specifies configuration options for the current user, such as
available Data Cube instances and which to use by default.
Open Data Cube needs to be told which database to connect to. This can be
done via a file specifying one or more environments, or via environment variables.


Configuration from a file
-------------------------

This is loaded from the following locations in order, if they exist, with
properties from latter files overriding those in earlier ones:

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


Note that the ``staging`` environment only specifies the hostname, all other
fields will use default values (database ``datacube``, current username,
password loaded from ``~/.pgpass``)

When using the datacube, it will use your default environment unless you specify one explicitly

eg.

.. code-block:: python

    with Datacube(env='staging') as dc:
        ...

or for cli commmands ``-E <name>``::

    datacube -E staging system check


Configuration via Environment Variables
---------------------------------------

The runtime config specifies configuration options for the current user, such as
available Data Cube instances and which to use by default.

It is possible to configure datacube with a single environment variable:
``DATACUBE_DB_URL``. This is useful when using datacube applications
inside a docker image. The format of the URL is the same as used by SQLAclchemy:
``postgresql://user:password@host:port/database``. Only the ``database`` parameter
is required. Note that ``password`` is url encoded, so it can contain special
characters. 

For more information refer to the `SQLAlchemy database URLs documentation
<https://docs.sqlalchemy.org/en/13/core/engines.html#database-urls>`_.

Examples:

``postgresql:///datacube``
   Connect to a local database ``datacube`` via UNIX socket.

``postgresql://ro_user:secret123@db.host.tld/db1``
   Connect to database named ``db1`` on the remote server ``db.host.tld`` on
   the default port (5432) using ``ro_user`` username and password
   ``secret123``.

``postgresql://ro_user:secret%21%25@db.host.tld:6432/db1``
   Same as above but using port ``6432`` and password ``secret!%``.


It is also possible to use separate environment variables for each component of
the connection URL. The recognised environment variables are 
``DB_HOSTNAME``, ``DB_PORT``, ``DB_USERNAME``, ``DB_PASSWORD`` and ``DB_DATABASE``.

Types of Indexes
================

It is possible to implement custom index driver and hook it into the datacube
via plugin mechanism. This is an experimental feature that was used to
investigate ``S3 AIO`` format. The index driver interface however is not
well defined and it is unrealistic to implement a completely new backend. One
could however extend existing PostgreSQL backend, and this was the strategy used
by ``S3 AIO`` driver before it got decommissioned.

The type of index driver to use is defined by the ``index_driver`` option in
each section of the user config file.

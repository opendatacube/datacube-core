.. _environment_config:

Environment Configuration
*************************


It is possible to connect to multiple Data Cube indexes from within the one python process.
When initialising a :class:`~.Datacube` instance, it will load configuration options from one or more
config files. These configuration options define which indexes are available, and any parameters required to connect
to them.


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


.. _runtime-config-doc:

Runtime Config
==============

Data Cube needs to be told which database index to connect to. This can be done via a file with profiles,
or directly via environment variables.

The runtime config specifies configuration options for the current user, such as
available Data Cube instances and which to use by default.

Configuration from a file
-------------------------

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


Note that the ``staging`` environment only specifies the hostname, all other fields will use default values (database
``datacube``, current username, password loaded from ``~/.pgpass``)

When using the datacube, it will use your default environment unless you specify one explicitly

eg.

.. code-block:: python

    with Datacube(env='staging') as dc:
        ...

or for cli commmands ``-E <name>``::

    datacube -E staging system check


Configuration via Environment Variables
---------------------------------------

It is also possible to configure datacube with a single environment variable:
``DATACUBE_DB_URL``. This is often convenient when using datacube applications
inside a docker image. Format of the URL is the same as used by SQLAclchemy:
``postgresql://user:password@host:port/database``. Only ``database`` parameter
is compulsory. Note that ``password`` is url encoded, so it can contain special
characters. For more information you can consult `SQLAlchemy documentations
<https://docs.sqlalchemy.org/en/13/core/engines.html#database-urls>`_

Examples:

``postgresql:///datacube``
   Connect to local database ``datacube`` via UNIX socket.

``postgresql://ro_user:secret123@db.host.tld/db1``
   Connect to database ``db1`` on a remote server ``db.host.tld`` on
   the default port (5432) using ``ro_user`` username with password
   ``secret123``.

``postgresql://ro_user:secret%21%25@db.host.tld:6432/db1``
   Same as above but using port ``6432`` and password ``secret!%``.

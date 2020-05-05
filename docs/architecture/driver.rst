.. _extending-datacube:

Extending Datacube
******************

Beyond the configuration available in ODC, there are three extension points
provided for implementing different types of data storage and indexing.

 - Drivers for Reading Data
 - Drivers for Writing Data
 - Alternative types of Index

Support for Plug-in drivers
===========================

A light weight implementation of a driver loading system has been
implemented in `datacube/drivers/driver_cache.py`_
which uses `setuptools dynamic service and plugin discovery mechanism`_ 
to name and define available drivers. This code caches the available
drivers in the current environment, and allows them to be loaded on
demand, as well as handling any failures due to missing dependencies or
other environment issues.

.. _datacube/drivers/driver_cache.py: https://github.com/opendatacube/datacube-core/blob/60187e38669d529c55d05a962bd7c5288d906f1b/datacube/drivers/driver_cache.py
.. _setuptools dynamic service and plugin discovery mechanism: http://setuptools.readthedocs.io/en/latest/setuptools.html#dynamic-discovery-of-services-and-plugins


Data Read Plug-ins
==================

:Entry point group: `datacube.plugins.io.read <https://github.com/opendatacube/datacube-core/blob/9c0ea8923fa5d29dc2a813141ad64daea74c4902/setup.py#L104>`__.

Read plug-ins are specified as supporting particular **uri protocols**
and **formats**, both of which are fields available on existing
``Datasets``

A ReadDriver returns a ``DataSource`` implementation, which is chosen
based on:

-  Dataset URI protocol, eg. ``s3://``
-  Dataset format. As stored in the Data Cube ``Dataset``.
-  Current system settings
-  Available IO plugins

If no specific ``DataSource`` can be found, a default
:py:class:`datacube.storage.storage.RasterDatasetDataSource` is returned, which uses ``rasterio`` to read
from the local file system or a network resource.

The ``DataSource`` maintains the same interface as before, which works
at the individual *dataset+time+band* level for loading data. This is
something to be addressed in the future.

Example code to implement a reader driver
-----------------------------------------

.. code:: python

    def init_reader_driver():
        return AbstractReaderDriver()

    class AbstractReaderDriver(object):
        def supports(self, protocol: str, fmt: str) -> bool:
            pass
        def new_datasource(self, band: BandInfo) -> DataSource:
            return AbstractDataSource(band)

    class AbstractDataSource(object):  # Same interface as before
        ...

Driver specific metadata will be present in ``BandInfo.driver_data`` if saved during ``write_dataset_to_storage``

Example Pickle Based Driver
---------------------------

Available in ``/examples/io_plugin``. Includes an example ``setup.py``
as well as an example **Read** and **Write** Drivers.

.. _write_plugin:

Data Write Plug-ins
===================

:Entry point group:
   `datacube.plugins.io.write <https://github.com/opendatacube/datacube-core/blob/9c0ea8923fa5d29dc2a813141ad64daea74c4902/setup.py#L107>`__

Are selected based on their name. The ``storage.driver`` field has been
added to the ingestion configuration file which specifies the name of
the write driver to use. Drivers can specify a list of names that they
can be known by, as well as publicly defining their output format,
however this information isn't used by the ingester to decide which
driver to use. Not specifying a driver counts as an error, there is no
default.

At this stage there is no decision on what sort of a public API to
expose, but the ``write_dataset_to_storage()`` method implemented in
each driver is the closest we've got. The **ingester** is using it to
write data.

Example code to implement a writer driver
-----------------------------------------

.. code:: python

    def init_writer_driver():
        return AbstractWriterDriver()

    class AbstractWriterDriver(object):
        @property
        def aliases(self):
            return []  # List of names this writer answers to

        @property
        def format(self):
            return ''  # Format that this writer supports

        def write_dataset_to_storage(self, dataset, filename,
                                     global_attributes=None,
                                     variable_params=None,
                                     storage_config=None,
                                     **kwargs):
            ...
            return {}  # Can return extra metadata to be saved in the index with the dataset

Extra metadata will be saved into the database and loaded into ``BandInfo`` during a load operation.

NetCDF Writer Driver
--------------------

:Name: ``netcdf``, ``NetCDF CF``
:Format: ``NetCDF``
:Implementation:
   :py:class:`datacube.drivers.netcdf.driver.NetcdfWriterDriver`

Index Plug-ins
==============

:Entry point group:
   `datacube.plugins.index <https://github.com/opendatacube/datacube-core/blob/9c0ea8923fa5d29dc2a813141ad64daea74c4902/setup.py#L112>`__

A connection to an ``Index`` is required to find data in the Data Cube.
Already implemented in the ``develop`` branch was the concept of
``environments`` which are a named set of configuration parameters used
to connect to an ``Index``. This PR extends this with an
``index_driver`` parameter, which specifies the name of the Index Driver
to use. If this parameter is missing, it falls back to using the default
PostgreSQL Index.

Example code to implement an index driver
-----------------------------------------

.. code:: python

    def index_driver_init():
        return AbstractIndexDriver()

    class AbstractIndexDriver(object):
        @staticmethod
        def connect_to_index(config, application_name=None, validate_connection=True):
            return Index.from_config(config, application_name, validate_connection)

Default Implementation
----------------------

The default ``Index`` uses a PostgreSQL database for all storage and
retrieval.


Drivers Plugin Management Module
================================

Drivers are registered in ``setup.py -> entry_points``::

    entry_points={
        'datacube.plugins.io.read': [
            'netcdf = datacube.drivers.netcdf.driver:reader_driver_init',
        ],
        'datacube.plugins.io.write': [
            'netcdf = datacube.drivers.netcdf.driver:writer_driver_init',
        ],
        'datacube.plugins.index': [
            'default = datacube.index.index:index_driver_init',
            *extra_plugins['index'],
        ],
    }

These are drivers ``datacube-core`` ships with. When developing custom driver one
does not need to add them to ``datacube-core/setup.py``, rather you have to define
these in the ``setup.py`` of your driver package.


Data Cube Drivers API
=====================

.. automodule:: datacube.drivers
    :members:
    :imported-members:

References and History
======================

- :pull:`346`
-  `Pluggable Back Ends Discussion [7 December
   2017] <https://github.com/orgs/opendatacube/teams/developers/discussions/2>`__
-  Teleconference with @omad @petewa @rtaib @Kirill888 on *12 December
   2017*.
-  `Notes from ODC Storage and Index Driver
   Meeting <https://docs.google.com/document/d/1l2xOaKyvQRV4h35NELKvyM3DYOUosXJhcj-lgHC8MN4/edit>`__

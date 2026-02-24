Extending the Open Data Cube
****************************

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
.. _setuptools dynamic service and plugin discovery mechanism: https://packaging.python.org/guides/creating-and-discovering-plugins/#using-package-metadata


Data Read Plug-ins
==================

:Entry point group: ``datacube.plugins.io.read``.

Read plug-ins are specified as supporting particular **uri protocols**
and **formats**, both of which are fields available on existing
``Datasets``

A ReadDriver returns a ``DataSource`` implementation, which is chosen
based on:

-  Dataset URI protocol, eg. ``s3://``
-  Dataset format. As stored in the Data Cube ``Dataset``.
-  Current system settings
-  Available IO plugins

If no specific :py:class:`datasource.storage.DataSource` can be found, a default
:py:class:`datacube.storage._rio.RasterDatasetDataSource` is returned, which uses ``rasterio`` to read
from the local file system or a network resource.

The ``DataSource`` maintains the same interface as before, which works
at the individual *dataset+time+band* level for loading data. This is
something to be addressed in the future.

See also `odc-loader`_ for a full-scale implementation.

.. _`odc-loader`: https://github.com/opendatacube/odc-loader

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

Available in ``/examples/io_plugin``. Includes an example ``pyproject.toml``
as well as example **Read** and **Write** Drivers.

.. _write_plugin:

Data Write Plug-ins
===================

:Entry point group: ``datacube.plugins.io.write``

Are selected based on their name. The ``storage.driver`` field has been
added to the ingestion configuration file which specifies the name of
the write driver to use. Drivers can specify a list of names that they
can be known by, as well as publicly defining their output format,
however this information isn't used by the ingester to decide which
driver to use. Not specifying a driver counts as an error, there is no
default.

Data write support is not currently a priority and may be removed
or relocated into another package in future.

See also `odc-geo`_'s COG-writing methods.

.. _`odc-geo`: https://github.com/opendatacube/odc-geo

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

        def mk_uri(self, file_path, storage_config):
            """
            Constructs a URI from the file_path and storage config.

            A typical implementation should return f'{scheme}://{file_path}'

            Example:
                file_path = '/path/to/my_file.nc'
                storage_config = {'driver': 'NetCDF CF'}

                mk_uri(file_path, storage_config) should return 'file:///path/to/my_file.nc'

            :param Path file_path: The file path of the file to be converted into a URI during the ingest process.
            :param dict storage_config: The dict holding the storage config found in the ingest definition.
            :return: file_path as a URI that the Driver understands.
            :rtype: str
            """
            return f'file://{file_path}'  # URI that this writer supports

        def write_dataset_to_storage(self, dataset, file_uri,
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

:Entry point group: ``datacube.plugins.index``

A connection to an ``Index`` is required to find data in the Data Cube.

ODC Configuration environments have an ``index_driver`` parameter, which specifies the name of the Index Driver
to use. See :doc:`database/configuration`.

A set of abstract base classes are defined in :py:mod:`datacube.index.abstract`. An index plugin
is expected to supply implementations of all these abstract base classes. If any abstract
methods is not relevant to or implementable by a particular Index Driver, that method should
defined to raise a ``NotImplementedError``.

Legacy Implementation
---------------------

The legacy ``postgres`` index driver uses a PostgreSQL database for all storage and
retrieval, with search supported by json indexes.

PostGIS Implementation
----------------------

The new ``postgis`` index driver uses a PostgreSQL database for all storage and
retrieval, with spatial search using postgis spatial indexes.

Default Implementation
----------------------

Not specifying an index driver (or specifying ``default`` as the index driver) results in
the ``postgres`` index driver. In a future release, this will switch to the ``postgis`` index
driver.

Null Implementation
-------------------

``datacube-core`` includes a minimal "null" index driver, that implements an index that is always
empty. The code for this driver is located at ``datacube.index.null`` and can be used by setting
the ``index_driver`` to ``null`` in the configuration file.

The null index driver may be useful:

1. for ODC use cases where no database access is required;
2. for testing scenarios where no database access is required; or
3. as an example/template for developing other index drivers.

Memory Implementation
---------------------

``datacube-core`` includes a non-persistent, local, in-memory index driver.  The index is maintained
in local memory and is not backed by a database.
The code for this driver is located at ``datacube.index.memory`` and can be used by setting
the ``index_driver`` to ``memory`` in the configuration file.

The memory index driver may be useful:

1. for ODC use cases where there is no need for the index to be reused beyond the current session;
2. for testing scenarios where no index persistence is required; or
3. as an example/template for developing other index drivers.


Drivers Plugin Management Module
================================

Drivers are registered in ``pyproject.toml -> [project.entry-points]``::

    [project.entry-points."datacube.plugins.io.read"]
    netcdf = 'datacube.drivers.netcdf.driver:reader_driver_init'

    [project.entry-points."datacube.plugins.io.write"]
    netcdf = 'datacube.drivers.netcdf.driver:writer_driver_init'

    [project.entry-points."datacube.plugins.index"]
    postgres = 'datacube.index.postgres.index:index_driver_init'
    null = 'datacube.index.null.index:index_driver_init'
    memory = 'datacube.index.memory.index:index_driver_init'
    postgis = 'datacube.index.postgis.index:index_driver_init'

These are drivers ``datacube-core`` ships with. When developing a custom driver one
does not need to add them to ``datacube-core/pyproject.toml``, rather you have to define
these in the ``pyproject.toml`` of your driver package.


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
-  `Notes from ODC Storage and Index Driver
   Meeting <https://docs.google.com/document/d/1l2xOaKyvQRV4h35NELKvyM3DYOUosXJhcj-lgHC8MN4/edit>`__

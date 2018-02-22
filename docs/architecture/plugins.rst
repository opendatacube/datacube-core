Plugins
*******

Reason for Pull Request
=======================

Although initially the ``DriverManager`` code seemed to address the need
for extensibility in Data Cube. Upon trying to use it in the NCI/GA
deployment we discovered a few show-stopper issues including:

-  Passing the ``DriverManager`` around everywhere broke the distributed
   bulk processing.
-  Database connections were being created unnecessarily in workers
   which simply needed to load pre-specified data.
-  While working with files stored on S3, we ran into conflicts with the
   S3+Block driver which was registered to handle the ``s3://``
   protocol.

In early December @omad @petewa @rtaib and @Kirill888 discussed some
potential solutions. These discussions are documented `in github
discussions <https://github.com/orgs/opendatacube/teams/developers/discussions/2>`__
and `in notes from a videoconference
meeting <https://docs.google.com/document/d/1l2xOaKyvQRV4h35NELKvyM3DYOUosXJhcj-lgHC8MN4/edit#heading=h.h400fj5rkdxg>`__.
Since then @omad and @Kirill888 have been working on implementing the
proposed changes.

This Pull Request is our proposed implementation.


Index Plug-ins
==============

**Entry point group:**
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

S3 Extensions
^^^^^^^^^^^^^


The :py:class:`datacube.drivers.s3aio_index.S3AIOIndex` driver subclasses the default PostgreSQL Index with
support for saving additional data about the size and shape of chunks
stored in S3 objects. As such, it implements an identical interface,
while overriding the ``dataset.add()`` method to save the additional
data.

Data Read Plug-ins
~~~~~~~~~~~~~~~~~~

**Entry point group:**
`datacube.plugins.io.read <https://github.com/opendatacube/datacube-core/blob/9c0ea8923fa5d29dc2a813141ad64daea74c4902/setup.py#L104>`__.

Read plug-ins are specified as supporting particular **uri protocols**
and **formats**, both of which are fields available on existing
``Datasets``

A ReadDriver returns a ``DataSource`` implementation, which is chosen
based on:

-  Dataset URI (protocol part, eg: ``s3://``)
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
        def new_datasource(self, dataset, band_name) -> DataSource:
            return AbstractDataSource(dataset, band_name)

    class AbstractDataSource(object):  # Same interface as before
        ...

S3 Driver
---------

**URI Protocol:** ``s3://`` **Dataset Format:** ``aio``
**Implementation location:**
```datacube/drivers/s3/driver.py`` <https://github.com/opendatacube/datacube-core/blob/9c0ea8923fa5d29dc2a813141ad64daea74c4902/datacube/drivers/s3/driver.py>`__

Example Pickle Based Driver
---------------------------

Available in ``/examples/io_plugin``. Includes an example ``setup.py``
as well as an example **Read** and **Write** Drivers.

Data Write Plug-ins
===================

**Entry point group:**
```datacube.plugins.io.write`` <https://github.com/opendatacube/datacube-core/blob/9c0ea8923fa5d29dc2a813141ad64daea74c4902/setup.py#L107>`__

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

NetCDF Writer Driver
--------------------

**Name:** ``netcdf``, ``NetCDF CF`` **Format:** ``NetCDF``
**Implementation**:

:py:class:`datacube.drivers.netcdf.driver.NetcdfWriterDriver`

S3 Writer Driver
----------------

**Name:** ``s3aio`` **Protocol:** ``s3`` **Format:** ``aio``
**Implementation**:

:py:class:`datacube.drivers.s3.driver.S3WriterDriver`


Change to Ingestion Configuration
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Must now specify the **Write Driver** to use. For s3 ingestion there was
a top level ``container`` specified, which has been renamed and moved
under ``storage``. The entire ``storage`` section is passed through to
the **Write Driver**, so drivers requiring other configuration can
include them here. eg:

.. code:: yaml

    ...
    storage:
      ...
      driver: s3aio
      bucket: my_s3_bucket
    ...

References and History
======================

- :pull:`346`
-  `Pluggable Back Ends Discussion [7 December
   2017] <https://github.com/orgs/opendatacube/teams/developers/discussions/2>`__
-  Teleconference with @omad @petewa @rtaib @Kirill888 on *12 December
   2017*.
-  `Notes from ODC Storage and Index Driver
   Meeting <https://docs.google.com/document/d/1l2xOaKyvQRV4h35NELKvyM3DYOUosXJhcj-lgHC8MN4/edit#heading=h.h400fj5rkdxg>`__

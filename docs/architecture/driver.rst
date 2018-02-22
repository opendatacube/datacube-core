.. _extending-datacube:

Extending Datacube
******************

Beyond the configuration available in ODC, there are three
extension points provided for implementing different types of data storage and
indexing.

 - Drivers for Reading Data
 - Drivers for Writing Data
 - Alternative types of Index

Support for Plug-in drivers
===========================

A light weight implementation of a driver loading system has been
implemented in
`datacube/drivers/driver_cache.py <https://github.com/opendatacube/datacube-core/blob/60187e38669d529c55d05a962bd7c5288d906f1b/datacube/drivers/driver_cache.py>`__,
which uses `setuptools dynamic service and plugin discovery
mechanism <http://setuptools.readthedocs.io/en/latest/setuptools.html#dynamic-discovery-of-services-and-plugins>`__
to name and define available drivers. This code caches the available
drivers in the current environment, and allows them to be loaded on
demand, as well as handling any failures due to missing dependencies or
other environment issues.

Drivers for Reading Data
========================

``ReaderDriver``

Drivers for Writing Data
========================

``WriterDriver``


Alternative types of Index
==========================

``IndexDriverCache``

Drivers Plugin Management Module
================================

Drivers are defined in ``setup.py -> entry_points``::

    entry_points={
        'datacube.plugins.io.read': [
            's3aio = datacube.drivers.s3.driver:reader_driver_init'
        ],
        'datacube.plugins.io.write': [
            'netcdf = datacube.drivers.netcdf.driver:writer_driver_init',
            's3aio = datacube.drivers.s3.driver:writer_driver_init',
            's3aio_test = datacube.drivers.s3.driver:writer_test_driver_init',
        ]
    }


.. automodule:: datacube.drivers
    :members:

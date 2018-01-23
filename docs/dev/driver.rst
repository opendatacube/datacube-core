.. _extending-datacube:

Extending Datacube
==================

Beyond the very flexible configuration available in ODC, there are three
extension points provided for implementing different types of data storage and
indexing.

 - Drivers for Reading Data
 - Drivers for Writing Data
 - Alternative types of Index

Data Read Drivers
-----------------

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

This assigns the name of a 
A ``WriterDriver`` 


Storage and Indexing Drivers
============================

Drivers Plugin Management Module
--------------------------------

.. automodule:: datacube.drivers
    :members:

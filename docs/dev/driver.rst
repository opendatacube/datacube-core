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

Plug-in Drivers
---------------

.. autosummary::
   :toctree: generate/

   datacube.drivers.s3.driver

Drivers Plugin Management Module
--------------------------------

.. automodule:: datacube.drivers
    :members:

S3 Array IO
-----------

S3 Byte IO
~~~~~~~~~~

.. currentmodule:: datacube.drivers.s3.storage.s3aio

.. autosummary::
   :toctree: generate/

    S3IO.__init__
    S3IO.list_created_arrays
    S3IO.delete_created_arrays
    S3IO.s3_resource
    S3IO.s3_bucket
    S3IO.s3_object
    S3IO.list_buckets
    S3IO.list_objects
    S3IO.delete_objects
    S3IO.bucket_exists
    S3IO.object_exists
    S3IO.put_bytes
    S3IO.put_bytes_mpu
    S3IO.put_bytes_mpu_mp
    S3IO.get_bytes
    S3IO.get_byte_range
    S3IO.get_byte_range_mp

S3 Array IO
~~~~~~~~~~~

.. currentmodule:: datacube.drivers.s3.storage.s3aio

.. autosummary::
   :toctree: generate/

    S3AIO.__init__
    S3AIO.to_1d
    S3AIO.to_nd
    S3AIO.get_point
    S3AIO.get_slice
    S3AIO.get_slice_mp
    S3AIO.get_slice_by_bbox

S3 Labeled IO
~~~~~~~~~~~~~

.. currentmodule:: datacube.drivers.s3.storage.s3aio

.. autosummary::
   :toctree: generate/

    S3LIO.__init__
    S3LIO.chunk_indices_1d
    S3LIO.chunk_indices_nd
    S3LIO.put_array_in_s3
    S3LIO.put_array_in_s3_mp
    S3LIO.shard_array_to_s3
    S3LIO.shard_array_to_s3_mp
    S3LIO.assemble_array_from_s3
    S3LIO.regular_index
    S3LIO.get_data
    S3LIO.get_data_unlabeled
    S3LIO.get_data_unlabeled_mp

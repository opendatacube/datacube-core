Storage and Indexing Drivers
============================

Plug-in Drivers
---------------

.. autosummary::
   :toctree: generate/

   datacube.drivers.netcdf.driver
   datacube.drivers.s3.driver
   datacube.drivers.s3_test.driver

Drivers Plugin Management Module
--------------------------------

.. automodule:: datacube.drivers
    :members:

Abstract Driver Class
~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: datacube.drivers.driver.Driver
   :members:

Plugin Manager
~~~~~~~~~~~~~~

.. autoclass:: datacube.drivers.manager.DriverManager
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
    S3AIO.bytes_to_array
    S3AIO.copy_bytes_to_shared_array
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

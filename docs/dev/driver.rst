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

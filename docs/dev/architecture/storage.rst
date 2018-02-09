.. _dev_arch_storage:

Storage
=======
Formats
-------
Data can be stored in any format that can be read by a storage driver.


Storage Drivers
---------------
GDAL
~~~~
The GDAL-based driver uses `rasterio` to


S3IO
~~~~



Reading Data
------------

.. uml:: /diagrams/current_data_read_process.plantuml
   :caption: Current Data Read Process



Data Load Classes
-----------------

.. uml:: /diagrams/storage_drivers_old.plantuml
   :caption: Classes currently implementing the DataCube Data Read Functionality



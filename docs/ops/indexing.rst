.. highlight:: console

.. _indexing:

Indexing Data
=============

Once you have the datacube software installed and connected to a database, you
can start to load in some data. This step is performed using the **datacube**
command line tool.

When you load data into the Data Cube, all you are doing is recording the
existence and detailed metadata about the data into the **database**, none of
the raw data itself is copied or moved or transformed. This is therefore a
relatively fast and lightweight process.

Prerequisites for Indexing Data
-------------------------------

 * A working Data Cube setup
 * Some *Analysis Ready Data* to load
 * A Product Type configuration loaded into the database for each Dataset
 * Dataset YAML files for each dataset

.. _product-definitions:

Product Definitions
-------------------

The Data Cube can handle many different types of Data, and requires a bit of information up front to know what to do with them. This is the task of the Product Definition.

A Product Definition provides a short **name**, a **description**, some basic
source **metadata** and (optionally) a list of **measurements** describing the type of data that will be contained in the Datasets of it's type.

The **measurements** is an ordered list of data, which specify a **name** and some **aliases**, a data type or **dtype**, and some options extras including what type of **units** the measurement is in, a **nodata** value, and even a way of specifying **bit level descriptions** or the **spectral response** in the case of reflectance data.

A set of example Product definitions are supplied in `docs/config_samples/dataset_types` to cover some common Geoscience Australia and other Earth Observation Data.

Loading Product Definitions
---------------------------

To load Products into your Data Cube run::

    datacube product add <path-to-dataset-type-yml>


Dataset Documents
-----------------
As well as the product information loaded in the previous step, every Dataset
requires some metadata describing what the data represents and where it has come from, as well has what sort of files it is stored in. We call this *blah* and it is expected to be stored in _YAML_ documents. It is what is loaded into the Database for searching, querying and accessing the data.

In the case of data from Geoscience Australia, no further steps are required.

For third party datasets, see :ref:`prepare-scripts`.

:ref:`dataset-metadata-doc` is required to accompany the dataset for it to be
 recognised by Data Cube. It defines critical metadata of the dataset such as:

    - measurements
    - platform and sensor names
    - geospatial extents and projection
    - acquisition time

.. note::

    Some metadata requires cleanup before they are ready to be loaded.

For more information see :ref:`dataset-metadata-doc`.


Adding Some Data
----------------

Everything is now ready, and we can use the **datacube** tool to add one or more datasets into our Cube by running::

    datacube dataset add --auto-match <path-to-dataset>




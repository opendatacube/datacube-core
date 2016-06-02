.. _indexing:

Loading Data into your Data Cube - Indexing
===========================================

Once you have the datacube software installed and connected to a database, you can start to load in some data. This step is called **indexing**, and is performed using the **datacube** command line tool.

Prerequisites for Indexing Data
-------------------------------

 * A working Data Cube setup
 * Some *Analysis Ready Data*  to load
 * Dataset Description YAML files for each dataset
 * A Dataset Type configuration loading into the database

Adding Dataset Type Descriptions
--------------------------------

The Data Cube can handle many different types of Data, and requires a bit of information up front to know what to do with them. This is the task of the Dataset Type.

A Dataset Type provides a short **name**, a **description**, some basic source **metadata** and (optionally) a list of **measurements** describing the type of data that will be contained in the Datasets of it's type.

The **measurements** is an ordered list of data, which specify a **name** and some **aliases**, a data type or **dtype*, and some options extras including what type of **units** the measurement is in, a **nodata** value, and even a way of specifying **bit level descriptions** or the **spectral response** in the case of reflectance data.

A set of example Dataset Type descriptions are supplied in `docs/config_samples/dataset_types` to cover some common Geoscience Australia and other Earth Observation Data.

To load Dataset Types into your Data Cube run:

.. code-block:: console

    datacube-config type add <path-to-dataset-type-yml>


Dataset Documents
-----------------
As well as the type information loaded in the previous step, every Dataset requires some metadata describing what the data represents and where it has come from, as well has what sort of files it is stored in. We call this *blah* and it is expected to be stored in _YAML_ documents. It is what is loaded into the Database for searching, querying and accessing the data.






Indexing Some Data
------------------

Everything is ready now to re



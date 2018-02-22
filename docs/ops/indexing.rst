.. highlight:: console

.. _indexing:

Indexing Data
*************

Once you have the Data Cube software installed and connected to a database, you
can start to load in some data. This step is performed using the **datacube**
command line tool.

When you load data into the Data Cube, all you are doing is recording the
existence of and detailed metadata about the data into the **index**. None of
the data itself is copied, moved or transformed. This is therefore a relatively
safe and fast process.

Prerequisites for Indexing Data
===============================

 * A working Data Cube setup
 * Some *Analysis Ready Data* to load
 * A Product definition added to your Data Cube for each type of dataset
 * Dataset metadata documents for each individual dataset


Sample Earth Observation Data
=============================

The U.S. Geological Survey provides many freely available, Analysis Ready,
earth observation data products. The following are a good place to start
looking.

* Landsat
    * `USGS Landsat Surface Reflectance - LEDAPS 30m`__
* MODIS
    * `MCD43A1 - BRDF-Albedo Model Parameters 16-Day L3 Global 500m`__
    * `MCD43A2 - BRDF-Albedo Quality 16-Day L3 Global 500m`__
    * `MCD43A3 - Albedo 16-Day L3 Global 500m`__
    * `MCD43A4 - Nadir BRDF-Adjusted Reflectance 16-Day L3 Global 500m`__

__ http://landsat.usgs.gov/CDR_LSR.php
__ https://lpdaac.usgs.gov/dataset_discovery/modis/modis_products_table/mcd43a1
__ https://lpdaac.usgs.gov/dataset_discovery/modis/modis_products_table/mcd43a2
__ https://lpdaac.usgs.gov/dataset_discovery/modis/modis_products_table/mcd43a3
__ https://lpdaac.usgs.gov/dataset_discovery/modis/modis_products_table/mcd43a4

Once you have downloaded some data, it will need :ref:`metadata preparation
<prepare-scripts>` before use in the Data Cube.


.. _product-definitions:

Product Definition
==================

The Data Cube can handle many different types of data, and requires a bit of
information up front to know what to do with them. This is the task of a
Product Definition.

A Product Definition provides a short **name**, a **description**, some basic
source **metadata** and (optionally) a list of **measurements** describing the
type of data that will be contained in the Datasets of its type. In Landsat Surface
Reflectance, for example, the measurements are the list of bands.

The **measurements** is an ordered list of data, which specify a **name** and
some **aliases**, a data type or **dtype**, and some options extras including
what type of **units** the measurement is in, a **nodata** value, and even a way
of specifying **bit level descriptions** or the **spectral response** in the
case of reflectance data.

A set of example Product definitions are supplied in
`docs/config_samples/dataset_types` to cover some common Geoscience Australia
and other Earth Observation Data.

Loading Product Definitions
===========================

To load Products into your Data Cube run::

    datacube product add <path-to-dataset-type-yml>


Dataset Documents
=================

Every dataset requires a metadata document describing what the data represents and where it has come
from, as well has what format it is stored in. At a minimum, you need the dimensions or fields your want to
search by, such as lat, lon and time, but you can include any information you deem useful.

It is typically stored in YAML documents, but JSON is also supported. It is stored in the index
for searching, querying and accessing the data.

The data from Geoscience Australia already comes with relevant files (named ``ga-metadata.yaml``), so
no further steps are required for indexing them.

For third party datasets, see :ref:`prepare-scripts`.

A :ref:`dataset-metadata-doc` is required to accompany the dataset for it to be
recognised by the Data Cube. It defines critical metadata of the dataset such as:

    - measurements
    - platform and sensor names
    - geospatial extents and projection
    - acquisition time

.. note::

    Some metadata requires cleanup before they are ready to be loaded.

For more information see :ref:`dataset-metadata-doc`.


Adding Some Data
================

Everything is now ready, and we can use the **datacube** tool to add one or more
datasets into our Cube by running::

    datacube dataset add --auto-match <path-to-dataset>




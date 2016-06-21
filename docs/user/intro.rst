.. _introduction:

Overview
========

The Data Cube is a system designed to:

* Catalogue large amounts of Earth Observation data
* Provide a :term:`Python` based :term:`API` for high performance querying and data access
* Give scientists and other users easy ability to perform Exploratory Data Analysis
* Allow scalable continent scale processing of the stored data
* Track the providence of all the contained data to allow for quality control and updates

Getting Started
===============

If you're reading this, hopefully someone has already set up and loaded data into a Data Cube
for you.

Check out the :ref:`installation` for instructions on configuring and setting up


Australian Users
================

If you have a login with the :term:`NCI`, check out the :ref:`nci_usage_guide`


.. toctree::

   nci_usage


Types of Datasets in a Data Cube
================================

When using the Data Cube, it will contain records about 3 different types of
products and datasets.

========================= ============= ================
 Type of product/dataset   In Database   Data available
========================= ============= ================
 Referenced                Yes           No
------------------------- ------------- ----------------
 Indexed                   Yes           Maybe
------------------------- ------------- ----------------
 Managed                   Yes           Yes
========================= ============= ================

Referenced Datasets
~~~~~~~~~~~~~~~~~~~

The existence of these datasets is know about through the provenance history
of datasets, but the raw data files are not tracked by the Data Cube.

Example:

- Raw Landsat Telemetry

Indexed Datasets
~~~~~~~~~~~~~~~~

Data has been available on disk at some point, with associated metadata
available in a format understood by the Data Cube.

Example:

- USGS Landsat Scenes with prepared ``agdc-metadata.yaml``
- GA Landsat Scenes

Managed Datasets
~~~~~~~~~~~~~~~~

On disk data has been created by/and is managed by the Data Cube. The data has
been copied, compressed, tiled and possibly re-projected into a shape suitable
for analysis, and stored in NetCDF4 files.

Example:

- Tiled GA Landsat Data, ingested into Australian Albers Equal Area
  Projection (EPSG:3577) and stored in 100km tiles in NetCDF4


Provenance
==========

.. digraph:: provenance

    node[fontname="Helvetica"] //, colorscheme=greens3, color=1];

    raw[label="RAW Telemetry", style=dashed]
    ortho[label="Ortho Rectified", style=dashed]
    nbar[style=solid, label="NBAR Scene", styled=solid]
    nbartile[label="NBAR Albers Tile", shape=box, style=filled]

    raw -> ortho -> nbar -> nbartile;
    //rankdir=LR;

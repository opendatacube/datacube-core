.. _introduction:

Overview
========

The Data Cube is a system designed to:

* Catalogue large amounts of Earth Observation data
* Provide a :term:`Python` based :term:`API` for high performance querying and data access
* Give scientists and other users easy ability to perform Exploratory Data Analysis
* Allow scalable continent scale processing of the stored data
* Track the provenance of all the contained data to allow for quality control and updates

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

================= ========== ================= ================================
 Type of dataset   In Index   Data available           Typical data
================= ========== ================= ================================
 Referenced           Yes           No           Historic or provenance record
----------------- ---------- ----------------- --------------------------------
 Indexed              Yes           Maybe             Created externally
----------------- ---------- ----------------- --------------------------------
 Ingested             Yes           Yes         Created within the Data Cube
================= ========== ================= ================================

Referenced Datasets
~~~~~~~~~~~~~~~~~~~

The existence and metadata of these datasets is known but the data itself is not
accessible to the Data Cube. ie. A dataset without a location.

These usually come from the provenance / source information of other datasets.

Example:

- Raw Landsat Telemetry

Indexed Datasets
~~~~~~~~~~~~~~~~

Data is available (has a file location or uri), with associated metadata
available in a format understood by the Data Cube.

Example:

- USGS Landsat Scenes with prepared ``agdc-metadata.yaml``
- GA Landsat Scenes

Ingested Datasets
~~~~~~~~~~~~~~~~~

Data has been created by/and is managed by the Data Cube. The data has typically been
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

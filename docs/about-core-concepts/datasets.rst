Datasets
========
Datasets are a fundamental part of the Open Data Cube. A dataset is *“The smallest aggregation of data independently described, inventoried, and managed.”​* (Definition of “Granule” from NASA EarthData Unified Metadata Model​)

.. admonition:: Examples of Datasets
  :class: important

  - a Landsat Scene​
  - an Albers Equal Area tile portion of a Landsat Scene​


Types of Datasets
=================
The Open Data Cube supports two primary types of dataset, ``indexed`` datasets, and ``ingested`` datasets.

Indexed Datasets
~~~~~~~~~~~~~~~~

An indexed dataset is available via a file location or from an external uri, with associated metadata
available in a format understood by the Data Cube. **The pixel data does not need to be stored in the DataCube.**

Example:

- USGS Landsat Scenes stored in AWS S3, with prepared ``agdc-metadata.yaml``
- GA Landsat Scenes

Ingested Datasets
~~~~~~~~~~~~~~~~~

Data has been created by/and is managed by the Data Cube. The data has typically been
been copied, compressed, tiled and possibly re-projected into a shape suitable
for analysis, and stored in NetCDF4 files.

Example:

- Tiled GA Landsat Data, ingested into Australian Albers Equal Area
  Projection (EPSG:3577) and stored in 100km tiles in NetCDF4

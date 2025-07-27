Datasets
========
Datasets are a fundamental part of the Open Data Cube. A dataset is *“The smallest aggregation of data independently described, inventoried, and managed.”​* (Definition of “Granule” from NASA EarthData Unified Metadata Model​)

.. admonition:: Examples of Datasets
  :class: important

  - a Landsat Scene
  - an Albers Equal Area tile portion of a Landsat Scene


Dataset metadata format
=======================

For a detailed description of the format of a valid dataset document, refer to the
`formal specification`_ in the eo3 github repository.

.. _`formal specification`: https://github.com/opendatacube/eo3/blob/develop/SPECIFICATION.md

Ingested Datasets
=================

Previous releases distinguished between ``indexed`` and ``ingested`` datasets.  In datacube 1.9,
support for the ``ingestion`` workflow has been dropped, and all datasets can be considered
``indexed``.

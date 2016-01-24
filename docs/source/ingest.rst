Ingestion Configuration
=======================

Dataset Preparation
-------------------
:ref:`dataset-metadata-doc` is required to accompany the dataset for it to be recognised by Data Cube. It defines critical metadata of the dataset such as:
    - measurements
    - platform and sensor names
    - geospatial extents and projection
    - acquisition time

For more information see :ref:`dataset-metadata-doc`

Storage Types
-------------
:ref:`storage-type-doc` is a document that defines the way an input dataset is stored inside the Data Cube.

It controls things like:
    - which measurements are stored
    - what projection the data is stored in
    - what resolution the data is stored in
    - how data is tiled
    - where the data is stored

Multiple storage type definitions can be used to ingest datasets into different projections, resolutions, etc.

For more information see :ref:`storage-type-doc`

:ref:`datacube-config-tool` can be used to add storage type::

    datacube-config storage add docs/config_samples/ga_landsat_7/ls7_nbar_mapping.yaml

Ingestion
---------
:ref:`datacube-ingest-tool` can be used to ingest a dataset::

    datacube-ingest -v packages/nbar/LS8_OLITIRS_TNBAR_P54_GALPGS01-002_112_079_20140126

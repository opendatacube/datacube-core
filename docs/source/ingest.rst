Ingestion Configuration
=======================

Dataset Preparation
-------------------
:ref:`dataset-metadata-doc` is required to accompany the dataset for it to be recognised by Datacube. It defines critical metadata of the dataset such as:
    - measurements
    - platform and sensor names
    - geospatial extents and projection
    - acquisition time

For more information see :ref:`dataset-metadata-doc`

Storage Mapping
---------------
:ref:`storage-mapping-doc` is a document that defines the way an input dataset is stored inside the Datacube.

It controls things like:
    - which measurements are stored
    - what projection the data is stored in
    - what resolution the data is stored in
    - how data is tiled
    - where the data is stored

At least one Storage Mapping is required to ingest a dataset. Multiple mappings can be used to ingest datasets into different projections, resolutions, etc.

For more information see :ref:`storage-mapping-doc`

:ref:`datacube-config-tool` can be used to add storage mapping::

    datacube-config mappings add docs/config_samples/ls8-nbar-terrain.yaml

Ingestion
---------
:ref:`datacube-ingest-tool` can be used to ingest a dataset::

    datacube-ingest -v packages/nbar/LS8_OLITIRS_TNBAR_P54_GALPGS01-002_112_079_20140126

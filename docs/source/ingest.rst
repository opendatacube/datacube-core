Ingestion Configuration
=======================

Dataset Preparation
-------------------
:ref:`dataset-metadata-doc` is required to accompany the dataset for it to be recognised by Data Cube. It defines critical metadata of the dataset such as:

    - measurements
    - platform and sensor names
    - geospatial extents and projection
    - acquisition time

.. note::

    Some metadata requires cleanup before they are ready to be loaded.

For more information see :ref:`dataset-metadata-doc`.

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

For more information see :ref:`storage-type-doc`.

:ref:`datacube-config-tool` can be used to add storage types. Sample configs are in ``docs/config_samples``.
::

    datacube-config storage add docs/config_samples/ga_landsat_7/ls7_nbar_mapping.yaml docs/config_samples/ga_landsat_7/ls7_pq_mapping.yaml

.. note::

    You should refer to ``platform`` in your metadata file to determine which kind of mapping to configure. For example, ``LANDSAT_5`` means you should configure for the Landsat 5 configuration.

`Configuration samples <https://github.com/data-cube/agdc-v2/tree/develop/docs/config_samples>`_ are available as part of the open source Github repoistory.

Ingestion
---------
:ref:`datacube-ingest-tool` can be used to ingest prepared datasets::

    datacube-ingest -v ingest packages/nbar/LS8_OLITIRS_TNBAR_P54_GALPGS01-002_112_079_20140126 packages/pq/LS8_OLITIRS_PQ_P55_GAPQ01-002_112_079_20140126

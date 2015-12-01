Ingestion Configuration
=======================

Storage Type and Mapping
------------------------
TODO: describe what those are


:ref:`datacube-config-tool` can be used to add storage type::

    datacube-config storage add docs/config_samples/25m_bands_storage_type.yaml

and dataset to storage mapping::

    datacube-config mappings add docs/config_samples/ls8-nbar-terrain.yaml

Ingestion
---------
:ref:`datacube-ingest-tool` can be used to ingest a dataset::

    datacube-ingest -v packages/nbar/LS8_OLITIRS_TNBAR_P54_GALPGS01-002_112_079_20140126

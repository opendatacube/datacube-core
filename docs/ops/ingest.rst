.. _ingestion:

Ingesting Data
**************

Congratulations, you're ready to ingest some data. If you've made it this far
you should already have some data :ref:`indexed <indexing>`, and want to
tile it up into a faster storage format or projection system.

.. _ingest-config:

Ingestion Configuration
=======================

An Ingestion Configuration file defines a mapping from the way one set of
Datasets is stored, into a new storage scheme for the data. This will be
recorded in the index as a new :ref:`product`, and the data will be
manipulated and written out to disk in the new format.

An Ingestion Config is written in YAML and contains the following:

    - which measurements are stored
    - what projection the data is stored in
    - what resolution the data is stored in
    - how data is tiled
    - where the data is stored
    - how the data should be resampled and compressed


Multiple ingestion configurations can be kept around to ingest datasets into
different projections, resolutions, etc.

For more information see :ref:`ingestion-config`.

Ingest Some Data
================

Use the :ref:`datacube-tool` to ingest your data, specifying the desired
configuration file.
::

    datacube ingest -c {configuration_file}


`Configuration samples <https://github.com/opendatacube/datacube-core/tree/develop/docs/config_samples>`_ are available as part of the open source Github repository.

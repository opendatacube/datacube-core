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

Ingestion Config
================
An ingestion config is a document which defines the way data should be prepared
for high performance access. This can include  slicing the data into regular
chunks, reprojecting into to the desired projection and compressing the data.


An Ingestion Config is written in :term:`YAML` and contains the following:

   - Source Product name - ``source_type``
   - Output Product name - ``output_type``
   - Output file location and file name template
   - Global metadata attributes
   - Storage format, specifying:

        - Driver
        - :term:`CRS`
        - Resolution
        - Tile size
        - Tile Origin

   - Details about **measurements**:

        - Output measurement name
        - Source measurement name
        - Resampling method
        - Data type
        - Compression options


output_type
    Name of the output Product. It's used as a human-readable identifer. Must
    be unique and consist of alphanumeric characters and/or underscores.

description (optional)
    A human-readable description of the output Product.

location
    Directory to write the output storage units.

file_path_template
    File path pattern defining the name of the storage unit files. **TODO:** list available substitutions

global_attributes
    File level (NetCDF) attributes

storage
    driver
        Storage type format. Currently only 'NetCDF CF' is supported

    crs
        Definition of the output coordinate reference system for the data to be
        stored in. May be specified as an EPSG code or WKT.

    tile_size
        Size of the tiles for the data to be stored in specified in projection units. Use ``latitude`` and ``longitude``
        if the projection is geographic, otherwise use ``x`` and ``y``

    origin
        Coordinates of the bottom-left or top-left corner of the (0,0) tile specified in projection units. If
        coordinates are for top-left corner, ensure that the ``latitude`` or ``y`` dimension of ``tile_size`` is
        negative so tile indexes count downward. Use ``latitude`` and ``longitude`` if the projection is geographic,
        otherwise use ``x`` and ``y``

    resolution
        Resolution for the data to be stored in specified in projection units.
        Negative values flip the axis. Use ``latitude`` and ``longitude`` if the projection is geographic,
        otherwise use ``x`` and ``y``

    chunking
        Size of the internal NetCDF chunks in 'pixels'.

    dimension_order
        Order of the dimensions for the data to be stored in. Use ``latitude`` and ``longitude`` if the projection
        is geographic, otherwise use ``x`` and ``y``. **TODO:** currently ignored. Is it really needed?


measurements
    Mapping of the input measurement names as specified in the :ref:`dataset-metadata-doc`
    to the per-measurement ingestion parameters

    dtype
        Data type to store the data in. One of (u)int(8,16,32,64), float32,
        float64

    resampling_method
        Resampling method. One of  nearest, cubic, bilinear, cubic_spline,
        lanczos, average.

    name
        Name of the NetCDF variable to store the data in.

    nodata (optional)
        No data value

Ingest Some Data
================

A command line tool is used to ingest data

.. click:: datacube.scripts.ingest:ingest_cmd
   :prog: datacube ingest



`Configuration samples <https://github.com/opendatacube/datacube-core/tree/develop/docs/config_samples>`_ are available as part of the open source Github repository.

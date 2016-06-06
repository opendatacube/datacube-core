Configuration Files
===================

.. _product-doc:

Product
-------

.. literalinclude:: ../config_samples/dataset_types/ls5_scenes.yaml
   :start-after: # Start LS5_SCENE
   :end-before: # End LS5_SCENE


.. _dataset-metadata-doc:

Dataset Metadata
----------------
Dataset Metadata is a document that defines critical metadata of the dataset such as:

    - measurements
    - platform and sensor names
    - geospatial extents and projection
    - acquisition time

.. literalinclude:: example-dataset.yaml
   :lines: 1-61,93-209,357-365


.. _ingestion-config:

Ingestion Config
----------------
An ingestion config is a document which defines the way data should be prepared
 for high performance access. This can include  slicing the data into regular
 chunks, reprojecting into to the desired projection and compressing the data.


An Ingestion Config is written in YAML and contains the following:

   - Source Product name - ``source_type``
   - Output Product name - ``output_type``
   - Output file location and file name template
   - Global metadata attributes
   - Outer boundary of data to ingest
   - Storage format, specifying:
        - Driver
        - CRS
        - Resolution
        - Tile size
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
    File path pattern defining the name of the storage unit files.
        - TODO: list available substitutions

ingestion_bounds
    Outer boundary of the region to ingest. Specified as ``left``,
    ``bottom``, ``right``, ``top`` in Storage CRS coordinates. They will be
    expanded out to the nearest tile boundary.

    Define region of interest for the subset of the data to be ingested.
    Currently only bounding box specified in projection units is supported

global_attributes
    TODO: list useful attributes

storage
    driver
        Storage type format. Currently only 'NetCDF CF' is supported

    crs
        Definition of the output coordinate reference system for the data to be
        stored in. May be specified as an EPSG code or WKT.

    tile_size
        Size of the tiles for the data to be stored in specified in projection units.
            - Use ``latitude`` and ``longitude`` if the projection is geographic,
              otherwise use ``x`` and ``y``

    aggregation_period
        Storage unit aggregation period. One of 'month', 'year'

    resolution
        Resolution for the data to be stored in specified in projection units.
        Negative values flip the axis.

            - Use ``latitude`` and ``longitude`` if the projection is geographic,
              otherwise use ``x`` and ``y``

    chunking
        Size of the internal NetCDF chunks in 'pixels'.

    dimension_order
        Order of the dimensions for the data to be stored in.
            - Use ``latitude`` and ``longitude`` if the projection is geographic,
              otherwise use ``x`` and ``y``
            - TODO: currently ignored. Is it really needed?


measurements
    Mapping of the input measurement names as specified in `Dataset Metadata`_
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

.. _runtime-config-doc:

Runtime Config
--------------
Runtime Config document specifies various runtime configuration options such as:
 database connection parameters and location mappings

.. code-block:: text

    [datacube]
    db_hostname: 130.56.244.227
    db_database: democube
    db_username: cube_user


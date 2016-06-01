Configuration Files
===================

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

.. _dataset-type-doc:

Dataset Type
------------

.. literalinclude:: ../config_samples/dataset_types/ls5_scenes.yaml
   :start-after: # Start LS5_SCENE
   :end-before: # End LS5_SCENE

.. _storage-type-doc:

Storage Type
------------
A Storage Type is a document that defines the way an input dataset is stored inside the Data Cube.

It controls things like:

    - which measurements are stored
    - what projection the data is stored in
    - what resolution the data is stored in
    - how data is tiled
    - where the data is stored

.. code-block:: yaml


    name: ls5_nbar
    description: LS5 NBAR 25 metre, 1 degree tile

    # Any datasets matching these metadata properties.
    match:
        metadata:
            platform:
                code: LANDSAT_5
            instrument:
                name: TM
            product_type: NBAR

    location_name: eotiles

    file_path_template: '{platform[code]}_{instrument[name]}_{tile_index[0]}_{tile_index[1]}_NBAR_{start_time}.nc'

    global_attributes:
        title: Experimental Data files From the Australian Geoscience Data Cube - DO NOT USE
        summary: These files are experimental, short lived, and the format will change.
        source: This data is a reprojection and retile of Landsat surface reflectance scene data available from /g/data/rs0/scenes/
        product_version: '0.0.0'
        license: Creative Commons Attribution 4.0 International CC BY 4.0

    storage:
        driver: NetCDF CF
        crs: |
            GEOGCS["WGS 84",
                DATUM["WGS_1984",
                    SPHEROID["WGS 84",6378137,298.257223563,
                        AUTHORITY["EPSG","7030"]],
                    AUTHORITY["EPSG","6326"]],
                PRIMEM["Greenwich",0,
                    AUTHORITY["EPSG","8901"]],
                UNIT["degree",0.0174532925199433,
                    AUTHORITY["EPSG","9122"]],
                AUTHORITY["EPSG","4326"]]
        tile_size:
            longitude: 1.0
            latitude:  1.0
        resolution:
            longitude: 0.00025
            latitude: -0.00025
        chunking:
            longitude: 500
            latitude:  500
            time: 1
        dimension_order: ['time', 'latitude', 'longitude']
        aggregation_period: year

    roi:
        longitude: [110, 120]
        latitude: [10, 20]

    measurements:
        '10':
            dtype: int16
            nodata: -999
            resampling_method: cubic
            varname: band_10
        '20':
            dtype: int16
            nodata: -999
            resampling_method: cubic
            varname: band_20


name
    Name of the storage type. It's used as a human-readable identifer. Must be unique and consist of
    alphanumeric characters and/or underscores.

description (optional)
    A human-readable description of the storage type.

location_name
    Name of the location where the storage units go. See `Runtime Config`_.

file_path_template
    File path pattern defining the name of the storage unit files.
        - TODO: list available substitutions

match/metadata
    TODO

global_attributes
    TODO: list useful attributes

storage
    driver
        Storage type format. Currently only 'NetCDF CF' is supported

    crs
        WKT defining the coordinate reference system for the data to be stored in.
            - TODO: support EPSG codes?

    tile_size
        Size of the tiles for the data to be stored in specified in projection units.
            - Use 'latitude' and 'longitude' if the projection is geographic, else use 'x' and 'y'

    aggregation_period
        Storage unit aggregation period. One of 'month', 'year'

    resolution
        Resolution for the data to be stored in specified in projection units.
        Negative values flip the axis.

            - Use 'latitude' and 'longitude' if the projection is geographic, else use 'x' and 'y'

    chunking
        Size of the internal NetCDF chunks in 'pixels'.

    dimension_order
        Order of the dimensions for the data to be stored in.
            - Use 'latitude' and 'longitude' if the projection is geographic, else use 'x' and 'y'
            - TODO: currently ignored. Is it really needed?

roi (optional)
    Define region of interest for the subset of the data to be ingested
    Currently only bounding box specified in projection units is supported

measurements
    Mapping of the input measurement names as specified in `Dataset Metadata`_ to the per-measurement ingestion parameters

    dtype
        Data type to store the data in. One of (u)int(8,16,32,64), float32, float64

    resampling_method
        Resampling method. One of  nearest, cubic, bilinear, cubic_spline, lanczos, average.

    varname
        Name of the NetCDF variable to store the data in.

    nodata (optional)
        No data value

.. _runtime-config-doc:

Runtime Config
--------------
Runtime Config document specifies various runtime configuration options such as: database connection parameters and location mappings

.. code-block:: text

    [Data Cube]
    db_hostname: 130.56.244.227
    db_database: democube
    db_username: cube_user

    [locations]
    eotiles: file:///short/public/democube/
    v1tiles: file:///g/data/rs0/tiles/EPSG4326_1deg_0.00025pixel/

locations
    Mapping of location names to URI prefixes. How to reach each location from the current machine.

    **Note:** You may want to rename ``eotiles`` path to a location you can modify. The database will create storage there.

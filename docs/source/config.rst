Configuration Files
===================

Dataset Metadata
----------------
.. code-block:: yaml

    id: 4678bf44-82b5-11e5-9264-a0000100fe80
    ga_label: LS5_TM_NBAR_P54_GANBAR01-002_090_085_19900403
    ga_level: P54
    product_type: NBAR
    creation_dt: 2015-03-22 01:37:41
    checksum_path: package.sha1
    platform:
        code: LANDSAT_5
    instrument:
        name: TM
    format:
        name: GeoTiff
    acquisition:
        aos: 1990-04-03 23:05:30
        los: 1990-04-03 23:13:06
        groundstation:
            code: ASA
            label: Alice Springs
            eods_domain_code: '002'
    extent:
        coord:
            ul:
                lat: -35.04885921004133
                lon: 148.08553520367545
            ur:
                lat: -34.996165736608994
                lon: 150.7361052128533
            ll:
                lat: -37.014186845449004
                lon: 148.11284610299305
            lr:
                lat: -36.95758002539804
                lon: 150.829848574551
        from_dt: 1990-04-03 23:10:30
        center_dt: 1990-04-03 23:10:42
        to_dt: 1990-04-03 23:10:54
    grid_spatial:
        projection:
            geo_ref_points:
                ul:
                    x: 599000.0
                    y: 6121000.0
                ur:
                    x: 841025.0
                    y: 6121000.0
                ll:
                    x: 599000.0
                    y: 5902975.0
                lr:
                    x: 841025.0
                    y: 5902975.0
            datum: GDA94
            ellipsoid: GRS80
            zone: -55
            unit: metre
    image:
        satellite_ref_point_start:
            x: 90
            y: 85
        satellite_ref_point_end:
            x: 90
            y: 85
        bands:
            '10':
                path: product/scene01/LS5_TM_NBAR_P54_GANBAR01-002_090_085_19900403_B10.tif
    lineage:
        machine: {}
        source_datasets: {}


Storage Mapping
---------------
.. code-block:: yaml

    name: LS5 PQ

    # Any datasets matching these metadata properties.
    match:
        metadata:
            platform:
                code: LANDSAT_5
            instrument:
                name: TM
            product_type: PQ

    storage:
        - name: 25m_bands
          location_name: eotiles
          file_path_template: '{platform[code]}_{instrument[name]}_PQ_{lons[0]}_{lats[0]}_{extent[center_dt]:%Y-%m-%dT%H-%M-%S.%f}.nc'
          global_attributes:
            title: Experimental Data files From the Australian Geoscience Data Cube - DO NOT USE
            summary: These files are experimental, short lived, and the format will change.
            source: This data is a reprojection and retile of Landsat surface reflectance scene data available from /g/data/rs0/scenes/
            product_version: '0.0.0'
            license: Creative Commons Attribution 4.0 International CC BY 4.0
          measurements:
            '1111111111111100':
                dtype: int16
                resampling_method: nearest
                varname: band_pixelquality

name
    Name of the storage mapping. Must be unique.

match/metadata
    TODO

storage
    name
        Name of the `Storage Type`_ to use.

    location_name
        Name of the location where the storage units go.

    file_path_template
        File path pattern defining the name of the storage unit files.
            - TODO: list available substitutions

    measurements
        Mapping of the input measurement names as specified in `Dataset Metadata`_ to the per-measurement ingestion parameters

        dtype
            Data type to store the data in.

        resampling_method
            Resampling method. One of  nearest, cubic, bilinear, cubic_spline, lanczos, average.

        varname
            Name of the NetCDF variable to store the data in.


Storage Type
------------
.. code-block:: yaml

    name: 25m_bands
    description: 25 metre, 1 degree EO NetCDF storage unit.
    driver: NetCDF CF

    projection:
        spatial_ref: |
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
            x: 1.0
            y: 1.0
    resolution:
            x: 0.00025
            y: -0.00025
    chunking:
        x: 500
        y: 500
        t: 1
    dimension_order: ['t', 'y', 'x']

name
    Name of the storage type. Must be unique.

driver
    Storage type format. Currently only NetCDF CF is supported

projection/spatial_ref
    WKT defining the spatial reference for the data to be stored in.
        - TODO: should it just be called 'spatial_reference'?

tile_size
    Size of the tiles for the data to be stored in specified in projection units.
        - TODO: Use 'latitude' and 'longitude' if the projection is geographic, else use 'x' and 'y'

resolution
    Resolution for the data to be stored in specified in projection units.
    Negative values flip the axis.
        - TODO: Use 'latitude' and 'longitude' if the projection is geographic, else use 'x' and 'y'
        - TODO: 't' should be 'time'

chunking
    Size of the internal NetCDF chunks in 'pixels'.

dimension_order
    Order of the dimensions for the data to be stored in.
        - TODO: currently ignored. Is it really needed?
        - TODO: Use 'latitude' and 'longitude' if the projection is geographic, else use 'x' and 'y'

Runtime Config
--------------
.. code-block:: text

    [datacube]
    db_hostname: 130.56.244.227
    db_database: democube
    db_username: cube_user

    [locations]
    eotiles: file:///short/public/democube/
    v1tiles: file:///g/data/rs0/tiles/EPSG4326_1deg_0.00025pixel/

locations
    Mapping of location names to URI's

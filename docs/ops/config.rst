Configuration Files
*******************

See also :ref:`create-configuration-file` for the datacube config file.

.. _product-doc:

Product Definition
==================

Product description document defines some of the metadata common to all the datasets belonging to the products.
It also describes the measurements that product has and some of the properties of the measurements.

.. literalinclude:: ../config_samples/dataset_types/dsm1sv10.yaml
   :language: yaml

name
    Product name

description
    Product description

metadata_type
    Name of the `Metadata Type Definition`_

metadata
    Dictionary containing bits of metadata common to all the datasets in the product.
    It is used during indexing (if ``--auto-match`` options is used) to match datasets to thier products.

storage (optional)
    Describes some of common storage attributes of all the datasets. While optional defining this will make
    product data easier to access and use.

    crs
        Coordinate reference system common to all the datasets in the product. 'EPSG:<code>' or WKT string.

    resolution
        Resolution of the data of all the datasets in the product specified in projection units.
        Use ``latitude``, ``longitude`` if the projection is geographic and ``x``, ``y`` otherwise

measurements
    List of measurements in this product

    name
         Name of the measurement

    units
         Units of the measurement

    dtype
         Data type. One of ``(u)int(8,16,32,64), float32, float64``

    nodata
         No data value

    spectral_definition (optional)
         Spectral response of the reflectance measurement.

         .. code-block:: yaml

             spectral_definition:
                  wavelength: [410, 411, 412]
                  response: [0.0261, 0.029, 0.0318]

    flags_definition (optional)
        Bit flag meanings of the bitset 'measurement'

        .. code-block:: yaml

            flags_definition:
                platform:
                  bits: [0,1,2,3]
                  description: Platform name
                  values:
                    0: terra
                    1: aqua_terra
                    2: aqua
                contiguous:
                  bits: 8
                  description: All bands for this pixel contain non-null values
                  values: {0: false, 1: true}

.. _dataset-metadata-doc:

Dataset Metadata Document
=========================
Dataset document defines critical metadata of the dataset such as:

    - measurements
    - platform and sensor names
    - geospatial extents and projection
    - acquisition time

.. code-block:: yaml

    id: a066a2ab-42f7-4e72-bc6d-a47a558b8172
    creation_dt: '2016-05-04T09:06:54'
    product_type: DEM
    platform: {code: SRTM}
    instrument: {name: SIR}
    format: {name: ENVI}
    extent:
      coord:
        ll: {lat: -44.000138890272005, lon: 112.99986111}
        lr: {lat: -44.000138890272005, lon: 153.99986111032797}
        ul: {lat: -10.00013889, lon: 112.99986111}
        ur: {lat: -10.00013889, lon: 153.99986111032797}
      from_dt: '2000-02-11T17:43:00'
      center_dt: '2000-02-21T11:54:00'
      to_dt: '2000-02-22T23:23:00'
    grid_spatial:
      projection:
        geo_ref_points:
          ll: {x: 112.99986111, y: -44.000138890272005}
          lr: {x: 153.999861110328, y: -44.000138890272005}
          ul: {x: 112.99986111, y: -10.00013889}
          ur: {x: 153.999861110328, y: -10.00013889}
        spatial_reference: GEOGCS["GCS_WGS_1984",DATUM["WGS_1984",SPHEROID["WGS_84",6378137.0,298.257223563]],PRIMEM["Greenwich",0.0],UNIT["degree",0.0174532925199433],AUTHORITY["EPSG","4326"]]
    image:
      bands:
        elevation: {path: dsm1sv1_0_Clean.img}
    lineage:
      source_datasets: {}

id
    UUID of the dataset

creation_dt
    Creation datetime

product_type, platform/code, instrument/name
    Metadata fields supported by default

format
    Format the data is stored in. For NetCDF and HDF formats it **must** be 'NetCDF' and 'HDF'

extent
    Spatio-tempral extents of the data. Used for search in the database.

grid_spatial/projection
    spatial_reference
        Coordinate reference system the data is stored in. 'EPSG:<code>' or WKT string.

    geo_ref_points
        Spatial extents of the data in the CRS of the data.

    valid_data (optional)
        `GeoJSON Geometry Object <http://geojson.org/geojson-spec.html#geometry-objects>`_ for the 'data-full'
        (non no-data) region of the data. Coordinates are assumed to be in the CRS of the data.
        Used to avoid loading useless parts of the dataset into memory.
        Only needs to be roughly correct. Prefer simpler geometry over accuracy.

image/bands
    Dictionary of band names to band definitions

    path
        Path to the file containing band data. Can be absolute of relative to the folder containing this document.

    layer (optional)
        Variable name if format is 'NetCDF' or 'HDF'. Band number otherwise. Default is 1.

lineage
    Dataset lineage metadata

    source_datasets
        Dictionary of source classifier to dataset documents like this one (yay recursion!).

        .. code-block:: yaml

            source_datasets:
                level1:
                    id: b7d01e8c-1cd2-11e6-b546-a0000100fe80
                    product_type: level1
                    creation_dt: 2016-05-18 08:09:34
                    platform: { code: LANDSAT_5 }
                    instrument: { name: TM }
                    format: { name: GeoTIFF }
                    ...

    algorithm (optional)
        Algorithm used to generate this dataset.

        .. code-block:: yaml

            algorithm:
                name: brdf
                version: '2.0'
                doi: http://dx.doi.org/10.1109/JSTARS.2010.2042281
                parameters:
                    aerosol: 0.078565

    machine (optional)
        Machine and software used to generate this dataset.

        .. code-block:: yaml

                machine:
                    hostname: r2200
                    uname: 'Linux r2200 2.6.32-573.22.1.el6.x86_64 #1 SMP Wed Mar 23 03:35:39 UTC 2016 x86_64'
                    runtime_id: d052fcb0-1ccb-11e6-b546-a0000100fe80
                    software_versions:
                        eodatasets:
                            repo_url: https://github.com/GeoscienceAustralia/eo-datasets.git
                            version: '0.4'

    ancillary (optional)
        Additional data used to generate this dataset.

        .. code-block:: yaml

                ancillary:
                    ephemeris:
                        name: L52011318DEFEPH.S00
                        uri: /g/data/v10/eoancillarydata/sensor-specific/LANDSAT5/DefinitiveEphemeris/LS5_YEAR/2011/L52011318DEFEPH.S00
                        access_dt: 2016-05-18 18:30:03
                        modification_dt: 2011-11-15 02:10:26
                        checksum_sha1: f66265314fc12e005deb356b69721a7031a71374

.. _ingestion-config:

Metadata Type Definition
========================
A Metadata Type defines which fields should be searchable in your product or dataset metadata.

A metadata type is added by default called 'eo' with platform/instrument/lat/lon/time fields.

You would create a new metadata type if you want custom fields to be searchable for your products, or
if you want to structure your metadata documents differently.

You can see the default metadata type in the repository at ``datacube/index/default-metadata-types.yaml``.

Or more elaborate examples (with fewer comments) in GA's configuration
repository: https://github.com/GeoscienceAustralia/datacube-ingestion

Ingestion Config
================
An ingestion config is a document which defines the way data should be prepared
for high performance access. This can include  slicing the data into regular
chunks, reprojecting into to the desired projection and compressing the data.


An Ingestion Config is written in YAML and contains the following:

   - Source Product name - ``source_type``
   - Output Product name - ``output_type``
   - Output file location and file name template
   - Global metadata attributes
   - Storage format, specifying:

        - Driver
        - CRS
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
    Mapping of the input measurement names as specified in the `Dataset Metadata Document`_
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

.. _ops-runtime-config-doc:

Runtime Config
==============

The runtime config specifies configuration options for the current user, such as
available datacube instances and which to use by default.

This is loaded from the following locations in order, if they exist, with properties from latter files
overriding those in earlier ones:

 * ``/etc/datacube.conf``
 * ``$DATACUBE_CONFIG_PATH``
 * ``~/.datacube.conf``
 * ``datacube.conf``

Example:

.. code-block:: text

    [user]
    # This should correspond to a section name in your config.
    default_environment: dev

    ## Development environment ##

    [dev]
    # These fields are all the defaults, so they could be omitted, but are here for reference

    db_database: datacube

    # A blank host will use a local socket. Specify a hostname (such as localhost) to use TCP.
    db_hostname:

    # Credentials are optional: you might have other Postgres authentication configured.
    # The default username is the current user id
    # db_username:
    # A blank password will fall back to default postgres driver authentication, such as reading your ~/.pgpass file.
    # db_password:

    ## Staging environment ##

    [staging]
    db_hostname: staging.dea.ga.gov.au

Note that the staging environment only specifies the hostname, all other fields will use default values (dbname
datacube, current username, password loaded from ``~/.pgpass``)

When using the datacube, it will use your default environment unless you specify one explicitly

eg.

.. code-block:: python

    with Datacube(env='staging') as dc:
        ...

or for cli commmands ``-E <name>``:

    datacube -E staging system check


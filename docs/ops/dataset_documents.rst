.. _dataset-metadata-doc:

Dataset Documents
*****************

Dataset metadata documents define critical metadata about a dataset including:

   - available data measurements
   - platform and sensor names
   - geospatial extents and projection
   - acquisition time
   - provenance information

Traditionally :ref:`dataset-metadata-doc-eo` format was used to capture
information about individual datasets. However there are a number of issues with
this format, so it is now deprecated and we recommend everyone move to using
:ref:`dataset-metadata-doc-eo3`.

The format is determined by ODC using the ``$schema`` field in the document.
Include an eo3 ``$schema`` for eo3 documents. If no schema field exists, it
is treated as the older ``eo`` format.

.. _dataset-metadata-doc-eo3:


EO3 Format
==========

EO3 is an intermediate format before we move to something more standard like `STAC <https://stacspec.org/>`_. Primary driver for the development

#. Avoid duplication of spatial information, by storing only native projection information
#. Capture geo-registration information per band, not per entire dataset
#. Capture image size/resolution per band
#. Lightweight lineage representation


.. code-block:: yaml

   # UUID of the dataset
   id: f884df9b-4458-47fd-a9d2-1a52a2db8a1a
   $schema: 'https://schemas.opendatacube.org/dataset'

   # Product name
   product:
     name: landsat8_example_product

   # Native CRS, assumed to be the same across all bands
   crs: "epsg:32660"

   # Optional GeoJSON object in the units of native CRS.
   # Defines a polygon such that, all valid pixels across all bands
   # are inside this polygon.
   geometry:
     type: Polygon
     coordinates: [[..]]

   # Mapping name:str -> { shape:     Tuple[ny: int, nx: int]
   #                       transform: Tuple[float x 9]}
   # Captures image size, and geo-registration
   grids:
       default:  # "default" grid must be present
          shape: [7811, 7691]
          transform: [30, 0, 618285, 0, -30, -1642485, 0, 0, 1]
       pan:  # Landsat Panchromatic band is higher res image than other bands
          shape: [15621, 15381]
          transform: [15, 0, 618292.5, 0, -15, -1642492.5, 0, 0, 1]

   # Per band storage information and references into `grids`
   # Bands using "default" grid should not need to reference it
   measurements:
      pan:               # Band using non-default "pan" grid
        grid: "pan"      # should match the name used in `grids` mapping above
        path: "pan.tif"
      red:               # Band using "default" grid should omit `grid` key
        path: red.tif    # Path relative to the dataset location
      blue:
        path: blue.tif
      multiband_example:
        path: multi_band.tif
        band: 2          # int: 1-based index into multi-band file
      netcdf_example:    # just example, mixing TIFF and netcdf in one product is not recommended
        path: some.nc
        layer: some_var  # str: netcdf variable to read

   # Dataset properties, prefer STAC standard names here
   # Timestamp is the only compulsory field here
   properties:
     eo:platform: landsat-8
     eo:instrument: OLI_TIRS

     # If it's a single time instance use datetime
     datetime: 2020-01-01T07:02:54.188Z  # Use UTC

     # When recording time range use dtr:{start,end}_datetime
     dtr:start_datetime: 2020-01-01T07:02:02.233Z
     dtr:end_datetime:   2020-01-01T07:03:04.397Z

     # ODC specific "extensions"
     odc:processing_datetime: 2020-02-02T08:10:00.000Z

     odc:file_format: GeoTIFF
     odc:region_code: "074071"   # provider specific unique identified for the same location
                                 # for Landsat '{:03d}{:03d}'.format(path, row)

     dea:dataset_maturity: final # one of: final| interim| nrt (near real time)
     odc:product_family: ard     # can be useful for larger installations

   # Lineage only references UUIDs of direct source datasets
   # Mapping name:str -> [UUID]
   lineage: {}  # set to empty object if no lineage is defined


Elements ``shape`` and ``transform`` can be obtained from the output of ``rio
info <image-file>``. ``shape`` is basically ``height, width`` tuple and
``transform`` captures a linear mapping from pixel space to projected space
encoded in a row-major order:

A command-line tool to validate eo3 documents called ``eo3-validate`` is available
in the `eodatasets3 library <https://github.com/GeoscienceAustralia/eo-datasets>`_,
as well as optional tools to write these files more easily.


.. code-block::

   # transform [a0, a1, a2, a3, a4, a5, 0, 0, 1]

   [X]   [a0, a1, a2] [ Pixel]
   [Y] = [a3, a4, a5] [ Line ]
   [1]   [ 0,  0,  1] [  1   ]



.. _dataset-metadata-doc-eo:

EO (deprecated)
===============

Majority of prepare scripts still generate this format, so this section is
maintained for historical context.


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
    Spatio-temporal extents of the data in EPSG:4326 (lat/lon) coordinates. Used for search in the database.
    Note: Take care when reprojecting the geo_ref_points bounding box to the new coordinate system. The extent
    should be the bounding box of the data in EPSG:4326. (Don't just re-project the four points, its likely wrong)

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

Reasons for deprecation
~~~~~~~~~~~~~~~~~~~~~~~

#. Duplication of spatial information

   Extent is stored in native projection ``grid_spatial->projection->geo_ref_points->{ll,lr,ul,ur}->{x,y}``, and then again in
   lon/lat: ``extent->coord->{ll,lr,ul,ur}->{lat,lon}``

#. Extent in lon/lat uses 4 points to encode a bounding box

   This format strongly suggests `incorrect implementation
   <https://github.com/opendatacube/datacube-core/issues/537>`_ of simply
   projecting four image corners into lon/lat in the prepare script.

#. Costly lineage representation

   To record lineage one has to recursively include entire dataset document for
   every input dataset. This gets expensive for summary products with thousands
   of input datasets.

#. Format does not capture per band resolution/image size

.. _metadata-type-definition:

Metadata Type Definition
========================
A Metadata Type defines which fields should be searchable in your product or dataset metadata.

A metadata type is added by default called ``eo`` with *platform/instrument/lat/lon/time* fields.

You would create a new metadata type if you want custom fields to be searchable for your products, or
if you want to structure your metadata documents differently.

You can see the default metadata type in the repository at ``datacube/index/default-metadata-types.yaml``.

Or more elaborate examples (with fewer comments) in GA's configuration
repository: https://github.com/GeoscienceAustralia/datacube-ingestion

.. _dataset-metadata-doc:

Dataset Documents
*****************

Dataset metadata documents define critical metadata about a dataset including:

   - available data measurements
   - platform and sensor names
   - geospatial extents and projection
   - acquisition time
   - provenance information

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


Metadata Type Definition
========================
A Metadata Type defines which fields should be searchable in your product or dataset metadata.

A metadata type is added by default called ``eo`` with *platform/instrument/lat/lon/time* fields.

You would create a new metadata type if you want custom fields to be searchable for your products, or
if you want to structure your metadata documents differently.

You can see the default metadata type in the repository at ``datacube/index/default-metadata-types.yaml``.

Or more elaborate examples (with fewer comments) in GA's configuration
repository: https://github.com/GeoscienceAustralia/datacube-ingestion



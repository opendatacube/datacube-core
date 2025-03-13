Dataset Documents
*****************

Dataset metadata documents define critical metadata about a dataset including:

   - available data measurements
   - platform and sensor names
   - geospatial extents and projection
   - acquisition time
   - provenance information

Traditionally the :ref:`dataset-metadata-doc-eo` format was used to capture
information about individual datasets. However there are a number of issues with
this format, so it is now deprecated and we recommend everyone move to using
the :ref:`dataset-metadata-doc-eo3`.  The legacy eo format is not supported by
the ``postgis`` index driver.  Support for the eo format will be dropped in
Datacube 2.0

The format is determined by the Open Data Cube using the ``$schema`` field in the document.
Include an eo3 ``$schema`` for eo3 documents. If no schema field exists, it
is treated as the older ``eo`` format.

.. _dataset-metadata-doc-eo3:


EO3 Format
==========

EO3 is the current internal format for the Open Data Cube. Design goals included:

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
   # Defines a polygon such that all valid pixels across all bands
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
   # Bands using the "default" grid should not need to reference it
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

   # optional dataset location (useful for public datasets)
   location: https://landsatonaws.com/L8/099/072/LC08_L1GT_099072_20200523_20200523_01_RT/metadata.yaml

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
``transform`` capturing a linear mapping from pixel space to projected space
encoded in a row-major order.

For a full description of the eo3 dataset metadata format, refer to the
eo3 `formal specification`_.


A command-line tool to validate eo3 documents called ``eo3-validate`` is available
in the `eodatasets3 library <https://github.com/opendatacube/eo-datasets>`_,
as well as optional tools to write these files more easily.

A command-line tool also exists for dynamically converting STAC items to EO3 datasets
at indexing time in the `odc-apps-dc-tools`_ package.

.. code-block::

   # transform [a0, a1, a2, a3, a4, a5, 0, 0, 1]

   [X]   [a0, a1, a2] [ Pixel]
   [Y] = [a3, a4, a5] [ Line ]
   [1]   [ 0,  0,  1] [  1   ]

.. _odc-apps-dc-tools: https://github.com/opendatacube/odc-tools/tree/develop/apps/dc_tools

.. _dataset-metadata-doc-3d:

3D dataset metadata
-------------------

3D loading can be achieved through ``odc-loader`` package.

#. Install odc-loader, e.g. ``pip install odc-loader``
#. Add a ``driver="rio",`` argument to ``dc.load()``

Time-stacked NetCDF files
-------------------------

It is possible to add NetCDF files with multiple time slices to the Open Data Cube index.
The time slice index can be specified by adding a fragment `#part=<int>` to the `path` of a band starting from 0.

Example:

.. code-block:: yaml

   measurements:
      time_stacked_netcdf_example:
        path: file://some.nc#part=0
        layer: some_var

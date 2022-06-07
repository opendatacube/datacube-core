.. _whats_new:

.. default-role:: code

What's New
**********

v1.8.next
=========

- Fix readthedocs build. (:pull:`1269`)

v1.8.7 (7 June 2022)
====================

- Cleanup mypy typechecking compliance. (:pull:`1266`)
- When dataset add operations fail due to lineage issues, the produced error message now clearly indicates that
  the problem was due to lineage issues. (:pull:`1260`)
- Added support for group-by financial years to virtual products. (:pull:`1257`, :pull:`1261`)
- Remove reference to `rasterio.path`. (:pull:`1255`)
- Cleaner separation of postgis and postgres drivers, and suppress SQLAlchemy cache warnings. (:pull:`1254`)
- Prevent Shapely deprecation warning. (:pull:`1253`)
- Fix `DATACUBE_DB_URL` parsing to understand syntax like: `postgresql:///datacube?host=/var/run/postgresql` (:pull:`1256`)
- Clearer error message when local metadata file does not exist. (:pull:`1252`)
- Address upstream security alerts and update upstream library versions. (:pull:`1250`)
- Clone ``postgres`` index driver as ``postgis``, and flag as experimental. (:pull:`1248`)
- Implement a local non-persistent in-memory index driver, with maximal backwards-compatibility
  with default postgres index driver. Doesn't work with CLI interface, as every invocation
  will receive a new, empty index, but useful for testing and small scale proof-of-concept
  work. (:pull:`1247`)
- Performance and correctness fixes backported from ``odc-geo``. (:pull:`1242`)
- Deprecate use of the celery executor. Update numpy pin in rtd-requirements.txt to suppress
  Dependabot warnings. (:pull:`1239`)
- Implement a minimal "null" index driver that provides an always-empty index. Mainly intended
  to validate the recent abstraction work around the index driver layer, but may be useful
  for some testing scenarios, and ODC use cases that do not require an index. (:pull:`1236')
- Regularise some minor API inconsistencies and restore redis-server to Docker image. (:pull:`1234`)
- Move (default) postgres driver-specific files from `datacube.index` to `datacube.index.postgres`.
  `datacube.index.Index` is now an alias for the abstract base class index interface definition
  rather than postgres driver-specific implementation of that interface. (:pull:`1231`)
- Update numpy and netcdf4 version in docker build (:pull:`1229`)
  rather than postgres driver-specific implementation of that interface. (:pull:`1227`)
- Migrate test docker image from `datacube/geobase` to `osgeo/gdal`. (:pull:`1233`)
- Separate index driver interface definition from default index driver implementation. (:pull:`1226`)
- Prefer WKT over EPSG when guessing CRS strings. (:pull:`1223`, :pull:`1262`)
- Updates to documentation. (:pull:`1208`, :pull:`1212`, :pull:`1215`, :pull:`1218`, :pull:`1240`, :pull:`1244`)
- Tweak to segmented in geometry to suppress Shapely warning. (:pull:`1207`)
- Fix to ensure ``skip_broken_datasets`` is correctly propagated in virtual products (:pull:`1259`)
- Deprecate `Rename`, `Select` and `ToFloat` built-in transforms in virtual products (:pull:`1263`)

v1.8.6 (30 September 2021)
==========================

- Fix for searching for multiple products, now works with ``product="product_name"``
  as well as ``product=["product_name1", "product_name2"]`` (:pull:`1201`)
- Added ``dataset purge`` command for hard deletion of archived datasets.
  ``--all`` option deletes all archived datasets.  (N.B. will fail if there
  are unarchived datasets that depend on the archived datasets.)

  ``--all`` option also added to ``dataset archive`` and ``dataset restore``
  commands, to archive all unarchived datasets, and restore all archived
  datasets, respectively.
  (:pull:`1199`)
- Trivial fixes to CLI help output (:pull:`1197`)

v1.8.5 (18 August 2021)
=======================

- Fix unguarded dependencies on boto libraries (:pull:`1174`, :issue:`1172`)
- Various documentation fixes (:pull:`1175`)
- Address import problems on Windows due to use of Unix only functions (:issue:`1176`)
- Address ``numpy.bool`` deprecation warnings (:pull:`1184`)


v1.8.4 (6 August 2021)
=======================

- Removed example and contributed notebooks from the repository. Better `notebook examples`_ exist.
- Removed datacube_apps, as these are not used and not maintained.
- Add ``cloud_cover`` to EO3 metadata
- Add ``erosion`` functionality to Virtual products' ``ApplyMask`` to supplement existing ``dilation`` functionality (:pull:`1049`)
- Fix numeric precision issues in ``compute_reproject_roi`` when pixel size is small. (:issue:`1047`)
- Follow up fix to (:issue:`1047`) to round scale to nearest integer if very close.
- Add support for 3D Datasets. (:pull:`1099`)
- New feature: search by URI from the command line ``datacube dataset uri-search``.
- Added new "license" and "description" properties to `DatasetType` to enable easier access to product information. (:pull:`1143`, :pull:`1144`)
- Revised the ``Datacube.list_products`` function to produce a simpler and more useful product list table (:pull:`1145`)
- Refactor docs, making them more up to date and simpler (:pull `1137`) (:pull `1128`)
- Add new ``dataset_predicate`` param to ``dc.load`` and ``dc.find_datasets`` for more flexible temporal filtering (e.g. loading data for non-contiguous time ranges such as specific months or seasons over multiple years). (:pull:`1148`, :pull:`1156`)
- Fix to ``GroupBy`` to ensure output output axes are correctly labelled when sorting observations using ``sort_key`` (:pull:`1157`)
- ``GroupBy`` is now its own class to allow easier custom grouping and sorting of data (:pull:`1157`)
- add support for IAM authentication for RDS databases in AWS. (:pull:`1168`)

.. _`notebook examples`: https://github.com/GeoscienceAustralia/dea-notebooks/


v1.8.3 (18 August 2020)
=======================

- More efficient band alias handling
- More documentation cleanups
- Bug fixes in ``datacube.utils.aws``, credentials handling when ``AWS_UNSIGNED`` is set
- Product definition can now optionally include per-band scaling factors (:pull:`1002`, :issue:`1003`)
- Fix issue where new ``updated`` columns aren't created on a fresh database (:pull:`994`, :issue:`993`)
- Fix bug around adding ``updated`` columns locking up active databases (:pull:`1001`, :issue:`997`)

v1.8.2 (10 July 2020)
=====================

- Fix regressions in ``.geobox`` (:pull:`982`)
- Expand list of supported ``dtype`` values to include complex values (:pull:`989`)
- Can now specify dataset location directly in the yaml document (:issue:`990`, :pull:`989`)
- Better error reporting in ``datacube dataset update`` (:pull:`983`)

v1.8.1 (2 July 2020)
====================

- Added an ``updated`` column for trigger based tracking of database row updates in PostgreSQL. (:pull:`951`)
- Changes to the writer driver API. The driver is now responsible for constructing output URIs from user configuration. (:pull:`960`)
- Added a :meth:`datacube.utils.geometry.assign_crs` method for better interoperability with other libraries (:pull:`967`)
- Better interoperability with xarray_ --- the :meth:`xarray.Dataset.to_netcdf` function should work again (:issue:`972`, :pull:`976`)
- Add support for unsigned access to public S3 resources from CLI apps (:pull:`976`)
- Usability fixes for indexing EO3 datasets (:pull:`958`)
- Fix CLI initialisation of the Dask Distributed Executor (:pull:`974`)

.. _xarray: https://xarray.pydata.org/

v1.8.0 (21 May 2020)
====================

- New virtual product combinator ``reproject`` for on-the-fly reprojection of rasters (:pull:`773`)
- Enhancements to the ``expressions`` transformation in virtual products (:pull:`776`, :pull:`761`)
- Support ``/vsi**`` style paths for dataset locations (:pull:`825`)
- Remove old Search Expressions and replace with a simpler implementation based on Lark Parser. (:pull:`840`)
- Remove no longer required PyPEG2 dependency. (:pull:`840`)
- Switched from Travis-CI to Github Actions for CI testing and docker image builds (:pull:`845`)
- Removed dependency on ``singledispatch``, it's available in the Python 3.4+ standard library.
- Added some configuration validation to Ingestion
- Allow configuring ODC Database connection settings entirely through environment variables. (:pull:`845`, :issue:`829`)

  Uses ``DATACUBE_DB_URL`` if present, then falls back to ``DB_HOSTNAME``,
  ``DB_USERNAME``, ``DB_PASSWORD``, ``DB_DATABASE``

- New Docker images. Should be smaller, better tested, more reliable and easier to work with. (:pull:`845`).

  - No longer uses an entrypoint script to write database configuration into a file.
  - Fixes binary incompatibilities in geospatial libraries.
  - Tested before being pushed to Docker Hub.

- Drop support for Python 3.5.
- Remove S3AIO driver. (:pull:`865`)
- Change development version numbers generation. Use ``setuptools_scm`` instead of ``versioneer``. (:issue:`871`)
- Deprecated ``datacube.helpers.write_geotiff``, use :meth:`datacube.utils.cog.write_cog` for similar functionality
- Deprecated ``datacube.storage.masking``, moved to ``datacube.utils.masking``
- Changed geo-registration mechanics for arrays returned by ``dc.load``. (:pull:`899`, :issue:`837`)
- Migrate geometry and CRS backends from ``osgeo.ogr`` and ``osgeo.osr`` to shapely_ and pyproj_ respectively (:pull:`880`)
- Driver metadata storage and retrieval. (:pull:`931`)
- Support EO3 style datasets in ``datacube dataset add`` (:pull:`929`, :issue:`864`)
- Removed migration support from datacube releases before 1.1.5.

  .. warning:: If you still run a datacube before 1.1.5 (from 2016 or older), you will need to update it
     using ODC 1.7 first, before coming to 1.8.

.. _shapely: https://pypi.org/project/pyproj/
.. _pyproj: https://pypi.org/project/Shapely/

v1.7.0 (16 May 2019)
====================

Not a lot of changes since rc1.

- Early exit from ``dc.load`` on `KeyboardInterrupt`, allows partial loads inside notebook.
- Some bug fixes in geometry related code
- Some cleanups in tests
- Pre-commit hooks configuration for easier testing
- Re-enable multi-threaded reads for s3aio driver. Set use_threads to True in dc.load()


v1.7.0rc1 (18 April 2019)
=========================

Virtual Products
~~~~~~~~~~~~~~~~

Add :ref:`virtual-products` for multi-product loading.

(:pull:`522`, :pull:`597`, :pull:`601`, :pull:`612`, :pull:`644`, :pull:`677`, :pull:`699`, :pull:`700`)

Changes to Data Loading
~~~~~~~~~~~~~~~~~~~~~~~
The internal machinery used when loading and reprojecting data, has been completely rewritten. The new code has been
tested, but this is a complicated and fundamental part of code and there is potential for breakage.

When loading reprojected data, the new code will produce slightly different results. We don't believe that it is any
less accurate than the old code, but you cannot expect exactly the same numeric results.

Non-reprojected loads should be identical.

This change has been made for two reasons:

1. The reprojection is now core Data Cube, and is not the responsibility of the IO driver.

2. When loading lower resolution data, DataCube can now take advantage of available overviews.

- New futures based IO driver interface (:pull:`686`)

Other Changes
~~~~~~~~~~~~~

- Allow specifying different resampling methods for different data variables of
  the same Product. (:pull:`551`)
- Allow all reampling methods supported by `rasterio`. (:pull:`622`)
- Bug fix (Index out of bounds causing ingestion failures)
- Support indexing data directly from HTTP/HTTPS/S3 URLs (:pull:`607`)
- Renamed the command line tool `datacube metadata_type` to `datacube metadata` (:pull:`692`)
- More useful output from the command line `datacube {product|metadata} {show|list}`
- Add optional `progress_cbk` to `dc.load(_data)` (:pull:`702`), allows user to
  monitor data loading progress.
- Thread-safe netCDF access within `dc.load` (:pull:`705`)

Performance Improvements
~~~~~~~~~~~~~~~~~~~~~~~~

- Use single pass over datasets when computing bounds (:pull:`660`)
- Bugfixes and improved performance of `dask`-backed arrays (:pull:`547`, :pull:`664`)

Documentation Improvements
~~~~~~~~~~~~~~~~~~~~~~~~~~

- Improve :ref:`api-reference` documentation.

Deprecations
~~~~~~~~~~~~

- From the command line, the old query syntax for searching within vague time ranges, eg: ``2018-03 < time < 2018-04``
  has been removed. It is unclear exactly what that syntax should mean, whether to include or exclude the months
  specified. It is replaced by ``time in [2018-01, 2018-02]`` which has the same semantics as ``dc.load`` time queries.
  (:pull:`709`)




v1.6.1 (27 August 2018)
=======================

Correction release. By mistake, v1.6.0 was identical to v1.6rc2!


v1.6.0 (23 August 2018)
=======================

- Enable use of *aliases* when specifying band names
- Fix ingestion failing after the first run (:pull:`510`)
- Docker images now know which version of ODC they contain (:pull:`523`)
- Fix data loading when `nodata` is `NaN` (:pull:`531`)
- Allow querying based on python :class:`datetime.datetime` objects. (:pull:`499`)
- Require `rasterio 1.0.2`_ or higher, which fixes several critical bugs when
  loading and reprojecting from multi-band files.
- Assume fixed paths for `id` and `sources` metadata fields (:issue:`482`)
- :class:`datacube.model.Measurement` was put to use for loading in attributes
  and made to inherit from `dict` to preserve current behaviour. (:pull:`502`)
- Updates when indexing data with `datacube dataset add` (See :pull:`485`, :issue:`451` and :issue:`480`)


  - Allow indexing without lineage `datacube dataset add --ignore-lineage`
  - Removed the `--sources-policy=skip|verify|ensure`. Instead use
    `--[no-]auto-add-lineage` and `--[no-]verify-lineage`
  - New option `datacube dataset add --exclude-product` ``<name>``
    allows excluding some products from auto-matching

- Preliminary API for indexing datasets (:pull:`511`)
- Enable creation of MetadataTypes without having an active database connection (:pull:`535`)

.. _rasterio 1.0.2: https://github.com/mapbox/rasterio/blob/1.0.2/CHANGES.txt

v1.6rc2 (29 June 2018)
======================

Backwards Incompatible Changes
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

- The `helpers.write_geotiff()` function has been updated to support files smaller
  than 256x256. It also no longer supports specifying the time index. Before passing
  data in, use `xarray_data.isel(time=<my_time_index>)`. (:pull:`277`)

- Removed product matching options from `datacube dataset update` (:pull:`445`).
  No matching is needed in this case as all datasets are already in the database
  and are associated to products.

- Removed `--match-rules` option from `datacube dataset add` (:pull:`447`)

- The seldom-used `stack` keyword argument has been removed from `Datcube.load`.
  (:pull:`461`)

- The behaviour of the time range queries has changed to be compatible with
  standard Python searches (eg. time slice an xarray). Now the time range
  selection is inclusive of any unspecified time units. (:pull:`440`)

  Example 1:
    `time=('2008-01', '2008-03')` previously would have returned all data from
    the start of 1st January, 2008 to the end of 1st of March, 2008. Now, this
    query will return all data from the start of 1st January, 2008 and
    23:59:59.999 on 31st of March, 2008.

  Example 2:
    To specify a search time between 1st of January and 29th of February, 2008
    (inclusive), use a search query like `time=('2008-01', '2008-02')`. This query
    is equivalent to using any of the following in the second time element:

    | `('2008-02-29')`
    | `('2008-02-29 23')`
    | `('2008-02-29 23:59')`
    | `('2008-02-29 23:59:59')`
    | `('2008-02-29 23:59:59.999')`


Changes
~~~~~~~

- A `--location-policy` option has been added to the `datacube dataset update`
  command. Previously this command would always add a new location to the list
  of URIs associated with a dataset. It's now possible to specify `archive` and
  `forget` options, which will mark previous location as archived or remove them
  from the index altogether. The default behaviour is unchanged. (:pull:`469`)

- The masking related function `describe_variable_flags()` now returns a pandas
  DataFrame by default. This will display as a table in Jupyter Notebooks.
  (:pull:`422`)

- Usability improvements in `datacube dataset [add|update]` commands
  (:issue:`447`, :issue:`448`, :issue:`398`)

  - Embedded documentation updates
  - Deprecated `--auto-match` (it was always on anyway)
  - Renamed `--dtype` to `--product` (the old name will still work, but with a warning)
  - Add option to skip lineage data when indexing (useful for saving time when
    testing) (:pull:`473`)

- Enable compression for metadata documents stored in NetCDFs generated by
  `stacker` and `ingestor` (:issue:`452`)

- Implement better handling of stacked NetCDF files (:issue:`415`)

  - Record the slice index as part of the dataset location URI, using `#part=<int>`
    syntax, index is 0-based
  - Use this index when loading data instead of fuzzy searching by timestamp
  - Fall back to the old behaviour when `#part=<int>` is missing and the file is
    more than one time slice deep

- Expose the following dataset fields and make them searchable:

  -  `indexed_time` (when the dataset was indexed)
  -  `indexed_by` (user who indexed the dataset)
  -  `creation_time` (creation of dataset: when it was processed)
  -  `label` (the label for a dataset)

  (See :pull:`432` for more details)

Bug Fixes
~~~~~~~~~

- The `.dimensions` property of a product no longer crashes when product is
  missing a `grid_spec`. It instead defaults to `time,y,x`

- Fix a regression in `v1.6rc1` which made it impossible to run `datacube
  ingest` to create products which were defined in `1.5.5` and earlier versions of
  ODC. (:issue:`423`, :pull:`436`)

- Allow specifying the chunking for string variables when writing NetCDFs
  (:issue:`453`)



v1.6rc1 Easter Bilby (10 April 2018)
====================================

This is the first release in a while, and so there's a lot of changes, including
some significant refactoring, with the potential having issues when upgrading.


Backwards Incompatible Fixes
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

 - Drop Support for Python 2. Python 3.5 is now the earliest supported Python
   version.

 - Removed the old ``ndexpr``, ``analytics`` and ``execution engine`` code. There is
   work underway in the `execution engine branch`_ to replace these features.

Enhancements
~~~~~~~~~~~~

 - Support for third party drivers, for custom data storage and custom index
   implementations

 - The correct way to get an Index connection in code is to use
   :meth:`datacube.index.index_connect`.

 - Changes in ingestion configuration

   - Must now specify the :ref:`write_plugin` to use. For s3 ingestion there was
     a top level ``container`` specified, which has been renamed and moved
     under ``storage``. The entire ``storage`` section is passed through to
     the :ref:`write_plugin`, so drivers requiring other configuration can
     include them here. eg:

     .. code:: yaml

         ...
         storage:
           ...
           driver: s3aio
           bucket: my_s3_bucket
         ...

 - Added a ``Dockerfile`` to enable automated builds for a reference Docker image.

 - Multiple environments can now be specified in one datacube config. See
   :pull:`298` and the :ref:`runtime-config-doc`

   - Allow specifying which ``index_driver`` should be used for an environment.

 - Command line tools can now output CSV or YAML. (Issue :issue:`206`, :pull:`390`)

 - Support for saving data to NetCDF using a Lambert Conformal Conic Projection
   (:pull:`329`)

 - Lots of documentation updates:

   - Information about :ref:`bit-masking`.

   - A description of how data is loaded.

   - Some higher level architecture documentation.

   - Updates on how to index new data.


Bug Fixes
~~~~~~~~~

 - Allow creation of :class:`datacube.utils.geometry.Geometry` objects from 3d
   representations. The Z axis is simply thrown away.

 - The `datacube --config_file` option has been renamed to
   `datacube --config`, which is shorter and more consistent with the
   other options. The old name can still be used for now.

 - Fix a severe performance regression when extracting and reprojecting a small
   region of data. (:pull:`393`)

 - Fix for a somewhat rare bug causing read failures by attempt to read data from
   a negative index into a file. (:pull:`376`)

 - Make :class:`CRS` equality comparisons a little bit looser. Trust either a
   *Proj.4* based comparison or a *GDAL* based comparison. (Closed :issue:`243`)

New Data Support
~~~~~~~~~~~~~~~~

 - Added example prepare script for Collection 1 USGS data; improved band
   handling and downloads.

 - Add a product specification and prepare script for indexing Landsat L2 Surface
   Reflectance Data (:pull:`375`)

 - Add a product specification for Sentinel 2 ARD Data (:pull:`342`)



.. _execution engine branch: https://github.com/opendatacube/datacube-core/compare/csiro/execution-engine

v1.5.4 Dingley Dahu (13th December 2017)
========================================
 - Minor features backported from 2.0:

    - Support for ``limit`` in searches

    - Alternative lazy search method ``find_lazy``

 - Fixes:

    - Improve native field descriptions

    - Connection should not be held open between multi-product searches

    - Disable prefetch for celery workers

    - Support jsonify-ing decimals

v1.5.3 Purpler Unicorn with Starlight (16 October 2017)
=======================================================

 - Use ``cloudpickle`` as the ``celery`` serialiser

v1.5.2 Purpler Unicorn with Stars (28 August 2017)
==================================================

 - Fix bug when reading data in native projection, but outside ``source`` area. Often hit when running ``datacube-stats``

 - Fix error loading and fusing data using ``dask``. (Fixes :issue:`276`)

 - When reading data, implement ``skip_broken_datasets`` for the ``dask`` case too


v1.5.4 Dingley Dahu (13th December 2017)
========================================
 - Minor features backported from 2.0:

    - Support for ``limit`` in searches

    - Alternative lazy search method ``find_lazy``

 - Fixes:

    - Improve native field descriptions

    - Connection should not be held open between multi-product searches

    - Disable prefetch for celery workers

    - Support jsonify-ing decimals

v1.5.3 Purpler Unicorn with Starlight (16 October 2017)
=======================================================

 - Use ``cloudpickle`` as the ``celery`` serialiser

 - Allow ``celery`` tests to run without installing it

 - Move ``datacube-worker`` inside the main datacube package

 - Write ``metadata_type`` from the ingest configuration if available

 - Support config parsing limitations of Python 2

 - Fix :issue:`303`: resolve GDAL build dependencies on Travis

 - Upgrade ``rasterio`` to newer version


v1.5.2 Purpler Unicorn with Stars (28 August 2017)
==================================================

 - Fix bug when reading data in native projection, but outside ``source`` area.
   Often hit when running ``datacube-stats``

 - Fix error loading and fusing data using ``dask``. (Fixes :issue:`276`)

 - When reading data, implement ``skip_broken_datasets`` for the ``dask`` case too


v1.5.1 Purpler Unicorn (13 July 2017)
=====================================

 - Fix bug :issue:`261`. Unable to load Australian Rainfall Grid Data. This was as a
   result of the CRS/Transformation override functionality being broken when
   using the latest ``rasterio`` version ``1.0a9``


v1.5.0 Purple Unicorn (9 July 2017)
===================================

New Features
~~~~~~~~~~~~

 - Support for AWS S3 array storage

 - Driver Manager support for NetCDF, S3, S3-file drivers.

Usability Improvements
~~~~~~~~~~~~~~~~~~~~~~

 - When ``datacube dataset add`` is unable to add a Dataset to the index, print
   out the entire Dataset to make it easier to debug the problem.

 - Give ``datacube system check`` prettier and more readable output.

 - Make ``celery`` and ``redis`` optional when installing.

 - Significantly reduced disk space usage for integration tests

 - ``Dataset`` objects now have an ``is_active`` field to mirror ``is_archived``.

 - Added ``index.datasets.get_archived_location_times()`` to see when each
   location was archived.

v1.4.1 (25 May 2017)
====================

 - Support for reading multiband HDF datasets, such as MODIS collection 6

 - Workaround for ``rasterio`` issue when reprojecting stacked data

 - Bug fixes for command line arg handling

v1.4.0 (17 May 2017)
====================

- Adds more convenient year/date range search expressions (see :pull:`226`)

- Adds a **simple replication utility** (see :pull:`223`)

- Fixed issue reading products without embedded CRS info, such as ``bom_rainfall_grid`` (see :issue:`224`)

- Fixed issues with stacking and ncml creation for NetCDF files

- Various documentation and bug fixes

- Added CircleCI as a continuous build system, for previewing generated documenation on pull

- Require ``xarray`` >= 0.9. Solves common problems caused by losing embedded ``flag_def`` and ``crs`` attributes.


v1.3.1 (20 April 2017)
======================

 - Docs now refer to "Open Data Cube"

 - Docs describe how to use ``conda`` to install datacube.

 - Bug fixes for the stacking process.

 - Various other bug fixes and document updates.

v1.3.0
======

 - Updated the Postgres product views to include the whole dataset metadata
   document.

 - ``datacube system init`` now recreates the product views by default every
   time it is run, and now supports Postgres 9.6.

 - URI searches are now better supported from the cli: ``datacube dataset search uri = file:///some/uri/here``

 - ``datacube user`` now supports a user description (via ``--description``)
   when creating a user, and delete accepts multiple user arguments.

 - Platform-specific (Landsat) fields have been removed from the default ``eo``
   metadata type in order to keep it minimal. Users & products can still add
   their own metadata types to use additional fields.

 - Dataset locations can now be archived, not just deleted. This represents a
   location that is still accessible but is deprecated.

 - We are now part of Open Data Cube, and have a new home at
   https://github.com/opendatacube/datacube-core

This release now enforces the uri index changes to be applied: it will prompt
you to rerun ``init`` as an administrator to update your existing cubes:
``datacube -v system init`` (this command can be run without affecting
read-only users, but will briefly pause writes)

v1.2.2
======

 - Added ``--allow-exclusive-lock`` flag to product add/update commands, allowing faster index updates when
   system usage can be halted.

 - ``{version}`` can now be used in ingester filename patterns

v1.2.0 Boring as Batman (15 February 2017)
==========================================
 - Implemented improvements to `dataset search` and `info` cli outputs

 - Can now specify a range of years to process to `ingest` cli (e.g. 2000-2005)

 - Fixed `metadata_type update` cli not creating indexes (running `system init` will create missing ones)

 - Enable indexing of datacube generated NetCDF files. Making it much easier to pull
   selected data into a private datacube index.
   Use by running `datacube dataset add selected_netcdf.nc`.

 - Switch versioning system to increment the second digit instead of the third.

v1.1.18 Mushroom Milkshake (9 February 2017)
============================================
 - Added `sources-policy` options to `dataset add` cli

 - Multiple dataset search improvements related to locations

 - Keep hours/minutes when grouping data by `solar_day`

 - Code Changes: `datacube.model.[CRS,BoundingBox,Coordinate,GeoBox` have moved into
   `datacube.utils.geometry`. Any code using these should update their imports.

v1.1.17 Happy Festivus Continues (12 January 2017)
==================================================

 - Fixed several issues with the geometry utils

 - Added more operations to the geometry utils

 - Updated `recipes` to use geometry utils

 - Enabled Windows CI (python 3 only)

v1.1.16 Happy Festivus (6 January 2017)
=======================================

  - Added `update` command to `datacube dataset` cli

  - Added `show` command to `datacube product` cli

  - Added `list` and `show` commands to `datacube metadata_type` cli

  - Added 'storage unit' stacker application

  - Replaced `model.GeoPolygon` with `utils.geometry` library

v1.1.15 Minion Party Hangover (1 December 2016)
===============================================

  - Fixed a data loading issue when reading HDF4_EOS datasets.

v1.1.14 Minion Party (30 November 2016)
=======================================

  - Added support for buffering/padding of GridWorkflow tile searches

  - Improved the Query class to make filtering by a source or parent dataset easier.
    For example, this can be used to filter Datasets by Geometric Quality Assessment (GQA).
    Use `source_filter` when requesting data.

  - Additional data preparation and configuration scripts

  - Various fixes for single point values for lat, lon & time searches

  - Grouping by solar day now overlays scenes in a consistent, northern scene takes precedence manner.
    Previously it was non-deterministic which scene/tile would be put on top.

v1.1.13 Black Goat (15 November 2016)
=====================================

  - Added support for accessing data through `http` and `s3` protocols

  - Added `dataset search` command for filtering datasets (lists `id`, `product`, `location`)

  - `ingestion_bounds` can again be specified in the ingester config

  - Can now do range searches on non-range fields (e.g. `dc.load(orbit=(20, 30)`)

  - Merged several bug-fixes from CEOS-SEO branch

  - Added Polygon Drill recipe to `recipes`

v1.1.12 Unnamed Unknown (1 November 2016)
=========================================

  - Fixed the affine deprecation warning

  - Added `datacube metadata_type` cli tool which supports `add` and `update`

  - Improved `datacube product` cli tool logging

v1.1.11 Unnamed Unknown (19 October 2016)
=========================================

  - Improved ingester task throughput when using distributed executor

  - Fixed an issue where loading tasks from disk would use too much memory

  - :meth:`.model.GeoPolygon.to_crs` now adds additional points (~every 100km) to improve reprojection accuracy

v1.1.10 Rabid Rabbit (5 October 2016)
=====================================

  - Ingester can now be configured to have WELD/MODIS style tile indexes (thanks Chris Holden)

  - Added --queue-size option to `datacube ingest` to control number of tasks queued up for execution

  - Product name is now used as primary key when adding datasets.
    This allows easy migration of datasets from one database to another

  - Metadata type name is now used as primary key when adding products.
    This allows easy migration of products from one database to another

  - :meth:`.DatasetResource.has` now takes dataset id insted of :class:`.model.Dataset`

  - Fixed an issues where database connections weren't recycled fast enough in some cases

  - Fixed an issue where :meth:`.DatasetTypeResource.get` and :meth:`.DatasetTypeResource.get_by_name`
    would cache `None` if product didn't exist


v1.1.9 Pest Hippo (20 September 2016)
=====================================

  - Added origin, alignment and GeoBox-based methods to :class:`.model.GridSpec`

  - Fixed satellite path/row references in the prepare scripts (Thanks to Chris Holden!)

  - Added links to external datasets in :ref:`indexing`

  - Improved archive and restore command line features: `datacube dataset archive` and `datacube dataset restore`

  - Improved application support features

  - Improved system configuration documentation


v1.1.8 Last Mammoth (5 September 2016)
======================================

  - :meth:`.GridWorkflow.list_tiles` and :meth:`.GridWorkflow.list_cells` now
    return a :class:`.Tile` object

  - Added `resampling` parameter to :meth:`.Datacube.load` and :meth:`.GridWorkflow.load`. Will only be used if the requested data requires resampling.

  - Improved :meth:`.Datacube.load` `like` parameter behaviour. This allows passing in a :class:`xarray.Dataset` to retrieve data for the same region.

  - Fixed an issue with passing tuples to functions in Analytics Expression Language

  - Added a :ref:`user_guide` section to the documentation containing useful code snippets

  - Reorganized project dependencies into required packages and optional 'extras'

  - Added `performance` dependency extras for improving run-time performance

  - Added `analytics` dependency extras for analytics features

  - Added `interactive` dependency extras for interactivity features


v1.1.7 Bit Shift (22 August 2016)
=================================

  - Added bit shift and power operators to Analytics Expression Language

  - Added `datacube product update` which can be used to update product definitions

  - Fixed an issue where dataset geo-registration would be ignored in some cases

  - Fixed an issue where Execution Engine was using dask arrays by default

  - Fixed an issue where int8 data could not sometimes be retrieved

  - Improved search and data retrieval performance


v1.1.6 Lightning Roll (8 August 2016)
=====================================

  - Improved spatio-temporal search performance. `datacube system init` must be run to benefit

  - Added `info`, `archive` and `restore` commands to `datacube dataset`

  - Added `product-counts` command to `datacube-search` tool

  - Made Index object thread-safe

  - Multiple masking API improvements

  - Improved database Index API documentation

  - Improved system configuration documentation


v1.1.5 Untranslatable Sign (26 July 2016)
=========================================

  - Updated the way database indexes are patitioned. Use `datacube system init --rebuild` to rebuild indexes

  - Added `fuse_data` ingester configuration parameter to control overlaping data fusion

  - Added `--log-file` option to `datacube dataset add` command for saving logs to a file

  - Added index.datasets.count method returning number of datasets matching the query


v1.1.4 Imperfect Inspiration (12 July 2016)
===========================================

  - Improved dataset search performance

  - Restored ability to index telemetry data

  - Fixed an issue with data access API returning uninitialized memory in some cases

  - Fixed an issue where dataset center_time would be calculated incorrectly

  - General improvements to documentation and usablity


v1.1.3 Speeding Snowball (5 July 2016)
======================================

  - Added framework for developing distributed, task-based application

  - Several additional Ingester performance improvements


v1.1.2 Wind Chill (28 June 2016)
================================

This release brings major performance and usability improvements

  - Major performance improvements to GridWorkflow and Ingester

  - Ingestion can be limited to one year at a time to limit memory usage

  - Ingestion can be done in two stages (serial followed by highly parallel) by using
    --save-tasks/load-task options.
    This should help reduce idle time in distributed processing case.

  - General improvements to documentation.


v1.1.1 Good Idea (23 June 2016)
===============================

This release contains lots of fixes in preparation for the first large
ingestion of Geoscience Australia data into a production version of
AGDCv2.

  - General improvements to documentation and user friendliness.

  - Updated metadata in configuration files for ingested products.

  - Full provenance history is saved into ingested files.

  - Added software versions, machine info and other details of the
    ingestion run into the provenance.

  - Added valid data region information into metadata for ingested data.

  - Fixed bugs relating to changes in Rasterio and GDAL versions.

  - Refactored :class:`GridWorkflow` to be easier to use, and include
    preliminary code for saving created products.

  - Improvements and fixes for bit mask generation.

  - Lots of other minor but important fixes throughout the codebase.


v1.1.0 No Spoon (3 June 2016)
=============================

This release includes restructuring of code, APIs, tools, configurations
and concepts. The result of this churn is cleaner code, faster performance and
the ability to handle provenance tracking of Datasets created within the Data
Cube.

The major changes include:

    - The ``datacube-config`` and ``datacube-ingest`` tools have been
      combined into ``datacube``.

    - Added dependency on ``pandas`` for nicer search results listing and
      handling.

    - :ref:`Indexing <indexing>` and :ref:`ingestion` have been split into
      separate steps.

    - Data that has been :ref:`indexed <indexing>` can be accessed without
      going through the ingestion process.

    - Data can be requested in any projection and will be dynamically
      reprojected if required.

    - **Dataset Type** has been replaced by :ref:`Product <product-definitions>`

    - **Storage Type** has been removed, and an :ref:`Ingestion Configuration <ingest-config>`
      has taken it's place.

    - A new :ref:`datacube-class` for querying and accessing data.


1.0.4 Square Clouds (3 June 2016)
=================================

Pre-Unification release.

1.0.3 (14 April 2016)
=====================

Many API improvements.

1.0.2 (23 March 2016)
=====================

1.0.1 (18 March 2016)
=====================

1.0.0 (11 March 2016)
=====================

This release is to support generation of GA Landsat reference data.


pre-v1 (end 2015)
=================

First working Data Cube v2 code.

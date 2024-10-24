.. _whats_new:

.. default-role:: code

What's New
**********

v1.9.next
=========

v1.9.0-rc10 (25th October 2024)
===============================

- Permissions management cleanup in postgis driver. (:pull:`1613`)
- Add `skip_broken_datasets` and `dc_load_limit` config options. (:pull:`1616`)
- Enable global environment variables with `ODC_ALL` naming convention (:pull:`1616`)
- Remove workaround for an odc-geo bug that is now fixed. (:pull:`1622`)
- Fix call to geopolygon search. (:pull:`1627`)
- Use antimeridian package to "fix" extent polygons. (:pull:`1628`)
- Record lineage when adding datasets with postgis index (:pull:`1632`)
- Update schema logic (:pull:`1634`)
- Drop valid-area check and anti-meridian fix 3857 extents (:pull:1635)
- Remove problematic "common_timestamp" postgresql function from postgis driver. Some internal API changes
  required to accommodate and preserve all previous index-driver level behaviour. (:pull:`1623`)
- Cherry picks from 1.8 (#1624-#1626, #1629-#1631) (:pull:`1637`)
- Updates to index APIs - add `order_by` to dataset search, index name attribute, product most recent change method (:pull:`1643`)
- Fix alembic migrations (:pull:`1645`)
- Cherry pick recent merged PRs from develop branch and update whats_new for rc10 release. (:pull:`1647`)

v1.9.0-rc9 (3rd July 2024)
==========================

- Ensure config API works with a blank config/empty file. (:pull:`1604`)
- Various minor maintenance fixes. (:pull:`1607`)
- Misc cleanup, and add support for geospatial queries to count methods in postgis driver. (:pull:`1608`)
- Add new driver based loader (via odc.loader) (:pull:`1609`)
- Fix 1.9 docker image in GHA (:pull:`1610`)
- Consolidate spatial search argument handling in index layer and prepare for release. (:pull:`1611`)

v1.9.0-rc8 (18th June 2024)
===========================

Bugfix pre-release.

The 1.9.0 branch will remain in pre-release until we have working 1.9-compatible versions of the following
key ODC packages: `datacube-explorer`, `datacube-ows`, `eodatasets`, and `odc-apps-dc-tools`.
Work on migrating these packages is underway, and this pre-release addresses issues identified in
the course of that work.

Due to an error in packaging, schema creation and maintenance via alembic for the postgis
index driver has been broken for wheels based installations (including installing from PyPI via pip)
since it was first introduced in 1.9.0-rc1.  There was a failed attempt to fix this in the previous
pre-release.  This pre-release finally fixes it properly.

- Fix packaging so that alembic data files are correctly packaged in wheels and update whats_news.rst
  ready for 1.9.0-rc8 pre-release.  (:pull:`1599`)

v1.9.0-rc7 (17th June 2024)
===========================

Bugfix pre-release.

The 1.9.0 branch will remain in pre-release until we have working 1.9-compatible versions of the following
key ODC packages: `datacube-explorer`, `datacube-ows`, `eodatasets`, and `odc-apps-dc-tools`.
Work on migrating these packages is underway, and this pre-release addresses issues identified in
the course of that work.

Note: rc6 was accidentally released off the wrong branch and has been pulled from PyPI.

- Update whats_new.rst and dropped nominal support for Windows, ready for 1.9.0-rc6 release. (:pull:`1598`)
- Fix multi-threading race condition in config API. (:pull:`1596`)
- Move alembic.ini to a location where it will get installed by pip (without -e). (:pull:`1597`)

v1.9.0-rc5 (5th June 2024)
==========================

Another release candidate for the 1.9.0 release.  The 1.9.0 branch will remain in pre-release until we have
working 1.9-compatible versions of the following key ODC packages: `datacube-explorer`, `datacube-ows`,
`eodatasets`, and `odc-apps-dc-tools`.  Work on migrating these packages is underway, and this pre-release
is mostly a response to that work - applying changes intended to ease 1.8.x to 1.9.x migration.

- Update whats_new.rst, ready for 1.9.0-rc5 release (:pull:`1594`)
- Expand extra dimension support, WIP (:pull:`1593`)
- Ensure pre-prepared EO3 datasets can be indexed. (i.e. ensure the `prep_eo3()` function is idempotent) (:pull:`1591`)
- The canonical name of the postgres driver is now "postgres" with "default" as an alias instead of the other
  way around. (:pull:`1590`)
- Update docker image to GDAL 3.9/Python 3.12/Ubuntu 24.04 (:pull:`1588`)
- Fix typos in docs (:pull:`1577`)
- Merge in recent 1.8.x branch changes. (:pull:`1568`, :pull:`1579`)
- Add Product delete methods to API and command in CLI, plus misc cleanup of the surrounds (:pull:`1583`)

v1.9.0-rc4 (15th April 2024)
============================

- Standardize resampling input supported to `odc.geo.warp.Resampling` (:pull:`1571`)
- Refine default behaviour for config engine to support easier migration from 1.8 (:pull:`1573`)
- Convert legacy GeoBoxes to odc.geo GeoBoxes in the core API (:pull:`1574`)
- Add URL component pseudo to config layer to expose components to the api when configured as a URL,
  and reformat whats_new for 1.9.0-rc4 release. (:pull:`1575`)


v1.9.0-rc3 (27th March 2024)
============================

Re-pre-release of 1.9.0-rc1 to get PyPI version numbers back in sync.


v1.9.0-rc1 (27th March 2024)
============================

- Merge in 1.8.x branch changes. (:pull:`1459`, :pull:`1473`, :pull:`1532`, :pull:`1548`, :pull:`1565`)
- External Lineage API (:pull:`1401`)
- Add lineage support to index clone operation (:pull:`1429`)
- Migrate to SQLAlchemy 2.0 (:pull:`1432`)
- Clean up deprecated code and add deprecation warnings to legacy methods, simplify DocReader logic (:pull:`1406`)
- Mark geometry module as deprecated and replace all usage with odc-geo (:pull:`1424`)
- Mark GridSpec as deprecated, replace math and cog functions with odc-geo equivalents, enforce new odc-geo conventions (:pull:`1441`)
- Rename ``gbox`` to ``geobox`` in parameter names (:pull:`1441`)
- Remove executor API (:pull:`1462`)
- Remove ingestion methods, ``GridWorkflow`` and ``Tile`` classes (:pull:`1465`)
- Fix postgis queries for numeric custom search fields (:pull:`1475`)
- Document best practice for pulling in changes from develop and update constraints.txt (:pull:`1478`)
- Postgis index driver performance tuning (:pull:`1480`)
- Cleanup and formalise spatial index API and expose in CLI (:pull:`1481`)
- Increase minimum Python version to 3.10 (:pull:`1509`)
- Virtual product tests using odc-geo GridSpec (:pull:`1512`)
- New Configuration API, as per ODC-EP10 (:pull:`1505`)
- Alembic migrations for postgis driver (:pull:`1520`)
- EP08 lineage extensions/changes to datasets.get(). (:pull:`1530`)
- EP13 API changes to Index and IndexDriver. (:pull:`1534`)
- EP13 API changes to metadata and product resources. (:pull:`1536`)
- Phase 1 of EP13 API changes to dataset resource - get_unsafe, get_derived, temporal_extent. (:pull:`1538`)
- Add product argument to spatial_extent method, as per EP13. (:pull:`1539`)
- Index driver API type hint cleanup. (:pull:`1541`)
- Deprecate multiple locations. (:pull:`1546`)
- Deprecate search_eager and search_summaries and add `archived` arg to all dataset search/count methods. (:pull:`1550`)
- Compatibility fix - dc.load can take odc.geo GeoBox (:pull:`1551`)
- Migrate away from deprecated Python pkg_resources module (:pull:`1558`)
- Add ``custom_offsets`` and ``order_by`` arguments to search_retunrning() - order_by still unimplemented. (:pull:`1557`)
- Fix and enhance typehints, automated static type checking with mypy.  (:pull:`1562`)
- Improve SQLAlchemy join hints, addressing an recurring but intermittent bug.  (:pull:`1564`)
- Improve typehints and update docstrings in datacube/api/core.py (:pull:`1567`)
- Add migration notes, update documentation and whats_new.rst for 1.9.0-rc1 release (:pull:`1569`)

v1.8.next
=========
- Don't error when adding a dataset whose product doesn't have an id value (:pull:`1630`)

v1.8.19 (2nd July 2024)
=======================

- Update whats_new for 1.8.19 release (:pull:`1612`)
- Always write floating point bands to cogs with nodata=nan for ESRI and GDAL compatibility (:pull:`1602`)
- Add deprecation warning for config environment names that will not be supported in 1.9 (:pull:`1592`)
- Update docker image to GDAL 3.9/Python 3.12/Ubuntu 24.04 (:pull:`1587`)
- Update readthedocs stylesheet for dark theme (:pull:`1579`)

v1.8.18 (27th March 2024)
=========================

- Add dataset cli tool ``find-duplicates`` to identify duplicate indexed datasets (:pull:`1517`)
- Make solar_day() timezone aware (:pull:`1521`)
- Warn if non-eo3 dataset has eo3 metadata type (:pull:`1523`)
- Update pandas version in docker image to be consistent with conda environment and default to stdlib
  timezone instead of pytz when converting timestamps; automatically update copyright years (:pull:`1527`)
- Update github-Dockerhub credential-passing mechanism. (:pull:`1528`)
- Tweak ``list_products`` logic for getting crs and resolution values (:pull:`1535`)
- Add new ODC Cheatsheet reference doc to Data Access & Analysis documentation page (:pull:`1543`)
- Compatibility fix to allow users to supply ``odc.geo``-style GeoBoxes to ``dc.load(like=...)`` (:pull:`1551`)
- Fix broken codecov github action. (:pull:`1554`)
- Update documentation links to DEA Knowledge Hub (:pull:`1559`)
- Throw error if ``time`` dimension is provided as an int or float to Query construction
  instead of assuming it to be seconds since epoch (:pull:`1561`)
- Add generic NOT operator and for ODC queries and ``Not`` type wrapper (:pull:`1563`)
- Update whats_new.rst for release (:pull:`1568`)


v1.8.17 (8th November 2023)
===========================
- Fix schema creation with postgres driver when initialising system with ``--no-init-users`` (:pull:`1504`)
- Switch to new jsonschema 'referencing' API and repin jsonschema to >=4.18 (:pull:`1477`)
- Update whats_new.rst for release (:pull:`1510`)

v1.8.16 (17th October 2023)
===========================
- Improve error message for mismatch between dataset metadata and product signature (:pull:`1472`)
- Mark ``--confirm-ignore-lineage``, ``--auto-add-lineage``, and ``--verify-lineage`` as deprecated
  or to be deprecated (:pull:`1472`)
- Default delta values in ``archive_less_mature`` and ``find_less_mature`` (:pull:`1472`)
- Fix SQLAlchemy calls and pin jsonschema version to suppress deprecation warnings (:pull:`1476`)
- Throw a better error if a dataset is not compatible with ``archive_less_mature`` logic (:pull:`1491`)
- Fix broken Github action workflow (:pull:`1496`)
- Support ``like=<GeoBox>`` in virtual product ``load`` (:pull:`1497`)
- Don't archive less mature if archive_less_mature is provided as `False` instead of `None` (:pull:`1498`)
- Raise minimum supported Python version to 3.9 (:pull:`1500`)
- Manually apply Dependabot updates, and update whats_new.rst for 1.8.16 release (:pull:`1501`)

v1.8.15 (11th July 2023)
========================
- Replace `importlib_metadata` for python <3.10 compatibility
  (:pull:`1469`)
- Update whats_new.rst for release (:pull:`1470`)

v1.8.14 (28th June 2023)
========================

- Second attempt to address unexpected handling of image aspect ratios in rasterio and
  GDAL. (:pull:`1457`)
- Fix broken pypi publishing Github action (:pull:`1454`)
- Documentation improvements (:pull:`1455`)
- Increase default maturity leniency to +-500ms (:pull:`1458`)
- Add option to specify maturity timedelta when using ``--archive-less-mature`` option (:pull:`1460`)
- Mark executors as deprecated (:pull:`1461`)
- Mark ingestion as deprecated (:pull:`1463`)
- Replace deprecated ``pkg_resources`` with ``importlib.resources`` and ``importlib.metadata`` (:pull:`1466`)
- Update whats_new.rst for release (:pull:`1467`)

v1.8.13 (6th June 2023)
=======================

- Fix broken Github action workflows (:pull:`1425`, :pull:`1427`, :pull:`1433`)
- Setup Dependabot, and Dependabot-generated updates (:pull:`1416`, :pull:`1420`, :pull:`1423`,
            :pull:`1428`, :pull:`1436`, :pull:`1447`)
- Documentation fixes (:pull:`1417`, :pull:`1418`, :pull:`1430`)
- ``datacube dataset`` cli commands print error message if missing argument (:pull:`1437`)
- Add pre-commit hook to verify license headers (:pull:`1438`)
- Support open-ended date ranges in `datacube dataset search`, `dc.load`, and `dc.find_datasets` (:pull:`1439`, :pull:`1443`)
- Pass Y and Y Scale factors through to rasterio.warp.reproject, to eliminate projection bug affecting
  non-square Areas Of Interest (See `Issue #1448`_) (:pull:`1450`)
- Add `archive_less_mature` option to `datacube dataset add` and `datacube dataset update` (:pull:`1451`)
- Allow for +-1ms leniency in finding other maturity versions of a dataset (:pull:`1452`)
- Update whats_new.rst for release (:pull:`1453`)

.. _`Issue #1448`: https://github.com/opendatacube/datacube-core/issues/1448

v1.8.12 (7th March 2023)
========================

- Rename Geometry `type` attribute to `geom_type`, to align with Shapely 2.0 (:pull:`1402`)
- Remove some deprecated SQLAlchemy usages (:pull:`1403`, :pull:`1407`)
- Fix RTD docs build (:pull:`1399`)
- Minor Documentation fixes (:pull:`1409`, :pull:`1413`)
- Bug-fix and code cleanup in virtual products (:pull:`1410`)
- Reduce transaction isolation level to improve database write concurrency (:pull:`1414`)
- Update dependency versions and whats_new.rst for release (:pull:`1415`)


v1.8.11 (6 February 2023)
=========================

- Simplify Github actions (:pull:`1393`)
- Update conda create environment README (:pull:`1394`)
- Update conda environment file and add notes to release process to ensure pip and conda
  dependencies are in sync and up-to-date. (:pull:`1395`)
- Update docker constraints (:pull:`1396`)
- Compatible with the changes w.r.t. `MultiIndex` and `coord/dims` introduced since `xarray>2022.3.0` (:pull:`1397`)
- Final sync of conda/pip dependencies and release notes. (:pull:`1398`)


v1.8.10 (30 January 2023)
=========================

Notes for 1.8.10
~~~~~~~~~~~~~~~~

 1. The new APIs for bulk-reads, bulk-writes and index cloning should be considered unstable and may change
    in subsequent releases.
 2. Recent refactoring in the XArray library has lead to changes in behaviour that affect some ODC operations
    and are unlikely to be addressed by the XArray team.  This release includes changes in the way the ODC
    works with XArray to circumvent these issues. If you experience Xarray issues with this ODC release, please
    raise an issue on Github and we will try to address them before the next release.

Full list of changes:
~~~~~~~~~~~~~~~~~~~~~

- Add `grid_spec` to `list_products` (:pull:`1357`)
- Add database relationship diagram to doc (:pull:`1350`)
- Maintain search field index tables, and use them for dataset queries (:pull:`1360`)
- Change Github lint action to use ``conda`` and remove ``flake8`` from action (:pull:`1361`)
- Fix database relationship diagram instruction for docker (:pull:`1362`)
- Document ``group_by`` for ``dataset.load`` (:pull:`1364`)
- Add search_by_metadata facility for products (:pull:`1366`)
- Postgis driver cleanup - remove faux support for lineage (:pull:`1368`)
- Add support for nested database transactions (:pull:`1369`)
- Fix Github doc lint action (:pull:`1370`)
- Tighten EO3 enforcement in postgis driver, refactor tests, and rename Dataset.type to Dataset.product
  (with type alias for compatibility) (:pull:`1372`)
- Fix deprecation message due to distutils Version classes (:pull:`1375`)
- Postgresql drivers cleanup - consolidate split_uri into utils and removed unused constants (:pull:`1378`)
- Postgresql drivers cleanup - Handle NaNs in search fields and allow caching in sanitise_extent (:pull:`1379`)
- Fix example product yaml documentation (:pull:`1384`)
- Bulk read/write API methods and fast whole-index cloning. Cloning does NOT include lineage information yet,
  and new API methods may be subject to change. (:pull:`1381`)
- Documentation update. (:pull:`1385`)
- Clean up datetime functions (:pull:`1387`)
- Dependency updates (:pull:`1388`, :pull:`1391`)
- Upgrades for compatibility with newer versions of Shapely and Xarray.  (:pull:`1389`)
- Finalise release notes for 1.8.10 release (:pull:`1392`)

v1.8.9 (17 November 2022)
=========================

- Performance improvements to CRS geometry class (:pull:`1322`)
- Extend `patch_url` argument to `dc.load()` and `dc.load_data()` to Dask loading.  (:pull:`1323`)
- Add `sphinx.ext.autoselectionlabel` extension to readthedoc conf to support `:ref:` command (:pull:`1325`)
- Add `pyspellcheck` for `.rst` documentation files and fix typos (:pull:`1327`)
- Add `rst` documentation lint github action and apply best practices (:pull:`1328`)
- Follow PEP561_ to make type hints available to other packages (:pull:`1331`)
- Updated GitHub actions config to remove deprecated `set-output` (:pull:`1333`)
- Add what's new page link to menu and general doc fixes (:pull:`1335`)
- Add `search_fields` to required for metadata type schema and update doc (:pull:`1339`)
- Fix typo and update metadata documentation (:pull:`1340`)
- Add readthedoc preview github action (:pull:`1344`)
- Update `nodata` in readthedoc for products page (:pull:`1347`)
- Add `eo-datasets` to extensions & related software doc page (:pull:`1349`)
- Fix bug affecting searches against range types of zero width (:pull:`1352`)
- Add 1.8.9 release date and missing PR to `whats_news.rst` (:pull:`1353`)

.. _PEP561: https://peps.python.org/pep-0561/

v1.8.8 (5 October 2022)
=======================

- Migrate main test docker build to Ubuntu 22.04 and Python 3.10. (:pull:`1283`)
- Dynamically create tables to serve as spatial indexes in postgis driver. (:pull:`1312`)
- Populate spatial index tables, automatically and manually. (:pull:`1314`)
- Perform spatial queries against spatial index tables in postgis driver. (:pull:`1316`)
- EO3 data fixtures and tests. Fix SQLAlchemy bugs in postgis driver. (:pull:`1309`)
- Dependency updates. (:pull:`1308`, :pull:`1313`)
- Remove several features that had been deprecated in previous releases. (:pull:`1275`)
- Fix broken paths in api docs. (:pull:`1277`)
- Fix readthedocs build. (:pull:`1269`)
- Add support for Jupyter Notebooks pages in documentation (:pull:`1279`)
- Add doc change comparison for tuple and list types with identical values (:pull:`1281`)
- Add flake8 to Github action workflow and correct code base per flake8 rules (:pull:`1285`)
- Add `dataset id` check to dataset doc resolve to prevent `uuid` returning error when `id` used in `None`  (:pull:`1287`)
- Add how to run targeted single test case in docker guide to README (:pull:`1288`)
- Add `help message` for all `dataset`, `product` and `metadata` subcommands when required arg is not passed in (:pull:`1292`)
- Add `error code 1` to all incomplete `dataset`, `product` and `metadata` subcommands (:pull:`1293`)
- Add `exit_on_empty_file` message to `product` and `dataset` subcommands instead of returning no output when file is empty (:pull:`1294`)
- Add flags to index drivers advertising what format datasets they support (eo/eo3/non-geo (e.g. telemetry only))
  and validate in the high-level API. General refactor and cleanup of eo3.py and hl.py. (:pull: `1296`)
- Replace references to 'agdc' and 'dataset_type' in postgis driver with 'odc' and 'product'. (:pull: `1298`)
- Add warning message for product and metadata add when product and metadata is already in the database. (:pull: `1299`)
- Ensure SimpleDocNav.id is of type UUID, to improve lineage resolution (:pull: `1304`)
- Replace SQLAlchemy schema and query definitions in experimental postgis driver with newer "declarative" style ORM.
  Portions of API dealing with lineage handling, locations, and dynamic indexes are currently broken in the postgis
  driver. As per the warning message, the postgis driver is currently flagged as "experimental" and is not considered
  stable. (:pull: `1305`)
- Implement `patch_url` argument to `dc.load()` and `dc.load_data()` to provide a way to sign dataset URIs, as
  is required to access some commercial archives (e.g. Microsoft Planetary Computer).  API is based on the `odc-stac`
  implementation. Only works for direct loading.  More work required for deferred (i.e. Dask) loading. (:pull: `1317`)
- Implement public-facing index-driver-independent API for managing database transactions, as per Enhancement Proposal
  EP07 (:pull: `1318`)
- Update Conda environment to match dependencies in setup.py (:pull: `1319`)
- Final updates to whats_new.rst for release (:pull: `1320`)


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
  for some testing scenarios, and ODC use cases that do not require an index. (:pull:`1236`)
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
- Fix to ``GroupBy`` to ensure output axes are correctly labelled when sorting observations using ``sort_key`` (:pull:`1157`)
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

- The seldom-used `stack` keyword argument has been removed from `Datacube.load`.
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

- Added CircleCI as a continuous build system, for previewing generated documentation on pull

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

  - Updated the way database indexes are partitioned. Use `datacube system init --rebuild` to rebuild indexes

  - Added `fuse_data` ingester configuration parameter to control overlapping data fusion

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

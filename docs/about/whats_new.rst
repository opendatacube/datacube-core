What's New
==========

v1.1.6 Lightning Roll (8 August 2016)
-------------------------------------

  - Improved spatio-temporal search performance. `datacube system init` must be run to benefit

  - Added `info`, `archive` and `restore` commands to `datacube dataset`

  - Added `product-counts` command to `datacube-search` tool

  - Made Index object thread-safe

  - Multiple masking API improvements

  - Improved database Index API documentation

  - Improved system configuration documentation


v1.1.5 Untranslatable Sign (26 July 2016)
-----------------------------------------

  - Updated the way database indexes are patitioned. Use `datacube system init --rebuild` to rebuild indexes

  - Added `fuse_data` ingester configuration parameter to control overlaping data fusion

  - Added `--log-file` option to `datacube dataset add` command for saving logs to a file

  - Added index.datasets.count method returning number of datasets matching the query


v1.1.4 Imperfect Inspiration (12 July 2016)
-------------------------------------------

  - Improved dataset search performance

  - Restored ability to index telemetry data

  - Fixed an issue with data access API returning uninitialized memory in some cases

  - Fixed an issue where dataset center_time would be calculated incorrectly

  - General improvements to documentation and usablity


v1.1.3 Speeding Snowball (5 July 2016)
--------------------------------------

  - Added framework for developing distributed, task-based application

  - Several additional Ingester performance improvements


v1.1.2 Wind Chill (28 June 2016)
--------------------------------

This release brings major performance and usability improvements

  - Major performance improvements to GridWorkflow and Ingester

  - Ingestion can be limited to one year at a time to limit memory usage

  - Ingestion can be done in two stages (serial followed by highly parallel) by using
    --save-tasks/load-task options.
    This should help reduce idle time in distributed processing case.

  - General improvements to documentation.


v1.1.1 Good Idea (23 June 2016)
-------------------------------

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
-----------------------------

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

    - **Storage Type** has been removed, and an :ref:`Ingestion Configuration
      <ingest-config>` has taken it's place.

    - A new :ref:`datacube-class` for querying and accessing data.


1.0.4 Square Clouds (3 June 2016)
---------------------------------

Pre-Unification release.

1.0.3 (14 April 2016)
---------------------

Many API improvements.

1.0.2 (23 March 2016)
---------------------

1.0.1 (18 March 2016)
---------------------

1.0.0 (11 March 2016)
---------------------

This release is to support generation of GA Landsat reference data.


pre-v1 (end 2015)
-----------------

First working Data Cube v2 code.

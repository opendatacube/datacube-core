
Migrating from ODC 1.8.x to 1.9.x
=================================

The last new major release of the Open Data Cube was v1.8.0 in May 2020, nearly 4 years ago.

ODC developers and the Steering Council have been working hard behind the scenes over the last couple of years
to address some of the accumulated technical debt in datacube-core and prepare for new major releases.

The long-term plan includes a number of significant backwards-incompatible changes.  An effort has been made to
provide a smooth migration pathway wherever possible, with existing behaviour in 1.8.x versions being deprecated
in 1.9.x, with alternatives being provided, then removing the deprecated behaviour in 2.0.x with the alternative
approaches becoming the standard, but some minor backwards incompatible changes in 1.9.x were unavoidable.

This document describes the changes between 1.8.x and 1.9.x, with a particular focus on backwards incompatible
changes and new features.

After  the release of 1.9.0, focus will shift to updating secondary ODC libraries to work with ODC-1.9.  (Explorer
and OWS in particular will require major changes.)   We will continue to support and release 1.8.x versions after
the release of 1.9.0, until the 1.9.x releases have stabilised and all secondary libraries are up to date.

Smaller ODC installations will probably prefer to stick with the 1.8.x releases for the time being, but if you can
spare the resources we encourage you to set up a 1.9.x installation to test your existing code and systems
against the new release, and open issues on github for any problems that you come up against, especially any that are
not documented here.

Major Changes between 1.8.x and 1.9.x
-------------------------------------

1. Integration with ``odc-geo``.

   The old ``datacube.utils.geometry`` library has been replaced by ``odc-geo``.

   If you have already used ``odc-geo`` you will appreciate the additional power and flexibility that this brings to
   core.  If you have not, please take the time to have a read through the
   `odc-geo documentation <https://odc-geo.readthedocs.io/en/latest/>`_  and especially the
   `migration notes <https://odc-geo.readthedocs.io/en/latest/migration.html>`_.  In particular, you should familiarise
   yourself with ``.odc`` accessor which ``odc-geo`` dynamically adds to all xarray ``DataArray`` and ``Dataset``
   objects.

   Note that ``dc.load()`` now preferentially accepts ``odc-geo`` data types for passing ``GeoBox`` via the ``like``
   parameter, as well as ``resolution`` and ``align`` values, although backwards compatible behaviour with the old
   types is available with a deprecation warning.

   The classes and methods in ``datacube.utils.geometry`` are still available, but raise a deprecation warning when
   used.  Please migrate all code to use the equivalent methods and classes in ``odc-geo``.

2. A new configuration engine has replaced the configuration engine used previously.

   There are some backwards-incompatible changes as noted below, but most existing configuration files should
   continue to work as previously with minimal changes.

   The behaviour of the new configuration engine (and the reasoning behind the changes) is fully documented in
   `ODC Enhancement Proposal 10 <https://github.com/opendatacube/datacube-core/wiki/ODC-EP-010---Replace-Configuration-Layer>`_

   a. Previously multiple config files could be read and merged to generate the final effective configuration file.
      From 1.9.0 only a single config file is ever read at a time.  Managed instances which have previously allowed
      user customisation by the user creating a minimal config file which was loaded merged on top of a default system
      configuration will have to migrate to a system whereby users take a copy of the default system configuration file
      and edit that copy for their needs.

   b. The "user" section no longer has a special meaning, as the old special meaning is irrelevant now that config
      files are not merged.

   c. Previously only the INI file format was supported for configuration files. The JSON and YAML formats are now also
      supported.

   d. Previously configuration by Environment Variables was implemented in an inconsistent and ad hoc way that resulted
      in complex interactions that were impossible to predict without intimate knowledge of the source code that
      implemented it.  There is now a consistent and systematic approach taken to the interaction between the
      active configuration file and environment variables.  Partial backwards compatibility is attempted, but
      full backwards compatibility is not possible due to the ad hoc nature of the previous implementation.

      The new (preferred) environment variable names are of the form ``$ODC_<env_name>_<item_name>``

   e. Tighter restrictions are applied to environment names.  This is required to ensure consistent interaction
      between config files and environment variables.  Environment names can now only contain alphanumeric characters.
      (Dashes and underscores must be removed).

   f. The preferred default environment name is now ``default``.  It is suggested that every config file should
      start with a "default" section that is an alias to an environment defined in full elsewhere in the file.

3. The index driver API has been cleaned up and simplified, facilitating easier development of new index backends.
   This should be largely invisible to most users, although some more rarely used methods and/or arguments are now
   deprecated.  The deprecation warnings provide specific migration advice for each case.

4. A new PostGIS-based index backend is now available.

   The legacy Postgres index driver will continue to be supported in 1.9, but will be dropped in ODC-2.0.

   The Postgis index driver only supports EO3-compatible metadata types.  Older EO-style metadata types should
   be migrated to EO3 before indexing into a Postgis driver index.  We will try to provide tools to assist with
   this migration but they are not yet available in 1.9.0 and due to the arbitrary generality of pre-EO3 ODC
   metadata, such tools may not be possible in all cases.  (The legacy postgres driver will continue to support
   non-EO3 metadata types until it is dropped in 2.0)

   The postgis driver will support the creation of PostGIS spatial indexes for arbitrary CRSs.  This will improve
   efficiency and accuracy of database searches, particularly when working with data covering regions where
   conversions to/from EPSG:4326 lat/long coordinates are highly non-linear (e.g. the Pacific around the
   anti-meridian and the north and south polar regions).

   The postgis driver uses Alembic for managing schema migrations, so future changes to the postgis database
   schema will be much easier to roll out than in the past.

   See below for more information about migrating to the Postgis index driver.

   Note that many other libraries in the ODC ecosystem may not work well with the Postgis driver at first. As noted
   above, Explorer and Datacube-OWS in particular will need extensive changes before they can be used with the new
   index driver.

5. New Lineage API (Postgis driver only)

   The postgis driver handles lineage very differently to the postgres driver: Lineage data is only loosely coupled
   to dataset metadata and  a completely new API is introduced for working with lineages.  It is now possible to
   store external lineage information - i.e. it is not necessary for both the source and derived dataset to exist
   in the index for the lineage relationship between them to be recorded in the database and powerful new
   data structures allow working with arbitrarily nested lineage trees in both the "source-wards" and
   "derived-wards" directions.

   A full description of the new lineage API can be found in
   `ODC Enhancement Proposal 8 <https://github.com/opendatacube/datacube-core/wiki/ODC-EP-008>`_

   The handling of lineage in the legacy postgres index driver has not changed - the postgres driver does NOT support
   the new lineage API.

6. Support for multi-dimensional loading of hyperspectral datasets (Coming Soon)

   This is a work in progress and will not be available in 1.9.0. It will appear in a later 1.9.x release.

7. The long-deprecated "ingestion" workflow and "excecutor" API have both been removed.

8. Multiple locations per dataset is now deprecated.

The New Postgis Index Driver
----------------------------

Configuration
+++++++++++++

The configuration for a postgis index looks the same as the configuration for a legacy postgres index, you simply
set the ``index_driver`` setting to ``postgis``::

   [default]
      alias: prod

   [old]
      index_driver: postgres
      db_hostname: production.dbs.internal
      db_database: odc_prod
      db_username: odc
      db_password: secret_and_secure

   [new]
      index_driver: postgis
      db_hostname: dev.dbs.example.net
      db_database: odc_dev
      db_username: odc

Initialisation
++++++++++++++

You then initialise the database as previously, using ``system init`` command (-E new says to use the ``new`` environment
from the configuration file)::

   datacube -E new system init

You should also create Postgis spatial indexes for any CRS you want to be able to search on (note that an EPSG:4326
spatial index is created by default).   Postgis spatial indexes should be created before indexing any data where
possible.  Adding a new spatial index to a populated index will be very slow.  For example to create a spatial index
for EPSG:3577::

   datacube -E new spindex create 3577

Migrating (Cloning) Data From a Postgres Index
++++++++++++++++++++++++++++++++++++++++++++++

To clone data from an old index to a new one (the two indexes may use different index drivers)::

   datacube -E new system clone old

Note that the target index is specified with the ``-E`` flag and the source index is provided as an argument to the
``system clone`` command.

Data that cannot be directly copied is skipped, e.g.:

* Non-EO3 compatible data cannot be copied from a ``postgres`` index into a ``postgis`` index.
* External lineage information cannot be copied from a ``postgis`` index to a ``postgres`` index.

The clone command supports the following options:

* ``--skip-lineage`` If set, lineage data is not copied.  Default is to NOT skip lineage (to attempt to copy lineage data)
* ``--lineage-only``  If set, ONLY lineage data is copied.
* ``--batch-size N``  Index cloning is batched for performance. This option specifies the number of records to write to
  the target database at a time.  Default is 1000.

Geospatial search
+++++++++++++++++

Geopolygons for spatial search can be passed to ``dc.load()``, as before::

   dc.load(...., geopolygon=poly, ...)

In the postgres driver, the search is done against a bounding box around the polygon projected into EPSG:4326,
then the extents of datasets returned by the bounding box search are checked for overlap with the original
geopolygon.  In the postgis driver, the polygon is passed directly to Postgis for an indexed spatial search.

* Only datasets whose extents overlap the geopolygon will be loaded.
* Geopolygons whose CRS does NOT have a native spatial index available will be projected to EPSG:4326 for search
  purposes.
* Datasets whose projected extents are not contained within a given CRS's "valid area" will NOT be included in that
  CRS's spatial index.

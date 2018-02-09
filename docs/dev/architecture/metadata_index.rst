.. _dev_arch_index:

Index
=====
Dataset Interface
-----------------
Add/update/archive datasets​
Search for datasets​
Manage locations (i.e. file path)​
Basic stats (Count of datasets)

Implementations
---------------
Postgres
~~~~~~~~
The current implementation uses Postgres 9.5, with JSONB support.

The `metadata_types` are used to create database indexes into the JSONB dataset documents.

Future Possiblities
~~~~~~~~~~~~~~~~~~~

* Spatial:

  * PostGIS - PostgreSQL with optimised geometry datatypes and operations

* Lightweight:

  * SQLite
  * File-based (eg YAML)

* Enterprise:

  * DynamoDB

* Remote:

  * OGC Catalog Service-Web (CS-W)
  * `NASA Common Metadata Repository`_
  * `Radiant Earth Spatial Temporal Assest Metadata`_

.. _`NASA Common Metadata Repository`: https://earthdata.nasa.gov/about/science-system-description/eosdis-components/common-metadata-repository
.. _`Radiant Earth Spatial Temporal Assest Metadata`: https://github.com/radiantearth/stam-spec/blob/dev/abstract-spec.md


Problems
--------
Spatial Support
~~~~~~~~~~~~~~~
Currently the index stores spatial regions for the data, but indexes it on a ranges of latitudes and longitudes.

A database with support for spatial objects, such as the PostGIS extension for Postgres, could improve the efficiency
(and simplicity of implementation) of spatial queries.

Pre-computed Summaries
~~~~~~~~~~~~~~~~~~~~~~
We don't currently store the spatial and temporal extents of a product.

To calculate this in the database requires scanning the entire dataset table to get min and max extent values.
More commonly, to do this in Python, every dataset record is retrieved which is a very memory and CPU intensive
operation.

This is an important feature for apps such as `datacube-wms` and `cubedash` that need to know the entire bounds for
sensibly displaying the user interface.

Replication
~~~~~~~~~~~
Syncing the index across systems (e.g. from NCI to an external system) requires a standard interface.
There are issues using Postgres tools that require locking tables, etc, that need to be investigated.

Publication
~~~~~~~~~~~
There is no standard way to access the `Index` from remote systems.
Directly using the database exposes implementation specifics.
We could possibly use an existing protocol, such as:


  * Various OGC standards, such as CS-W, WCS2-EO or WFS2
  * `NASA Common Metadata Repository`_
  * `Radiant Earth Spatial Temporal Assest Metadata`_

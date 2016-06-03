What's New
==========

v1.1-prerelease Unification (3 June 2016)
-----------------------------------------

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

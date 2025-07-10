ODC Software Packages
=====================

The Open Data Cube consists of a number of packages.  Some of the most important are outlined here.

Core Packages
-------------

ODC Geo
+++++++

`odc-geo`_ provides low level geometry, CRS handling and reprojecting utilities, and the ``.odc``
xarray extensions.

ODC Loader
++++++++++

`odc-loader`_ Provides tools for loading data from files in local or cloud storage into
xarray objects.

(Depends on `odc-geo`_)

ODC STAC
++++++++

`odc-stac`_ supports integrated data discovery from STAC API endpoints and loading of data into
xarray objects.

(Depends on `odc-geo`_ and `odc-loader`_)

Datacube
++++++++

`datacube`_ (aka `datacube-core`_) supports integrated data discovery from a locally-maintained ODC
database, and loading of data into xarray objects.  Also supports administrative collection-management
activities.

(Depends on `odc-geo`_  and `odc-loader`_, but also has its own legacy loading engine)

Publication Services
--------------------

Open Web Services
+++++++++++++++++

`datacube-ows`_ provides WMS, WMTS, and WCS web service endpoints for serving data from one or more
ODC databases, for supplying raw data or rendered visualisations to web map clients (like TerriaJS
or Leaflet) or desktop GIS applications (like QGIS).  Includes a
powerful configurable visualisation library.

See `DEA Maps`_ or `Digital Earth Africa Maps`_ maps for examples of Datacube-OWS in action

(Depends on `odc-geo`_ and `datacube-core`_)

Data Cube Explorer
++++++++++++++++++

`datacube-explorer`_ provides a web-based front end and a STAC-API endpoint for browsing and searching
the contents of an ODC database.

It has rich visualisation abilities to show the
available data extents, and can be used to browse the provenance of indexed data.

See the `Digital Earth Australia Explorer`_ for an example deployment showing the power of this tool.

(Depends on `odc-geo`_ and `datacube-core`_)

Indexing and Packaging Utilities
++++++++++++++++++++++++++++++++

DC Tools
--------

`odc-apps-dc-tools`_ provides a collection of command line tools for indexing large data collections
into an ODC database from various locations, including local file system or S3 buckets. Note that these tools
assume that metadata for the data being indexed is already available, in either eo3 or STAC format.

(Depends on `odc-geo`_ and `datacube-core`_)

EO Datasets
-----------

`eodatasets3`_ provides tools for generating or converting eo3 format metadata and repackaging data
in COG format.

(Depends on `odc-geo`_ and `datacube-core`_)

Scalable Parallel Processing Tools
----------------------------------

ODC Algo
++++++++

`odc-algo`_ is a Python library providing parallelisable EO processing and analysis tools and methods

(Depends on `odc-geo`_ and `datacube-core`_)

ODC Statistician
++++++++++++++++

`odc-stats`_ (aka `Statistician`_) supports cloud-scalable generation of statistical summary products.

(Depends on `odc-geo`_, `datacube-core`_ and `odc-algo`_).

Datacube Alchemist
++++++++++++++++++

`datacube-alchemist`_ supports generation of cloud-scalable generation of derivative products.

It can be used in conjunction with AWS' Simple Queue Service to process very large numbers of datasets,
producing datasets that are packaged completely.

(Depends on `odc-geo`_, `datacube-core`_, `odc-algo`_, `eodatasets3`_ and `odc-apps-dc-tools`_)


.. _odc-geo: https://github.com/opendatacube/odc-geo
.. _odc-loader: https://github.com/opendatacube/odc-loader
.. _odc-stac: https://github.com/opendatacube/odc-stac
.. _datacube: https://github.com/opendatacube/datacube-core
.. _datacube-core: https://github.com/opendatacube/datacube-core
.. _datacube-explorer: https://github.com/opendatacube/datacube-explorer
.. _datacube-ows: https://github.com/opendatacube/datacube-ows
.. _odc-apps-dc-tools: https://github.com/opendatacube/odc-tools/tree/develop/apps/dc_tools
.. _eodatasets3: https://github.com/opendatacube/eo-datasets
.. _odc-algo: https://github.com/opendatacube/odc-algo
.. _Statistician: https://github.com/opendatacube/odc-stats
.. _odc-stats: https://github.com/opendatacube/odc-stats
.. _datacube-alchemist: https://github.com/opendatacube/datacube-alchemist
.. _`DEA Maps`: https://maps.dea.ga.gov.au
.. _`Digital Earth Africa Maps`: https://maps.digitalearth.africa
.. _`Digital Earth Australia Explorer`: https://explorer.sandbox.dea.ga.gov.au/products


.. _datacube-ecosystem:

Data Cube Ecosystem
===================

API Access from Jupyter
-----------------------
One of the most common ways to use Open Data Cube is through interactively
writing Python code within a Jupyter Notebook. This allows dynamically loading
data, performing analysis and developing scientific algorithms.

See :ref:`jupyter-notebooks` for some examples.


Open Web Services
----------------

The datacube-ows_ server allows users to interact with
an Open Data Cube instance using client software
(for example, Desktop GIS like QGIS, or a web mapping library like Leaflet or Terria), 
through the use of Web Map Service (WMS) and Web Coverage Service (WCS) standards from the Open Goespatial Consortium.


.. _datacube-ows: https://github.com/opendatacube/datacube-ows

Examples of this web server can be found in the `Australian National Map`_ service.

.. _`Australian National Map`: https://nationalmap.gov.au/#share=s-jfEZEOkxRXgNsAsHEC6xBddeS1b


Data Cube Explorer
------------------

`Data Cube Explorer`_ is a web application for searching and browsing the metadata
available from an Open Data Cube. It has rich visualisation abilities to show the
available data extents, and can be used to browse the provence of indexed data.

See the `Digital Earth Australia Explorer`_ for an example deployment showing the power of this tool.

.. _`Data Cube Explorer`: https://github.com/opendatacube/datacube-explorer
.. _`Digital Earth Australia Explorer`: https://explorer.sandbox.dea.ga.gov.au


Cube in a Box
-------------

_`Cube in a Box` provides everything needed to get up and running quickly with Open Data Cube inside
an Amazon Web Services Environment.

.. _`Cube in a Box`: https://github.com/opendatacube/cube-in-a-box


Data Cube Alchemist
-------------------

`Data Cube Alchemist`_ is a scene-to-scene data transformation library. It can be run as a command line
application or as a containerised massively-scaleable deployment. It can be used in
conjunction with AWS' Simple Queue Service to process very large numbers of datasets,
producing datasets that are packaged completely.

.. _`Data Cube Alchemist`: https://github.com/opendatacube/datacube-alchemist


Data Cube Statistician
----------------------

_`Data Cube Statistician` is a framework of tools for generating statistical summaries of large collections of Earth Observation Imagery
managed in an Open Datacube Instance. It is a spiritual successor to `datacube-stats`_, but intended to run in a
cloud environment rather than on a High Performance Computer (HPC). It has already run at continental scale to produce annual geomedian
summaries of all of Africa based on Sentinel-2 data. It is still under development, including adding support
for processing sibling products, eg. Water Observations together with Surface Reflectance Quality classifications.

.. _`Data Cube Statistician`: https://github.com/opendatacube/odc-tools/tree/develop/libs/stats


Data Cube Stats
---------------

`Data Cube Statistics`_ is no longer actively maintained, but is an application used to calculate
large scale temporal statistics on data stored using an Open
Data Cube installation. It provides a command line application which uses a YAML configuration file to specify the
data range and statistics to calculate.

.. _`Data Cube Statistics`: https://github.com/opendatacube/datacube-stats


Command line interface
----------------------

Open Data Cube Core includes a powerful command line interface for managing data. It can:

 * Initialise indexes
 * Add products
 * Add datasets
 * Query available data

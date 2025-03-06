============================
Accessing data with odc-stac
============================

.. note::
   This tutorial is under development

Introduction
============

In this tutorial we will use Python libraries to search for freely available 
Sentinel-2 imagery and load it into memory.
You will then create a true-colour image and export it as a cloud-optimised GeoTiff.

During the tutorial, we will:

* Specify our search in terms of:
  
  * what (data provider and satellite)
  * where (area of interest)
  * when (date range)
* Use `pystac-client`_ to connect to a Spatio-Temporal Asset Catalog (STAC) 
  endpoint and search for data matching our what, where, and when
* Use `odc-stac`_ to load the matching data into memory
* Visualise and export the data

There is no need to install anything.
This tutorial runs in an online environment that we have prepared for you. 

Launch tutorial environment
===========================

Right-click on the Binder button below and select "Open Link in New Window" to launch the tutorial environment.
This will allow you to keep the tutorial instructions in view alongside the environment.

The tutorial environment may take a few minutes to start.

.. image:: https://mybinder.org/badge_logo.svg
 :target: https://mybinder.org/v2/gh/opendatacube/tutorial-odc-stac/binder?urlpath=%2Fdoc%2Ftree%2FREADME.md

Once launched, you should see INSERTIMAGE.
The tutorial notebook has headers that match up with the tutorial instructions below.

.. note::
   For this tutorial, we believe you will learn more if you type the code yourself, rather than using copy-paste.
   Typing encourages you to slow down and think about what you're doing, which will help you gain understanding!

Tutorial
========

Python imports
--------------

The notebook requires three libraries to run:

* :code:`geopandas` for loading an area of interest from a file
* :code:`odc.stac` for loading satellite data
* :code:`pystac-client` for querying catalogs of satellite data

Type the following into the empty cell below the **Python imports** heading:

.. code-block:: python

   import geopandas as gpd
   from odc.stac import load
   from pystac_client import Client

When you have finished, run the cell by pressing :code:`Shift+Enter` on your keyboard.
   
Set up query parameters
-----------------------

In this section of the tutorial, you will specify:

* The area you want to load data for
* The date range you want to load data for
* The catalog you want to load data from

Area of interest
^^^^^^^^^^^^^^^^

Type the following into the empty cell below the **Area of interest** heading:

.. code-block:: python

   aoi = gpd.read_file("aoi.geojson")
   aoi_geometry = aoi.iloc[0].geometry

When you have finished, run the cell by pressing :code:`Shift+Enter` on your keyboard.

Date range
^^^^^^^^^^

Type the following into the empty cell below the **Date range** heading:

.. code-block:: python

   start_date = "2021-12-24"
   end_date = "2021-12-26"
   date_query = (start_date, end_date)


When you have finished, run the cell by pressing :code:`Shift+Enter` on your keyboard.

Catalog and measurements
^^^^^^^^^^^^^^^^^^^^^^^^

Type the following into the empty cell below the **Catalog and measurements** heading:

.. code-block:: python

   catalog = "https://earth-search.aws.element84.com/v1/"
   collections_query = ["sentinel-2-l2a"]
   bands_query = ["red", "green", "blue"]

When you have finished, run the cell by pressing :code:`Shift+Enter` on your keyboard.

Connect to catalog and find items
---------------------------------

Search for items
^^^^^^^^^^^^^^^^

Load items with odc-stac
------------------------

Visualise loaded data
---------------------

Export loaded data
------------------


.. _pystac-client: https://pystac-client.readthedocs.io/en/stable/
.. _odc-stac: https://odc-stac.readthedocs.io/en/latest/ 
.. _GitHub: https://github.com/opendatacube/tutorial-odc-stac/tree/main

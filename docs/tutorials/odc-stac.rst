****************************
Accessing data with odc-stac
****************************

.. note::
   This tutorial is under development

Introduction
============

In this tutorial we will use Python libraries to find and load land cover data from the freely available `Impact Observatory Annual Land Use Land Cover <iolulc_>`_ product.
After loading the data, we will export it as a cloud-optimised GeoTiff.
This will allow you to further view or work with the data in GIS software and other tools.

During the tutorial, we will:

* Specify our search in terms of:
  
  * what (data provider and product)
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
 :width: 240px
 :align: center

| Once launched, you should see INSERTIMAGE.

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

We will import either the library, or specific functions and classes from the library.
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
* The data source you want to load from

Area of interest
^^^^^^^^^^^^^^^^

We specify the area of interest using the :code:`aoi.geojson` file, which can be loaded with :code:`geopandas`.

The area of interest is the southern part of Isla Isabela, one of the islands in the Galapagos.

.. image:: ../_static/tutorial-images/odc-stac/aoi.png
 :width: 600
 :alt: A satellite image of Isla Isabela, with the area of interest shown as a yellow bounding box.
 :align: center

| Type the following into the empty cell below the **Area of interest** heading:

.. code-block:: python

   aoi = gpd.read_file("aoi.geojson")
   aoi_geometry = aoi.iloc[0].geometry

When you have finished, run the cell by pressing :code:`Shift+Enter` on your keyboard.

Date range
^^^^^^^^^^

We must specify a start and end date for our query.
Type the following into the empty cell below the **Date range** heading:

.. code-block:: python

   start_date = "2017-01-01"
   end_date = "2023-01-01"
   date_query = (start_date, end_date)


When you have finished, run the cell by pressing :code:`Shift+Enter` on your keyboard.

Catalogs, collections, and items
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Many Earth observation data providers generate STAC metadata, which can be used to find and load data you're interested in.
STAC metadata has four important components:

* **Catalog**: A structure for organising multiple datasets managed by a given provider. For example, `Planetary Computer's Catalog <pc-stac_>`_
* **Collection**: A structure for organising all items in a single dataset. For example, `Land Use Land Cover Collection <pc-lulc_>`_
* **Item** A single spatio-temporal item, such as one observation in a dataset. For example, `Land Use Land Cover Data for Supercell 15M in 2013 <pc-item_>`_
* **Asset** A single data measurement associated with an item, such as a single band. The Land Use Land Cover Dataset has only one asset, called "data".

We must specify the catalog and collection we wish to search, and which assets we want to load. 
The precise items that we need to load will be returned by a query that we run later.

Type the following into the empty cell below the **Catalogs, collections, and items** heading:

.. code-block:: python

   catalog_query = "https://planetarycomputer.microsoft.com/api/stac/v1/"
   collections_query = ["io-lulc-annual-v02"]
   assets_query = ["data"]

When you have finished, run the cell by pressing :code:`Shift+Enter` on your keyboard.

Connect to catalog and find items
---------------------------------

We use `pystac-client`_'s :code:`Client` class to connect to Planetary Computer's STAC catalog.
Type the following into the empty cell below the **Connect to catalog and find items** heading:

.. code-block:: python

   stac_client = Client.open(catalog_query)

When you have finished, run the cell by pressing :code:`Shift+Enter` on your keyboard.

Search for items
^^^^^^^^^^^^^^^^

After setting up the :code:`Client`, we use the :code:`search` method to find items that match our chosen collection, area of interest, and date range.
Type the following into the empty cell below the **Search for items** heading:

.. code-block:: python

   items = stac_client.search(
       collections=collections_query,
       intersects=aoi_geometry,
       datetime=date_query,
   ).item_collection()

   print(f"Found {len(items)} items")

After running the cell, you should see a printed sentence reporting "Found 17 items"

Troubleshooting
"""""""""""""""

If the sentence shows a different number of items, try checking whether your :code:`date_query` parameter is correct by printing it:

.. code-block:: python

   print(date_query)

should return :code:`('2017-01-01', '2023-01-01')`.
If you see a different date range, return to the **Set up query parameters - Date range** section and ensure your :code:`start_date` and :code:`end_date` values match those given in the instructions.

Load items with odc-stac
------------------------

After producing a list of items to load, we can use the :code:`load` function from :code:`odc-stac` to read the requested assets from the items and return them as xarrays.

Visualise loaded data
---------------------

Export loaded data
------------------


.. _pystac-client: https://pystac-client.readthedocs.io/en/stable/
.. _odc-stac: https://odc-stac.readthedocs.io/en/latest/ 
.. _GitHub: https://github.com/opendatacube/tutorial-odc-stac/tree/main
.. _iolulc: https://planetarycomputer.microsoft.com/dataset/io-lulc-annual-v02
.. _pc-stac: https://radiantearth.github.io/stac-browser/#/external/planetarycomputer.microsoft.com/api/stac/v1/
.. _pc-lulc: https://radiantearth.github.io/stac-browser/#/external/planetarycomputer.microsoft.com/api/stac/v1/collections/io-lulc-annual-v02
.. _pc-item: https://radiantearth.github.io/stac-browser/#/external/planetarycomputer.microsoft.com/api/stac/v1/collections/io-lulc-annual-v02/items/15M-2023
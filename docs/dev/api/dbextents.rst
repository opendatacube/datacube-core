

Product Extents Tracking
========================

Example Usage
-------------

.. include:: db_extent_examples.py
   :code: python

Querying API
------------

.. currentmodule:: datacube.index.index

.. automethod:: ProductResource.extent

.. automethod:: ProductResource.extent_periodic

.. automethod:: ProductResource.ranges


Extents Generation and Storage API
----------------------------------

.. currentmodule:: datacube.db_extent

.. autoclass:: ExtentUpload
   :members: store_extent, store_bounds, update_bounds

Database Structure
------------------

.. uml:: product_extent_schema.plantuml
   :caption: Database Tables used for tracking product extents



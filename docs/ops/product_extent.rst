==================
Extents and Bounds
==================

db_extent module provides an api for access and maintenance of extent, extent_meta, and product_bounds tables.
Each extent table record maintains the extent aggregate for a specific product for a specific time period.
The meta information such as what sort of time periods for what duration are stored in extent_meta table.
product_bounds table maintains time bounds and rectangular extent bounds per product.

Fast access to extent record is provided by mapping dataset_type_ref, start, and offset_alias
field combination to extent id as a python UUID.

Retrieval
=========

Monthly and yearly extents are available for retrieval collectively or individually for a given product.
Further the global max and min time bounds as well as rectangular spacial bounds are available for a
given product.

Database upload
===============

Multiprocess based upload functions are available for both extent and bound data uploads.

Keeping Product Extents Up To Date
==================================

Product extents can be updated using the ``datacube product_extents update`` command, which takes
the following options.

   datacube product_extents update --crs EPSG:4326 <PRODUCT_NAME>


In the default configuration, with two period types (Monthly and Yearly).
For our product ls8_nbar_albers:
In `Extent meta` there will be two records, one for each period type. The start and end should match the
times of the first and last datasets available for the product, expanded out to the extents of the period.

The extent table will contain specific geometry for each period for each product.

The ranges table has absolute minimum and max time and absolute rectangular spatial bounds for all
of the datasets in a product.


This tool goes through all the specified products in the list, and updates first the ranges
table for each product, and depending on the new ranges table, updates the extents for each
period.

By default it computes extents for each period from start to end,

datacube product_extents update

datacube product_extents update
datacube product_extents update
datacube product_extents update
datacube product_extents update


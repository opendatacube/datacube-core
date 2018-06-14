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

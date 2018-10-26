
.. _api-reference:

API reference
#############

.. currentmodule:: datacube

.. highlight:: python

For examples on how to use the API, check out the `example Jupyter notebooks
<http://nbviewer.jupyter.org/github/opendatacube/datacube-core/blob/develop/examples/notebooks/Datacube_Summary.ipynb>`_


.. _datacube-class:

Datacube Class
==============

.. autosummary::
   :toctree: generate/

   Datacube

Data Discovery
==============

.. autosummary::
   :nosignatures:
   :toctree: generate/

   Datacube.list_products
   Datacube.list_measurements


Data Loading
============

.. autosummary::
   :nosignatures:
   :toctree: generate/

   Datacube.load

Internal Loading Functions
--------------------------

This operations can be useful if you need to customise the loading process,
for example, to pre-filter the available datasets before loading.

.. currentmodule:: datacube

.. autosummary::
   :toctree: generate/

   Datacube.find_datasets
   Datacube.group_datasets
   Datacube.load_data
   Datacube.measurement_data

.. _grid-workflow-class:

Grid Processing API
===================

.. currentmodule:: datacube.api

.. autosummary::
   :toctree: generate/

   GridWorkflow
   grid_workflow.Tile
   GridWorkflow.list_cells
   GridWorkflow.list_tiles
   GridWorkflow.load

Grid Processing API Internals
-----------------------------

.. currentmodule:: datacube.api

.. autosummary::
   :toctree: generate/

   GridWorkflow.cell_observations
   GridWorkflow.group_into_cells
   GridWorkflow.tile_sources

Internal Data Model
===================

.. currentmodule:: datacube.model

.. autosummary::
   :toctree: generate/

   Dataset
   Measurement
   MetadataType
   DatasetType
   GridSpec

   Range
   Variable
   CellIndex



Database Index API
==================

Dataset Querying
----------------

When connected to an ODC Database, these methods are available for
searching and querying:

.. code-block:: python

   dc = Datacube()
   dc.index.datasets.{method}

.. currentmodule:: datacube.index._datasets.DatasetResource

.. autosummary::
   :nosignatures:
   :toctree: generate/

   get
   search
   search_by_metadata
   search_by_product
   search_eager
   search_product_duplicates
   search_returning
   search_summaries
   has
   bulk_has
   can_update
   count
   count_by_product
   count_by_product_through_time
   count_product_through_time
   get_derived
   get_field_names
   get_locations
   get_archived_locations
   get_datasets_for_location


Dataset Writing
---------------

When connected to an ODC Database, these methods are available for
adding, updating and archiving datasets::

   dc = Datacube()
   dc.index.datasets.{method}

.. autosummary::
   :nosignatures:
   :toctree: generate/

   add
   add_location
   archive
   archive_location
   remove_location
   restore
   restore_location
   update

Product Querying
----------------

When connected to an ODC Database, these methods are available for
discovering information about Products::

   dc = Datacube()
   dc.index.products.{method}

.. currentmodule:: datacube.index._products.ProductResource

.. autosummary::
   :nosignatures:
   :toctree: generate/

   from_doc
   add
   can_update
   add_document
   get
   get_by_name
   get_unsafe
   get_by_name_unsafe
   get_with_fields
   search
   search_robust
   get_all

Product Addition/Modification
-----------------------------

When connected to an ODC Database, these methods are available for
discovering information about Products::

   dc = Datacube()
   dc.index.products.{method}

.. currentmodule:: datacube.index._products.ProductResource

.. autosummary::
   :nosignatures:
   :toctree: generate/

   from_doc
   add
   update
   update_document
   add_document

Database Index Connections
--------------------------

.. currentmodule:: datacube

.. autosummary::
   :nosignatures:
   :toctree: generate/

   index.index_connect
   index.Index

Dataset to Product Matching
---------------------------

.. currentmodule:: datacube.index.hl

.. autosummary::
   :nosignatures:
   :toctree: generate/

   Doc2Dataset

Geometry Utilities
==================

.. currentmodule:: datacube

Open Data Cube includes a set of CRS aware geometry utilities.

Geometry Classes
----------------

.. autosummary::
   :nosignatures:
   :toctree: generate/

   utils.geometry.BoundingBox
   utils.geometry.CRS
   utils.geometry.Geometry
   model.GridSpec

.. Geometry.contains
   Geometry.crosses
   Geometry.disjoint
   Geometry.intersects
   Geometry.touches
   Geometry.within
   Geometry.overlaps
   Geometry.difference
   Geometry.intersection
   Geometry.symmetric_difference
   Geometry.union
   Geometry.type
   Geometry.is_empty
   Geometry.is_valid
   Geometry.boundary
   Geometry.centroid
   Geometry.coords
   Geometry.points
   Geometry.length
   Geometry.area
   Geometry.convex_hull
   Geometry.envelope
   Geometry.boundingbox
   Geometry.wkt
   Geometry.json


Creating Geometries
-------------------

.. currentmodule:: datacube.utils.geometry

.. autosummary::
   :toctree: generate/

   point
   multipoint
   line
   multiline
   polygon
   multipolygon
   box
   polygon_from_transform


Multi-geometry ops
------------------

.. autosummary::
   :toctree: generate/

   unary_union
   unary_intersection


Masking
=======

.. toctree::
   :maxdepth: 1

   masking.rst

.. currentmodule:: datacube.storage

.. autosummary::

   masking.mask_invalid_data
   masking.describe_variable_flags
   masking.make_mask

Query Class
===========

.. currentmodule:: datacube.api.query

.. autosummary::
   :toctree: generate/

   Query


User Configuration
==================

.. currentmodule:: datacube.config
.. autosummary::
  :toctree: generate/

  LocalConfig
  DEFAULT_CONF_PATHS

Everything Else
===============

.. toctree::
   :maxdepth: 1

   analytics_engine.rst
   s3block.rst


.. toctree::
   :hidden:

   external.rst



For **Exploratory Data Analysis** see :ref:`datacube-class` for more details

For **Writing Large Scale Workflows** see :ref:`grid-workflow-class` for more details
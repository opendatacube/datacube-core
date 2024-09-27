================
Product Querying
================
When connected to an ODC Database, these methods are available for discovering information about Products:

.. code-block:: python

   dc = Datacube()
   dc.index.products.{method}


.. currentmodule:: datacube.index.abstract.AbstractProductResource

.. autosummary::
   :toctree: generate
   :nosignatures:

   can_update
   get
   get_by_name
   get_with_fields
   search
   search_robust
   search_by_metadata
   get_all

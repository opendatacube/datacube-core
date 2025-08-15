================
Product Querying
================
When connected to an ODC Database, these methods are available for discovering information about Products:

.. code-block:: bash

   dc = Datacube()
   dc.index.products.{method}


.. currentmodule:: datacube.index.abstract.AbstractProductResource

.. autosummary::
   :nosignatures:
   :toctree: generate/

   can_update
   get
   get_by_name
   get_with_fields
   search
   search_robust
   search_by_metadata
   get_all

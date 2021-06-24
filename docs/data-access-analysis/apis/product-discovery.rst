
.. currentmodule:: datacube

Product Discovery
==============

Product Listings
----------------
Listings provide access to all ``products`` and ``measurements`` in the datacube.

.. autosummary::
   :toctree: generate/

   Datacube.list_products
   Datacube.list_measurements

Product Querying
----------------
Querying provides a way to discover information about ``products``, such as a collection of landsat 8 scenes, in the datacube.

.. code-block:: python

   dc = Datacube()
   dc.index.products.{method}


.. currentmodule:: datacube.index._products.ProductResource

.. autosummary::
   :toctree: generate/

   can_update
   get
   get_by_name
   get_unsafe
   get_by_name_unsafe
   get_with_fields
   search
   search_robust
   get_all

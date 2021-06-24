Dataset Searching & Querying
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

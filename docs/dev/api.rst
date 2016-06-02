Data Access API
===============

For examples on how to use the API, see the Jupyter notebooks at:
https://github.com/data-cube/agdc-v2/tree/develop/examples/notebooks


.. currentmodule:: datacube.api

Datacube Class
--------------


Create Accessor
~~~~~~~~~~~~~~~

.. autosummary::

   Datacube.__init__


Search Available Data
~~~~~~~~~~~~~~~~~~~~~

.. autosummary::

   Datacube.products
   Datacube.variables
   Datacube.list_variables


   Datacube.get_dataset
   Datacube.get_data_array
   Datacube.product_observations
   Datacube.product_sources
   Datacube.product_data
   Datacube.variable_data
   Datacube.variable_data_lazy


Class Definition
----------------

.. autoclass:: datacube.api.API
  :special-members: __init__
  :members:

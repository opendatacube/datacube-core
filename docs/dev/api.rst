Data Access API
===============

For examples on how to use the API, see the Jupyter notebooks at:
http://nbviewer.jupyter.org/github/data-cube/agdc-v2/blob/unification/examples/notebooks/The%20Unified%20Datacube%20Notebook.ipynb


.. currentmodule:: datacube.api

Datacube Class
--------------


.. autosummary::

   Datacube.__init__


Core Functions
~~~~~~~~~~~~~~

.. autosummary::
   :toctree: generate/

   Datacube.products
   Datacube.measurements
   Datacube.load


Advanced Functions
~~~~~~~~~~~~~~~~~~

.. autosummary::
   :toctree: generate/

   Datacube.product_data
   Datacube.product_observations
   Datacube.product_sources


   Datacube.measurement_data
   Datacube.measurement_data_lazy

   Datacube.list_measurements



Analytics Engine API
--------------------

.. autoclass:: datacube.api.API
  :special-members: __init__
  :members:


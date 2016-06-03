Data Access API
===============

For examples on how to use the API, see the Jupyter notebooks at:
http://nbviewer.jupyter.org/github/data-cube/agdc-v2/blob/unification/examples/notebooks/The%20Unified%20Datacube%20Notebook.ipynb


.. currentmodule:: datacube

.. _datacube-class:

Datacube Class
--------------

.. autosummary::
   :toctree: generate/

   Datacube.__init__


Core Functions
~~~~~~~~~~~~~~

.. autosummary::
   :toctree: generate/

   Datacube.list_products
   Datacube.list_measurements
   Datacube.load


Advanced Functions
~~~~~~~~~~~~~~~~~~

.. autosummary::
   :toctree: generate/

   Datacube.product_observations
   Datacube.product_sources
   Datacube.product_data

   Datacube.measurement_data
   Datacube.measurement_data_lazy


GridWorkflow Class
------------------

.. currentmodule:: datacube.api
.. autosummary::
   :toctree: generate/

   GridWorkflow.__init__


API for Analytics and Execution Engine
--------------------------------------

.. currentmodule:: datacube.api
.. autosummary::
   :toctree: generate/

    API.__init__
    API.list_products
    API.list_variables
    API.get_descriptor
    API.get_data


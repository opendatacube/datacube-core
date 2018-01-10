Loading Data from a Cube
========================

For examples on how to use the API, check out the `example Jupyter notebooks
<http://nbviewer.jupyter.org/github/opendatacube/datacube-core/blob/develop/examples/notebooks/Datacube_Summary.ipynb>`_


.. currentmodule:: datacube

.. _datacube-class:

Datacube Class
--------------

.. autosummary::
   :toctree: generate/

   Datacube


Higher Level User Functions
~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autosummary::
   :toctree: generate/

   Datacube.list_products
   Datacube.list_measurements
   Datacube.load


Low-Level Internal Functions
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autosummary::
   :toctree: generate/

   Datacube.find_datasets
   Datacube.group_datasets
   Datacube.load_data

   Datacube.measurement_data

.. _grid-workflow-class:

GridWorkflow Class
------------------

.. currentmodule:: datacube.api

.. autosummary::
   :toctree: generate/

   GridWorkflow
   grid_workflow.Tile


Higher Level User Functions
~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autosummary::
   :toctree: generate/

   GridWorkflow.list_cells
   GridWorkflow.list_tiles
   GridWorkflow.load


Low-Level Internal Functions
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autosummary::
   :toctree: generate/

   GridWorkflow.cell_observations
   GridWorkflow.group_into_cells
   GridWorkflow.tile_sources



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



.. _query-class:

Query Class
-----------

.. currentmodule:: datacube.api.query

.. autosummary::
   :toctree: generate/

   Query


User Configuration
------------------
.. currentmodule:: datacube.config
.. autosummary::
  :toctree: generate/

  LocalConfig
  DEFAULT_CONF_PATHS

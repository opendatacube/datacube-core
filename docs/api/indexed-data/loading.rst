============
Data Loading
============

.. currentmodule:: datacube

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


.. include:: ./../../ops/load_3d_dataset.rst


Group by
---------

.. currentmodule:: datacube.api.query

.. autosummary::

   :toctree: generate/

   query_group_by

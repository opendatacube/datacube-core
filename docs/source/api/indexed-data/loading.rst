============
Data Loading
============

.. currentmodule:: datacube

.. autosummary::
   :nosignatures:

   Datacube.load


Internal Loading Functions
--------------------------

This operations can be useful if you need to customise the loading process,
for example, to pre-filter the available datasets before loading.

.. currentmodule:: datacube

.. autosummary::

   Datacube.find_datasets
   Datacube.group_datasets
   Datacube.load_data


Group by
---------

.. currentmodule:: datacube.api.query

.. autosummary::
   :toctree: generate/

   query_group_by
   solar_day
   GroupBy

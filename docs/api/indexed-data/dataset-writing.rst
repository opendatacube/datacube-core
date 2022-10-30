===============
Dataset Writing
===============
When connected to an ODC Database, these methods are available for adding, updating and archiving datasets:

.. code-block:: bash

   dc = Datacube()
   dc.index.datasets.{method}



.. currentmodule:: datacube.index.abstract.AbstractDatasetResource

.. autosummary::

   :toctree: generate/

   add
   add_location
   archive
   archive_location
   remove_location
   restore
   restore_location
   update

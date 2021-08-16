Dataset Searching & Querying
----------------------------

``Datasets`` indexed within a Data Cube can be searched for using a variety of methods.

Minimal Example
~~~~~~~~~~~~~~~
.. code-block:: python

   dc = Datacube()
   ds = dc.index.datasets.get(1234)

API
~~~
The following methods are available on ``dc.index.datasets``.

.. code-block:: python

   dc = Datacube()
   ds = dc.index.datasets.{method}


Data Loading
============
Once you've know the ``products`` or ``datasets`` that you are interested in you will need to load the data.

Minimal Example
~~~~~~~~~~~~~~~~

.. code-block:: python

    import datacube

    dc = datacube.Datacube(app="my_analysis")

    ds = dc.load(product="ls8_nbart_geomedian_annual",
                 x=(153.3, 153.4),
                 y=(-27.5, -27.6),
                 time=("2015-01-01", "2015-12-31"))


API
~~~~~~~~~~~~~~~~
.. currentmodule:: datacube

.. autosummary::
   :toctree: generate/

   Datacube.load

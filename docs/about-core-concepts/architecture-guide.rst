Architectural Overview
======================

The modern paradigm that the Open Data Cube (ODC) operates under is the Cloud Native Geospatial
concept, where data is available over vast areas and via HTTP, generally from
`Cloud Optimised GeoTIFFs`_ (COGs). COGs, coupled with `SpatioTemporal Asset Catalog`_ metadata
is being used by Digital Earth Australia, Digital Earth Africa, Element 84's Sentinel-2 COGs,
USGS's Landsat Collection 2, Planet and a wide range of other organisations.

The ODC can index from STAC, although this process is not fully integrated, it is
done in production. An example of this is captured in these `Sentinel-2 Indexing notes`_. And
in addition to indexing from STAC, `Datacube Explorer`_ can present ODC metadata as STAC
documents through a STAC API.

The key design constraint that the ODC currently has is it's reliance on a direct
PostgreSQL connection, so one should consider how others in their team will access
the database that they are indexing into.

In general, any data format that can be read by RasterIO (GDAL, fundamentally), can
be indexed into the ODC, so long as it can be described by ODC metadata.

This section of the documentation describes the structure of the ODC and the key
components that make up an implementation, as well as the ecosystem around it.

.. _`Cloud Optimised GeoTIFFs`: https://www.cogeo.org/
.. _`SpatioTemporal Asset Catalog`: https://stacspec.org/
.. _`Sentinel-2 Indexing notes`: https://github.com/opendatacube/datacube-dataset-config/blob/master/sentinel-2-l2a-cogs.md
.. _`Datacube Explorer`: https://github.com/opendatacube/datacube-explorer

Product Definitions
*******************

A product definition defines what a dataset must look like and provides
load hints to Datacube, as well as basic metadata about a product.

The ``metadata`` section of a product definition is used to automatically match
the product to a dataset. The simplest product definition is included below.
This example uses only one measurement (equivalent to an ``asset`` in STAC)
and some very basic information about the product

.. code-block:: yaml

    ---
    name: dem_srtm
    metadata_type: eo3

    metadata:
        product:
        name: dem_srtm

    measurements:
        - name: elevation
        dtype: int16
        nodata: -32768.0
        units: "metre"

A slightly more complex product definition is shown below. This example uses
the ``storage`` section to provide load hints, so that default parameters are
known when loading data.

.. code-block:: yaml

    ---
    name: dem_srtm
    description: 1 second elevation model
    metadata_type: eo3

    license: CC-BY-4.0

    metadata:
        product:
        name: dem_srtm

    storage:
        crs: EPSG:4326
        resolution:
        longitude: 0.000277777777780
        latitude: -0.000277777777780

    measurements:
        - name: elevation
        dtype: int16
        nodata: -32768.0
        units: "metre"


You can add product definitions using the command line as follows: ``datacube product add <path-to-file>``
and you can update them using ``datacube product update <path-to-file>``.

A tool exists that can help you keep products in sync between a CSV list of products and the ODC
dataset. See the `datacube-product-sync <https://github.com/opendatacube/odc-tools/blob/develop/apps/dc_tools/README.md#dc-sync-products>`_ tool.

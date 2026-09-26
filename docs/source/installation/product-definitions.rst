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
        tile_size:
          x: 100000.0
          y: 100000.0
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

For a detailed description of the format of a valid product document, refer to the `formal specification`_.

Global datasets (PostGIS)
=========================

For products where **every dataset covers the entire globe**, set the optional
top-level boolean ``global_datasets: true`` in the product definition. It defaults
to ``false``. This is an assertion by the product administrator; the index does
not verify global coverage.

The PostGIS index omits the spatial predicate and spatial-index join for these
products when searching or counting datasets. Product, metadata and archive
filters still apply. In queries covering multiple products, only products with
the flag enabled skip spatial filtering. Products without the flag keep their
existing behaviour. Other index drivers do not use this optimisation.

Do not enable the flag for tiled, regional, near-global or mixed-coverage
products: doing so can return datasets that do not intersect the requested area.
Loading and clipping raster data are unchanged. The optimisation does not remove
global datasets from spatial indexes or improve searches of other products that
still use those indexes.

Changing this flag on an existing product requires the usual explicit approval
for an unsafe product update, because it changes spatial-query semantics.

.. _`formal specification`: https://github.com/opendatacube/eo3/blob/develop/SPECIFICATION-odc-product.md

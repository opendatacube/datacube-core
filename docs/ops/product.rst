.. _product-doc:

Product Definition
******************

A product definition document describes the measurements and common metadata
for a collection of datasets.

.. highlight:: language

.. literalinclude:: ../config_samples/dataset_types/landsat8_example_product.yaml
   :language: yaml

name
    Product name

description
    Product description

metadata_type
    Name of the :ref:`metadata-type-definition`

license
    The license of the data.

    This is either a SPDX License identifier (eg 'CC-BY-SA-4.0') or
    'various' or 'proprietary'

metadata
    Dictionary containing bits of metadata common to all the datasets in the product.

    It is used during indexing to match datasets to their products. That is, the keys and values defined
    here must also be in the :ref:`dataset-metadata-doc`.

    In the above example, ``product: name`` would match a specific product.

storage (optional)
    Describes some of common storage attributes of all the datasets. While optional defining this will make
    product data easier to access and use. This only applies to products that have data arranged on a regular
    grid, for example ingested products are like that.

    crs
        Coordinate reference system common to all the datasets in the product. ``'EPSG:<code>'`` or WKT string.

    resolution
        Resolution of the data of all the datasets in the product specified in projection units.
        Use ``latitude``, ``longitude`` if the projection is geographic and ``x``, ``y`` otherwise.

    tile_size
        Size of the tiles for the data to be stored in specified in projection units. Use ``latitude`` and ``longitude``
        if the projection is geographic, otherwise use ``x`` and ``y``.

    origin
        Coordinates of the bottom-left or top-left corner of the ``(0,0)`` tile specified in projection units. If
        coordinates are for top-left corner, ensure that the ``latitude`` or ``y`` dimension of ``tile_size`` is
        negative so tile indexes count downward. Use ``latitude`` and ``longitude`` if the projection is geographic,
        otherwise use ``x`` and ``y``.


measurements
    List of measurements in this product. The measurement names defined here need to match 1:1 with the measurement
    key names defined in the :ref:`dataset-metadata-doc`.

    name
         Name of the measurement

    units
         Units of the measurement

    dtype
         Data type. One of ``(u)int(8,16,32,64), float32, float64``

    nodata
         No data value

    scale_factor,add_offset (optional)
         Mapping from pixel value to real value ``real = scale_factor*pixel_value + add_offset``.

    spectral_definition (optional)
         Spectral response of the reflectance measurement.

         .. code-block:: yaml

             spectral_definition:
                  wavelength: [410, 411, 412]
                  response: [0.0261, 0.029, 0.0318]

    flags_definition (optional)
        Bit flag meanings of the bitset 'measurement'

        .. code-block:: yaml

            flags_definition:
                platform:
                  bits: [0,1,2,3]
                  description: Platform name
                  values:
                    0: terra
                    1: aqua_terra
                    2: aqua
                contiguous:
                  bits: 8
                  description: All bands for this pixel contain non-null values
                  values: {0: false, 1: true}

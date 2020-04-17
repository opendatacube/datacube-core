.. _product-doc:

Product Definition
******************

A product definition document describes the measurements and common metadata
for a collection of datasets.

.. highlight:: language

.. literalinclude:: ../config_samples/dataset_types/dsm1sv10.yaml
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

    It is used during indexing to match datasets to their products.

storage (optional)
    Describes some of common storage attributes of all the datasets. While optional defining this will make
    product data easier to access and use.

    crs
        Coordinate reference system common to all the datasets in the product. 'EPSG:<code>' or WKT string.

    resolution
        Resolution of the data of all the datasets in the product specified in projection units.
        Use ``latitude``, ``longitude`` if the projection is geographic and ``x``, ``y`` otherwise

measurements
    List of measurements in this product

    name
         Name of the measurement

    units
         Units of the measurement

    dtype
         Data type. One of ``(u)int(8,16,32,64), float32, float64``

    nodata
         No data value

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

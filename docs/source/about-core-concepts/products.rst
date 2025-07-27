
.. _Products:

Products
========

Products are collections of ``datasets`` that share the same set of measurements and some subset of metadata.


.. _product-definitions:

Product Definition
******************

A product definition document describes the measurements and common metadata
for a collection of datasets.

.. note::

   "Products" were historically originally known as "Dataset types" and you may see this terminology lingering
   in some places in documentation or the code base.

Example Product Definition
~~~~~~~~~~~~~~~~~~~~~~~~~~

.. highlight:: language

.. literalinclude:: ../config_samples/dataset_types/landsat8_example_product.yaml
   :language: yaml


Product Definition Format Specification
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

For a detailed description of the format of a valid product document, refer to the
`formal specification`_ in the eo3 Github repository.

.. _`formal specification`: https://github.com/opendatacube/eo3/blob/develop/SPECIFICATION-odc-product.md

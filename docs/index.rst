

..
   .. image:: _static/odc-logo-horizontal.svg
      :align: center
      :alt: Open Data Cube Logo

%%%%%%%%%%%%%%%%%%%%%%%%%%%
   Open Data Cube Manual
%%%%%%%%%%%%%%%%%%%%%%%%%%%

The Open Data Cube provides an integrated gridded data
analysis environment for decades of analysis ready earth observation satellite
and  related data from multiple satellite and other acquisition systems.

..
    The ReadTheDocs theme has a problem where it's seemingly impossible to have
    both nice sidebar navigation, and a nice breadcrumb trail at the top of the
    page.

    The breadcrumbs depend on having multi-level toctree, where documents that
    are included in this top level, contain their own sub-trees of documents.

    However, I can't find any way to have these subtrees displayed globally in
    the sidebar navigation. The subtrees are only displayed when you navigate to
    their parent page.


.. toctree::

   user/intro.rst
   user/deployments.rst
   user/notebooks.rst
   user/code_recipes.rst

   user/ecosystem.rst


.. toctree::
   :maxdepth: 2
   :caption: Installation and Data Loading

   ops/overview.rst
   ops/datasets.rst
   ops/install.rst
   ops/db_setup.rst
   ops/config.rst
   ops/indexing.rst
   ops/ingest.rst
   ops/product.rst
   ops/dataset_documents.rst
   ops/prepare_scripts.rst
   ops/tools.rst
   ops/replication.rst

.. toctree::
   :maxdepth: 2
   :caption: Developer Guide

   dev/api/index.rst
   dev/setup/index.rst
   dev/api/virtual-products.rst
   architecture/index.rst
   about/release_process.rst

.. toctree::
   :maxdepth: 2
   :caption: About Data Cube

   about/whats_new.rst
   about/glossary.rst
   about/license.rst



.. _support:

Support
=======

The best way to get help with the Data Cube is to open an issue on Github_.

.. _Github: https://github.com/opendatacube/datacube-core/issues


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

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
   :caption: User Guide

   user/intro.rst
   user/guide.rst
   user/config.rst



.. toctree::
   :maxdepth: 2
   :caption: Operations Guide

   ops/overview.rst
   ops/install.rst
   ops/db_setup.rst
   ops/indexing.rst
   ops/ingest.rst
   ops/config.rst
   ops/prepare_scripts.rst
   ops/tools.rst
   ops/replication.rst

.. toctree::
   :maxdepth: 2
   :caption: Developer Guide

   dev/developer.rst
   dev/setup/index.rst
   architecture/index.rst

.. toctree::
   :maxdepth: 2
   :caption: About Data Cube

   about/whats_new
   about/glossary
   about/release_process
   about/license


.. Kept to maintain any old hyperlinks.
.. toctree::
   :hidden:

   user/nci_usage

.. _support:

Support
=======

The best way to get help with the Data Cube is to open an issue on Github_.

.. _Github: http://github.com/opendatacube/datacube-core/issues


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

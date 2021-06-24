Introduction
=============================

Once you have the Data Cube software installed and connected to a database, you can start to load in some data. This step is performed using the datacube command line tool.

.. admonition:: Note
  :class: important

  When you load data into the Data Cube using indexing, all you are doing is recording the existence of and detailed metadata about the data into the index. None of the data itself is copied, moved or transformed. This is therefore a relatively safe and fast process.


Steps to Indexing Data
=============================
 * Create a new product
    Before the data itself can be added, a product describing the data must be created.
    Requires creation of a :ref:`product-definitions` document (yaml)

 * Index the data
    After this step the data is accessible through the datacube.
    Requires datacube friendly :ref:`dataset-documents` for data which is to be indexed

 * Ingest the data (OPTIONAL - Not Recommended in most circumstances)
    After indexing the data you can choose to ingest the data. This provides the ability to tile the original data into a faster storage format or a new projection system.
    This step requires creation of an ingestion configuration file (yaml).

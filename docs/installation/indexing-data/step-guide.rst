Step-by-step Guide to Indexing Data
===================================

Introduction
*************

Once you have the Data Cube software installed and connected to a database, you can start to load in some data. This step is performed using the datacube command line tool.

.. note:: 

  When you load data into the Data Cube using indexing, all you are doing is recording the existence of and detailed metadata about the data into the index. None of the data itself is copied, moved or transformed. This is therefore a relatively safe and fast process.

Steps Overview
**************

1. Create a new product definition
    Before the data itself can be added, a product describing the data must be created.
    Requires creation of a :ref:`product-definitions` document (yaml)

2. Ensure the data is prepared
    The data to be indexed requires datacube friendly :ref:`dataset-documents` for data which is to be indexed

3. Index the data
    Run the actual indexing process

4. (OPTIONAL) :ref:`Ingest <ingestion>` the data
    After indexing the data you can choose to ingest. This provides the ability to tile the original data into a faster storage format or a new projection system.
    Requires creation of an ingestion configuration file (yaml). This is not recommended.


Step 1. Creating a Product Definition
****************************************

The ODC can handle many different types of data, and requires a bit of
information up front to know what to do with them. This is the task of a
Product Definition.

More detailed information on the structure of a product definition document can be found :ref:`here <product-doc>`

Some example Product definitions are supplied `here <https://github.com/opendatacube/datacube-dataset-config/tree/master/products>`_.
Other examples include the `Digital Earth Africa product definitions <https://github.com/digitalearthafrica/config/tree/master/products>`_.


Loading Product Definitions
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To load Products into your ODC run:

.. code-block:: bash

    datacube product add <path-to-product-definition-yml>

If you made a mistake, you can update them with:

.. code-block:: bash

    datacube product update <path-to-product-definition-yml>


Step 2. Ensure Dataset Documents are complete
*********************************************

Every dataset that you intend to index requires a metadata document describing what the data represents and where it has come
from, as well has what format it is stored in. At a minimum, you need the dimensions or fields your want to
search by, such as lat, lon and time, but you can include any information you deem useful.

It is typically stored in YAML documents, but JSON is also supported. It is stored in the index
for searching, querying and accessing the data.

The data from Geoscience Australia already comes with relevant files (named ``ga-metadata.yaml``), so
no further steps are required for indexing them.

For third party datasets, see :ref:`prepare-scripts`.


.. note::

    Some metadata requires cleanup before they are ready to be loaded.

For more information see :ref:`dataset-metadata-doc`.


Step 3. Run the Indexing process
********************************

Everything is now ready, and we can use the **datacube** tool to add one or more
datasets into our Cube

.. code-block:: bash

    datacube dataset add <path-to-dataset-document-yaml>

Note that this path can be a URI, such as the path to a document on S3.

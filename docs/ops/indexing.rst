.. highlight:: console

.. _indexing:

Indexing Data
*************

Once you have the ODC software installed and connected to a database, you
can start to load in some data. This step is performed using the **datacube**
command line tool.

When you load data into the ODC, all you are doing is recording the
existence of and detailed metadata about the data into the **index**. None of
the data itself is copied, moved or transformed. This is therefore a relatively
safe and fast process.


Prerequisites for Indexing Data
===============================

 * A working ODC setup
 * Some data to load (Links to some freely available :ref:`sample-eo-data`.)


Steps to Adding Data
====================

 * Create a new product
    Before the data itself can be added, a product describing the data must be created.
    Requires creation of a :ref:`product-definitions` document (yaml)

 * Index the data
    After this step the data is accessible through the datacube.
    Requires datacube friendly :ref:`dataset-documents` for data which is to be indexed

 * (OPTIONAL) :ref:`Ingest <ingestion>` the data
    After indexing the data you can choose to ingest. This provides the ability to tile the original data into a faster storage format or a new projection system.
    Requires creation of an ingestion configuration file (yaml). This is not recommended.


.. _product-definitions:

Product Definition
==================

The ODC can handle many different types of data, and requires a bit of
information up front to know what to do with them. This is the task of a
Product Definition.

A Product Definition provides a short **name**, a **description**, some basic
source **metadata** and (optionally) a list of **measurements** describing the
type of data that will be contained in the Datasets of its type. In Landsat Surface
Reflectance, for example, the measurements are the list of bands.

The **measurements** is an ordered list of data, which specify a **name** and
some **aliases**, a data type or **dtype**, and some options extras including
what type of **units** the measurement is in, a **nodata** value, and even a way
of specifying **bit level descriptions** or the **spectral response** in the
case of reflectance data.

A product definition example:

.. code-block:: yaml

    name: ls8_level1_scene
    description: Landsat 8 Level 1 Collection-1

    metadata_type: eo3

    license: CC-BY-4.0

    metadata:
        # only match datasets with sub-tree `{"product": {"name": "ls8_level1_scene"}}` present
        product:
          name: ls8_level1_scene

    measurements:
        - name: 'coastal_aerosol'
          aliases: [band_1, coastal_aerosol]
          dtype: int16
          nodata: -999
          units: '1'

        - name: 'blue'
          aliases: [band_2, blue]
          dtype: int16
          nodata: -999
          units: '1'

        - name: 'green'
          aliases: [band_3, green]
          dtype: int16
          nodata: -999
          units: '1'

        - name: 'red'
          aliases: [band_4, red]
          dtype: int16
          nodata: -999
          units: '1'

        - name: 'nir'
          aliases: [band_5, nir]
          dtype: int16
          nodata: -999
          units: '1'

        - name: 'swir1'
          aliases: [band_6, swir1]
          dtype: int16
          nodata: -999
          units: '1'

        - name: 'swir2'
          aliases: [band_7, swir2]
          dtype: int16
          nodata: -999
          units: '1'

More detailed information on the structure of a product definition document can be found :ref:`here <product-doc>`

Some example Product definitions are supplied `here <https://github.com/opendatacube/datacube-dataset-config/tree/master/products>`_.
Other examples include the `Digital Earth Africa product definitions <https://github.com/digitalearthafrica/config/tree/master/products>`_.


Loading Product Definitions
===========================

To load Products into your ODC run:

  datacube product add <path-to-product-definition-yml>

If you made a mistake, you can update them with:

  datacube product update <path-to-product-definition-yml>

.. _dataset-documents:

Dataset Documents
=================

Every dataset requires a metadata document describing what the data represents and where it has come
from, as well has what format it is stored in. At a minimum, you need the dimensions or fields you want to
search by, such as lat, lon and time, but you can include any information you deem useful.

It is typically stored in YAML documents, but JSON is also supported. It is stored in the index
for searching, querying and accessing the data.

The data from Geoscience Australia already comes with relevant files (named ``ga-metadata.yaml``), so
no further steps are required for indexing them.

For third party datasets, see :ref:`prepare-scripts`.

A :ref:`dataset-metadata-doc` is required to accompany the dataset for it to be
recognised by the ODC. It defines critical metadata of the dataset such as:

    - measurements
    - platform and sensor names
    - geospatial extents and projection
    - acquisition time

.. note::

    Some metadata requires cleanup before they are ready to be loaded.

For more information see :ref:`dataset-metadata-doc`.


Adding Data - Indexing
======================

Everything is now ready, and we can use the **datacube** tool to add one or more
datasets into our Cube by running::


    datacube dataset add <path-to-dataset-document-yaml>

Note that this path can be a URI, such as the path to a document on S3.


.. _sample-eo-data:


Indexing Data on Amazon(AWS S3)
===============================

Options currently exist that allow for a user to store, index, and retrieve data
from cloud object stores, such as Amazon S3 buckets, using the open ODC.
The following sections outline this process.

Configuring AWS CLI Credentials
-------------------------------

Install the AWS CLI package and configure it with your Amazon AWS credentials.
For a more detailed tutorial on AWS CLI configurations, visit the
`official AWS docs <https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-install.html>`_.
The only two fields required to be configured are the ``Access Key``, and
``Secret Access Key``. These keys can be found on your AWS login
security page. Try not to lose your ``Secret Access Key`` as you will
not be able to view it again and you will have to request a new one.


Install Indexing Scripts
-------------------------

In order to utilize the convenience of S3 indexing, we must install
some tools that will help make it easier and faster. You can find the code
and further detailed documentation for the tools used below in the 
`odc-tools <https://github.com/opendatacube/odc-tools/tree/develop/apps/dc_tools>`_ repository.

Install the tools like this:

  pip install --extra-index-url="https://packages.dea.ga.gov.au" odc_apps_dc_tools

S3 Indexing Example
-----------------------------

For this example we will be indexing Digital Earth Australia's public data bucket,
which you can browse at `data.dea.ga.gov.au <https://data.dea.ga.gov.au>`_.

Run the two lines below, the first will add the product definition for the Landsat
Geomedian product and the second will add all of the Geomedian datasets. This will
take some time, but will add a continental product to your local Datacube.

.. code-block:: bash

  datacube product add https://data.dea.ga.gov.au/geomedian-australia/v2.1.0/product-definition.yaml
  s3-to-dc --no-sign-request 's3://dea-public-data/geomedian-australia/v2.1.0/L8/**/*.yaml' ls8_nbart_geomedian_annual

Now that you've 

.. code-block:: python

    import datacube

    dc = datacube.Datacube()

    ds = dc.load(
      product="ls8_nbart_geomedian_annual",
      limit=1,
      lon=(146.5, 146.7),
      lat=(-43.5, -43.7),
      time="2020"
    )

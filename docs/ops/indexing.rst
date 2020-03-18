.. highlight:: console

.. _indexing:

Indexing Data
*************

Once you have the Data Cube software installed and connected to a database, you
can start to load in some data. This step is performed using the **datacube**
command line tool.

When you load data into the Data Cube, all you are doing is recording the
existence of and detailed metadata about the data into the **index**. None of
the data itself is copied, moved or transformed. This is therefore a relatively
safe and fast process.


Prerequisites for Indexing Data
===============================

 * A working Data Cube setup
 * Some data to load (Links to some freely available :ref:`sample-eo-data`.)



Steps to Adding Data
====================

 * Create a new product
     Before the data itself can be added, a product describing the data must be created.
     Requires creation of a :ref:`product-definitions` document (yaml)

 * Index the data
     After this step the data is accessible through the datacube.
     Requires datacube friendly :ref:`dataset-documents` for data which is to be indexed

 * :ref:`Ingest <ingestion>` the data(optional)
     After indexing the data you can choose to ingest. This provides the ability to tile the original data into a faster storage format or a new projection system.
     Requires creation of an ingestion configuration file (yaml).


.. _product-definitions:

Product Definition
==================

The Data Cube can handle many different types of data, and requires a bit of
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
     metadata_type: eo

     metadata:
         platform:
             code: LANDSAT_8
         instrument:
             name: OLI_TIRS
         product_type: level1
         format:
             name: GeoTIFF
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

A set of Product definitions are supplied `here <https://github.com/opendatacube/datacube-core/tree/develop/docs/config_samples/dataset_types>`_ to cover some common Geoscience Australia and other Earth Observation Data.


Loading Product Definitions
===========================

To load Products into your Data Cube run::

    datacube product add <path-to-product-definition-yml>


.. _dataset-documents:

Dataset Documents
=================

Every dataset requires a metadata document describing what the data represents and where it has come
from, as well has what format it is stored in. At a minimum, you need the dimensions or fields your want to
search by, such as lat, lon and time, but you can include any information you deem useful.

It is typically stored in YAML documents, but JSON is also supported. It is stored in the index
for searching, querying and accessing the data.

The data from Geoscience Australia already comes with relevant files (named ``ga-metadata.yaml``), so
no further steps are required for indexing them.

For third party datasets, see :ref:`prepare-scripts`.

A :ref:`dataset-metadata-doc` is required to accompany the dataset for it to be
recognised by the Data Cube. It defines critical metadata of the dataset such as:

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


    datacube dataset add --auto-match <path-to-dataset-document-yaml>



.. _sample-eo-data:

Sample Earth Observation Data
-----------------------------

The U.S. Geological Survey provides many freely available, Analysis Ready,
earth observation data products. The following are a good place to start
looking.

* Landsat
    * `USGS Landsat Surface Reflectance - LEDAPS and LaSRC available via ESPA 30m`__
* MODIS
    * `MCD43A1 - BRDF-Albedo Model Parameters 16-Day L3 Global 500m`__
    * `MCD43A2 - BRDF-Albedo Quality 16-Day L3 Global 500m`__
    * `MCD43A3 - Albedo 16-Day L3 Global 500m`__
    * `MCD43A4 - Nadir BRDF-Adjusted Reflectance 16-Day L3 Global 500m`__

__ https://espa.cr.usgs.gov/
__ https://lpdaac.usgs.gov/dataset_discovery/modis/modis_products_table/mcd43a1_v006
__ https://lpdaac.usgs.gov/dataset_discovery/modis/modis_products_table/mcd43a2_v006
__ https://lpdaac.usgs.gov/dataset_discovery/modis/modis_products_table/mcd43a3_v006
__ https://lpdaac.usgs.gov/dataset_discovery/modis/modis_products_table/mcd43a4_v006

Once you have downloaded some data, it will need :ref:`metadata preparation
<prepare-scripts>` before use in the Data Cube.


Indexing Data on Amazon(AWS S3)
===============================

Cloud storage is a sought after feature for most storage platforms. Options currently exist that allow for a users to store, index, and retrieve data from Amazon S3 buckets using the open data cube. The following sections outline this process.  

Configuring AWS CLI Credentials
-------------------------------

Install the AWS CLI package and configure it with your Amazon AWS credentials. For a more detailed tutorial on AWS CLI configurations, visit the official AWS docs  The
only two fields required to be configured are the ``Access Key``, and
``Secret Access Key``. These keys can be found on your AWS login
security page. Try not to lose your ``Secret Access Key`` as you will
not be able to view it again and you will have to request a new one.

.. code-block:: bash

    pip install boto3 ruamel.yaml 
    sudo apt-get install awscli -y
    aws configure

Add the ca-certificates requisite for S3 indexing and export them to the
environment variable the data cube will look for. If you forget this
step you will see an error upon attempting to load the indexed dataset.

.. code-block:: bash

    sudo apt-get install ca-certificates
    export CURL_CA_BUNDLE=/etc/ssl/certs/ca-certificates.crt

You may want to add the line
``export CURL_CA_BUNDLE=/etc/ssl/certs/ca-certificates.crt`` to your
``.bashrc`` file to make these changes permanent.


Download Indexing Scripts
-------------------------


In order to utilize the convenience of S3 indexing, we must retrieve
scripts necessary for S3 indexing. The direct links are provided below
since, at the time of this document, they are not all included in the
latest release (1.6.1).

.. code-block:: bash

    cd ~/Datacube
    mkdir -p S3_scripts
    cd S3_scripts
    wget https://raw.githubusercontent.com/opendatacube/datacube-core/develop/datacube/index/hl.py
    wget https://raw.githubusercontent.com/opendatacube/datacube-dataset-config/master/scripts/index_from_s3_bucket.py
    wget https://raw.githubusercontent.com/opendatacube/datacube-core/develop/docs/config_samples/dataset_types/ls_usgs.yaml

Once the necessary scripts have been gathered, it is time to install the
AWS CLI package and configure it with your Amazon AWS credentials. The
only two fields required to be configured are the ``Access Key``, and
``Secret Access Key``. These keys can be found on your AWS login
security page. Try not to lose your ``Secret Access Key`` as you will
not be able to view it again and you will have to request a new one.

.. code-block:: bash

    pip install boto3 ruamel.yaml 
    sudo apt-get install awscli -y
    aws configure

Add the ca-certificates requisite for S3 indexing and export them to the
environment variable the data cube will look for. If you forget this
step you will see an error upon attempting to load the indexed dataset.

.. code-block:: bash

    sudo apt-get install ca-certificates
    export CURL_CA_BUNDLE=/etc/ssl/certs/ca-certificates.crt

You may want to add the line
``export CURL_CA_BUNDLE=/etc/ssl/certs/ca-certificates.crt`` to your
``.bashrc`` file to make these changes permanent.

S3 Indexing Example
-----------------------------

For this example we will be indexing from Amazon AWS' ``landsat-pds``.
This dataset is constantly updated and is free for use. It contains an
incredible amount of Landsat 8 data downloaded directly from USGS and
hosted on their public S3 bucket. More information can be found here:
https://registry.opendata.aws/landsat-8/.

Add a product that matches the metadata for the data found on the S3
bucket. If using a different dataset, you may have to use or create a
``yaml`` product definition file if an exact match is not readily
available.

.. code-block:: bash

    datacube product add ~/Datacube/S3_scripts/ls_usgs.yaml

This is an example of indexing an S3 dataset from AWS' landsat-pds.
Notice how ``MTL.txt`` is the file that is parsed to index the dataset.
``-p`` is the option for the path of the directory from the landsat-pds
main directory. ``--suffix`` refers to the suffix of the metadata file
to process, it will not always be an ``MTL.txt`` but for landsat-pds, it
will be.

.. code-block:: bash

    cd ~/Datacube/S3_scripts
    python3 index_from_s3_bucket.py landsat-pds -p c1/L8/139/045/ --suffix="MTL.txt"

This is an example that works with the command above to illustrate the
Python usage. The ``dc.load`` would just use bounds defined within the
data that was indexed. ``output_crs`` and ``resolution`` will be
required for this command to work. These commands will need to be
entered into a notebook or in a Python console, accessed with the
command ``python``

.. code-block:: python

    import datacube

    dc = datacube.Datacube()

    ds = dc.load("ls8_level1_usgs",
                 output_crs="EPSG:3857",
                 resolution=(-30, 30),
                 lat=(21,21.2),
                 lon=(86.7, 86.9))

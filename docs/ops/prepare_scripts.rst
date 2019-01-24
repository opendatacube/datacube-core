.. highlight:: bash

.. _prepare-scripts:

Dataset Preparation Scripts
***************************

Some data you may want to load into your Data Cube will come pre-packaged with a
dataset-description document and is ready to be :ref:`indexed/loaded <indexing>`
immediately.

In many other cases the data you want to load into your Data Cube will not have
these description documents. Before loading them will need to generate them,
using a tool which understands the format the dataset is in. Several of these
tools are provided in  :file:`utils/` in the source repository.

The two examples below shows USGS landsat data for ingestion into the Data cube:


A specific example for USGS collection 1 MTL format is :download:`USGS Landsat Prepare Script
<../../utils/ls_usgs_prepare.py>`


1. Preparing USGS Landsat Collection 1 - LEVEL1
===============================================

Download the USGS Collection 1 landsat scenes from any of the links below:

* `Earth-Explorer <https://earthexplorer.usgs.gov>`_
* `GloVis <https://glovis.usgs.gov>`_
* `ESPA ordering <https://espa.cr.usgs.gov>`_

The prepare script for collection 1 - level 1 data is provided in :file:`utils/ls_usgs_prepare.py`

::

    $ python utils/ls_usgs_prepare.py --help
    Usage: ls_usgs_prepare.py [OPTIONS] [DATASETS]...

        Prepare USGS Landsat Collection 1 data for ingestion into the Data Cube.
        This prepare script supports only for MTL.txt metadata file
        To Set the Path for referring the datasets -
        Download the  Landsat scene data from Earth Explorer or GloVis into
        'some_space_available_folder' and unpack the file.
        For example: yourscript.py --output [Yaml- which writes datasets into this file for indexing]
        [Path for dataset as : /home/some_space_available_folder/]

    Options:
        --output PATH  Write datasets into this file
        --help         Show this message and exit.

    $ python utils/ls_usgs_prepare.py --output ls8_usgs_lv1 ~/earth_explorer/Collection1/LANDSAT8

*ls8_usgs_lv1* is the output for required dataset for landsat 8 scene.

To add the product definitions:

For Landsat collection 1 level 1 product:

::

    $ datacube product add docs/config_samples/dataset_types/ls_usgs.yaml
    Added "ls8_level1_usgs"
    Added "ls7_level1_usgs"
    Added "ls5_level1_usgs"
    Added "ls8_l1_pc_usgs"


An another example for USGS landsat surface reflectance :download:`USGS Landsat LEDAPS
<../../utils/USGS_precollection_oldscripts/ls_usgs_ard_prepare.py>`

2. Preparing USGS Landsat Surface Reflectance - LEDAPS
======================================================

To prepare downloaded USGS LEDAPS Landsat scenes for use with the Data Cube,
use the script provided in :file:`utils/USGS_precollection_oldscripts/ls_usgs_ard_prepare.py`.

The following example generates the required Dataset Metadata files, named
`agdc-metadata.yaml` for three landsat scenes.

::

    $ python utils/USGS_precollection_oldscripts/usgslsprepare.py --help
    Usage: usgslsprepare.py [OPTIONS] [DATASETS]...

      Prepare USGS LS dataset for ingestion into the Data Cube.

    Options:
      --help  Show this message and exit.

    $ python utils/usgslsprepare.py ~/USGS_LandsatLEDAPS/*/
    2016-06-09 15:32:51,641 INFO Processing ~/USGS_LandsatLEDAPS/LC80960852015365-SC20160211222236
    2016-06-09 15:32:52,096 INFO Writing ~/USGS_LandsatLEDAPS/LC80960852015365-SC20160211222236/agdc-metadata.yaml
    2016-06-09 15:32:52,119 INFO Processing ~/USGS_LandsatLEDAPS/LE70960852016024-SC20160211221824
    2016-06-09 15:32:52,137 INFO Writing ~/USGS_LandsatLEDAPS/LE70960852016024-SC20160211221824/agdc-metadata.yaml
    2016-06-09 15:32:52,151 INFO Processing ~/USGS_LandsatLEDAPS/LT50960852011290-SC20160211221617
    2016-06-09 15:32:52,157 INFO Writing ~/USGS_LandsatLEDAPS/LT50960852011290-SC20160211221617/agdc-metadata.yaml


The scenes are now ready to be :ref:`indexed <indexing>` and accessed using
the Data Cube.

For Landsat Surface reflectance LEDAPS add:

::

    $ datacube product add docs/config_samples/dataset_types/*
    ...
    Added "ls5_ledaps_scene"
    ...
    Added "ls7_ledaps_scene"
    ...
    Added "ls8_ledaps_scene"
    ...

Then :ref:`index the data <indexing>`.

3. Prepare script and indexing Landsat data on AWS
==================================================

Landsat 8 data is available to use on Amazon S3 without needing to worry about the download of all scenes from
the start of imagery capture.

Landsat on AWS makes each band of each Landsat scene available as a stand-alone GeoTIFF and
the scenes metadata is hosted as a text file.

About the data::

     Source              -                                         USGS and NASA
     Category            -                            GIS, Sensor Data, Satellite Imagery, Natural Resource
     Format              -                                         GeoTIFF, txt, jpg
     Storage Service     -                                            Amazon S3
     Location            -                                s3://landsat-pds in US West (Oregon) Region
     Update Frequency    -                       New Landsat 8 scenes are added regularly as soon as they are available

Each scene's directory includes:

* a .TIF GeoTIFF for each of the scenes up to 12 bands (note that the GeoTIFFs include 512x512 internal tiling)
* .TIF.ovr overview file for each .TIF (useful in GDAL based applications)
* a _MTL.txt metadata file
* a small rgb preview jpeg, 3 percent of the original size
* a larger rgb preview jpeg, 15 percent of the original size
* an index.html file that can be viewed in a browser to see the RGB preview and links to the GeoTIFFs and metadata files

Accessing data on AWS
---------------------

The data are organized using a directory structure based on each scene's path and row.
For instance, the files for Landsat scene LC08_L1TP_139045_20170304_20170316_01_T1 are available in the following location:

..

s3://landsat-pds/c1/L8/139/045/LC08_L1TP_139045_20170304_20170316_01_T1/

> The `c1` refers to Collection 1, the `L8` refers to Landsat 8, `139` refers to the scene's path,
`045` refers to the scene's row, and the final directory matches the product's identifier,
which uses the following naming convention: LXSS_LLLL_PPPRRR_YYYYMMDD_yyymmdd_CC_TX, in which:

| L = Landsat
| X = Sensor
| SS = Satellite
| PPP = WRS path
| RRR = WRS row
| YYYYMMDD = Acquisition date
| yyyymmdd = Processing date
| CC = Collection number
| TX = Collection category
| In this case, the scene corresponds to WRS path 139, WRS row 045, and was taken on March 4th, 2017.The full scene list is available here_.

.. _here: https://landsat-pds.s3.amazonaws.com/c1/L8/scene_list.gz


The prepare script to index Landsat AWS data :download:`ls_public_bucket.py <../../utils/ls_public_bucket.py>`

Instead of downloading all the scenes, the following prepare script helps to directly
index the metadata available on S3 using the script :file:`utils/ls_public_bucket.py`

Usage of the script:
::

     $python ls_public_bucket.py --help
     Usage: ls_public_bucket.py [OPTIONS] BUCKET_NAME

        Enter Bucket name. Optional to enter configuration file to access a
        different database

     Options:
        -c, --config PATH  Pass the configuration file to access the database
        -p TEXT            Pass the prefix of the object to the bucket
        --help             Show this message and exit.


An example to use the script:
..

    `$python ls_public_bucket.py landsat-pds -p c1/139/045/`

where `landsat-pds` is the amazon public bucket name, `c1` refers to collection 1 and the numbers after represents the
WRS path and row.

Index any path and row by changing the prefix in the above command

Before indexing:
----------------

..

      1. You will need an AWS account and configure AWS credentials to access the data on S3 bucket

            For more detailed information refer amazon-docs_.

.. _amazon-docs: https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/credentials.html

::

        [default]
        aws_access_key_id = <Access key ID>
        aws_secret_access_key = <Secret access key>

..

      2. Add the product definition to datacube

             Sample product definition for LANDSAT_8 Colletcion 1 Level1 data is
             available at :file:`docs/config_samples/dataset_types/ls_sample_product.yaml`

::

        $ datacube product add ls_sample_product.yaml

          Added "ls8_level1_scene"


Custom Prepare Scripts
======================

We expect that many new Data Cube instances will require custom prepare scripts
to be written. It is generally a straightforward task of mapping metadata from
one form to another and writing out a YAML document. The code need not even be
written in Python, although starting with one of our examples is generally
the easiest way.

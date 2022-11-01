Data Preperation Scripts
========================

.. note::

  Much of the below content is not updated and has not been tested recently.


Sometimes data to load into an Open Data Cube will come packaged with
compatible :ref:`dataset-metadata-doc` and be ready to :ref:`index <indexing>`
immediately.

In other cases you will need to generate these :ref:`dataset-metadata-doc` yourself.
This is done using a ``Dataset Preparation Script``, which reads whatever format metadata
has been supplied with the data, and either writes out ODC compatible files, or adds
records directly to an ODC database.

For common distribution formats there is likely to be a script already, but
other formats will require one to be modified.

.. admonition:: Existing dataset-metadata-docs
   :class: tip

   Examples of prepare scripts are found in the `datacube-dataset-config <https://github.com/opendatacube/datacube-dataset-config>`_ repository
   on Github.


Landsat Samples
===============

The two examples below show preparing USGS Landsat data for indexing into an Open Data Cube:


1. Preparing USGS Landsat Collection 1 - LEVEL1
===============================================

Download the USGS Collection 1 landsat scenes from any of the links below:

* `Earth-Explorer <https://earthexplorer.usgs.gov>`_
* `GloVis <https://glovis.usgs.gov>`_
* `ESPA ordering <https://espa.cr.usgs.gov>`_

The prepare script for collection 1 - level 1 data is available in
`ls_usgs_prepare.py
<https://github.com/opendatacube/datacube-dataset-config/blob/master/old-prep-scripts/ls_usgs_prepare.py>`_.

::

    $ wget https://github.com/opendatacube/datacube-dataset-config/raw/master/old-prep-scripts/ls_usgs_prepare.py
    $ python ls_usgs_prepare.py --help
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

    $ python ls_usgs_prepare.py --output ls8_usgs_lv1 ~/earth_explorer/Collection1/LANDSAT8

*ls8_usgs_lv1* is the output for required dataset for landsat 8 scene.

To add the product definitions:

For Landsat collection 1 level 1 product:

::

    $ datacube product add docs/config_samples/dataset_types/ls_usgs.yaml
    Added "ls8_level1_usgs"
    Added "ls7_level1_usgs"
    Added "ls5_level1_usgs"
    Added "ls8_l1_pc_usgs"


2. Preparing USGS Landsat Surface Reflectance - LEDAPS
======================================================

To prepare downloaded USGS LEDAPS Landsat scenes for use with the Data Cube, use
the script provided in
`usgs_ls_ard_prepare.py
<https://github.com/opendatacube/datacube-dataset-config/blob/master/agdcv2-ingest/prepare_scripts/landsat_collection/usgs_ls_ard_prepare.py>`_

The following example generates the required Dataset Metadata files, named
`agdc-metadata.yaml` for three landsat scenes.

::

    $ wget https://github.com/opendatacube/datacube-dataset-config/raw/master/agdcv2-ingest/prepare_scripts/landsat_collection/usgs_ls_ard_prepare.py
    $ python USGS_precollection_oldscripts/usgslsprepare.py --help
    Usage: usgslsprepare.py [OPTIONS] [DATASETS]...

        Prepare USGS LS dataset for ingestion into the Data Cube.

    Options:
        --help  Show this message and exit.

    $ python usgslsprepare.py ~/USGS_LandsatLEDAPS/*/
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

3. Indexing data on AWS, an example using Sentinel-2
====================================================

To view an example of how to `index Sentinel-2 data from S3`_ check out the documentation
available in the datacube-dataset-config_ repository.

.. _`index Sentinel-2 data from S3`: https://github.com/opendatacube/datacube-dataset-config/blob/master/sentinel-2-l2a-cogs.md
.. _datacube-dataset-config: https://github.com/opendatacube/datacube-dataset-config/

Custom Prepare Scripts
======================

We expect that many new Data Cube instances will require custom prepare scripts
to be written. It is generally a straightforward task of mapping metadata from
one form to another and writing out a YAML document. The code need not even be
written in Python, although starting with one of our examples is generally
the easiest way.

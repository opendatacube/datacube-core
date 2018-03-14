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

The two examples below shows USGS landsat data for ingestion into the Data cube.

#. A specific example for USGS collection 1 MTL format is :download:`USGS Landsat Prepare Script
<../../utils/ls_usgs_prepare.py>`


Preparing USGS Landsat Collection 1 - LEVEL1
============================================

Download the USGS Collection 1 landsat scenes from any of the link below:

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



#. An another example for USGS landsat surface reflectance :download:`USGS Landsat LEDAPS
<../../utils/USGS_precollection_oldscripts/ls_usgs_ard_prepare.py>`

Preparing USGS Landsat Surface Reflectance - LEDAPS
===================================================

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

Custom Prepare Scripts
======================

We expect that many new Data Cube instances will require custom prepare scripts
to be written. It is generally a straightforward task of mapping metadata from
one form to another and writing out a YAML document. The code need not even be
written in Python, although starting with one of our examples is generally
the easiest way.

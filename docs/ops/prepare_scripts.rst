.. _prepare-scripts:

Dataset Preparation Scripts
===========================

Some data you may want to load into your Data Cube will come pre-packaged with a dataset-description document and is ready to be :ref:`loaded <indexing>` immediately.

In many other cases the data you want to load into your Data Cube will not have these description documents. Before loading them will need to generate them, using a tool which understands the format the dataset is in. Several of these tools are provided in  ``utils/`` in the source repository.

A specific example is the :download:`USGS Landsat Prepare Script <../../utils/usgslsprepare.py>`


Using the USGS Landsat LEDAPS Prepare Script
--------------------------------------------

To prepare downloaded USGS LEDAPS Landsat scenes for use with the Data Cube,
use the script provided in ``utils/usgslsprepare.py``.

The following example generates the required Dataset Metadata files, named
`agdc-metadata.yaml` for three landsat scenes.

::

    $ python utils/usgslsprepare.py --help
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

First add the product definitions::

    $ datacube product add docs/config_samples/dataset_types/*
    ...
    Added "ls5_ledaps_scene"
    ...
    Added "ls7_ledaps_scene"
    ...
    Added "ls8_ledaps_scene"
    ...

Then index the data.

Custom Prepare Scripts
----------------------

We expect that many new datacube will require custom prepare scripts to be written. It is generally a straightforward task of mapping metadata from one form to another and writing out a YAML document. The code need not even be written in Python, although starting with one of the examples is recommended.

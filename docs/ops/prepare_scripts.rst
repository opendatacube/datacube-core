.. _prepare-scripts:

Dataset Preparation Scripts
===========================

Some data you may want to load into your Data Cube will come pre-packaged with a dataset-description document and is ready to be :ref:`loaded <indexing>` immediately.

In many other cases the data you want to load into your Data Cube will not have these description documents. Before loading them will need to generate them, using a tool which understands the format the dataset is in. Several of these tools are provided in  ``utils/`` in the source repository.

A specific example is the :download:`USGS Landsat Prepare Script <../../utils/usgslsprepare.py>`


Custom Prepare Scripts
----------------------

We expect that many new datacube will require custom prepare scripts to be written. It is generally a straightforward task of mapping metadata from one form to another and writing out a YAML document. The code need not even be written in Python, although starting with one of the examples is recommended.

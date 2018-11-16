.. _introduction:

Introduction
############

Overview
========


The Open Data Cube is a collection of software designed to:

* Catalogue large amounts of Earth Observation data
* Provide a :term:`Python` based :term:`API` for high performance querying and data access
* Give scientists and other users easy ability to perform Exploratory Data Analysis
* Allow scalable continent scale processing of the stored data
* Track the provenance of all the contained data to allow for quality control and updates

The Open Data Cube software is based around the datacube-core_ library.


datacube-core_ is an open source Python library, released under the `Apache 2.0
<https://github.com/opendatacube/datacube-core/blob/develop/LICENSE>`_ license.

.. _datacube-core: https://github.com/opendatacube/datacube-core

Use Cases
=========

Large-scale workflows on HPC
----------------------------
Continent or global-scale processing of data on a High Performance Computing supercomputer cluster.

Exploratory Data Analysis
-------------------------
Allows interactive analysis of data, such as through a Jupyter Notebook.

Cloud-based Services
--------------------
Using :abbr:`ODC (Open Data Cube)` to serve :abbr:`WMS (Web Map Service)`, :abbr:`WCS (Web Coverage Service)`, or custom
tools (such as polygon drill time series analysis.

Standalone Applications
-----------------------
Running environmental analysis applications on a laptop, suitable for field work, or outreach to a developing region.
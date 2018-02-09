.. _dev_arch:

Introduction
============

The Open Data Cube community is based around the `datacube-core` library.
This document describes the architecture of the library and the ecosystem of systems and applications it interacts with.

`datacube-core` is an open source Python library, released under the `Apache 2.0
<https://github.com/opendatacube/datacube-core/blob/develop/LICENSE>`_ license.

Use Cases
---------
Large-scale workflows on HPC
~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Continent or global-scale processing of data on a High Performance Computing supercomputer cluster.

Exploratory Data Analysis
~~~~~~~~~~~~~~~~~~~~~~~~~
Allows interactive analysis of data, such as through a Jupyter Notebook.

Cloud-based Services
~~~~~~~~~~~~~~~~~~~~
Using ODC to serve WMS (Web Map Service), WCS (Web Coverage Service), or custom tools (such as polygon drill time series
analysis.

Standalone Applications
~~~~~~~~~~~~~~~~~~~~~~~
Running environmental analysis applications on a laptop, suitable for field work, or outreach to a developing region.

Assumptions and Design Constraints
----------------------------------
On a HPC system, the resources to provided to host the database may limited.
During execution of a task across many compute nodes, the database should not be relied upon to serve concurrent access
from all of the compute nodes.

The system must be able to support some particular mandated file and metadata formats on some platforms.
E.g. NCI requires data be NetCDF-CF compliant.

3rd-party data can be accessed without being manipulated or reformatted.

Data of differing resolutions and projections can be used together.
E.g. Landsat-MODIS blending.




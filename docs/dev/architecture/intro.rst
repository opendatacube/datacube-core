.. _dev_arch:

====================
 Architecture Guide
====================

Architecture Introduction
=========================
Use Cases
---------
Large-scale workflows on HPC
~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Continent or global-scale processing of data on a High Performance Computing supercomputer cluster.

Exploratory Data Analysis
~~~~~~~~~~~~~~~~~~~~~~~~~


Cloud-based Services
~~~~~~~~~~~~~~~~~~~~


Standalone Applications
~~~~~~~~~~~~~~~~~~~~~~~
Running environmental analysis applications on a laptop, suitable for field work or outreach to a developing region.


Assumptions and Design Constraints
----------------------------------
On a HPC system, the resources to provided to host the database may limited.
During execution of a task across many compute nodes, the database should not be relied upon to serve concurrent access
from all of the compute nodes.

The system must be able to support some particular mandated file and metadata formats on some platforms.
E.g. NCI requires data be NetCDF-CF compliant.

Data Model
==========
Datasets
--------

Products
--------

Metadata Types
--------------


API
===
Datacube Load
-------------
Find Datasets
~~~~~~~~~~~~~

Group Datasets
~~~~~~~~~~~~~~

Load Data
~~~~~~~~~

GridWorkflow
------------

Index
=====
Interface
---------


Implimentations
---------------
Postgres
~~~~~~~~

Future Possiblities
~~~~~~~~~~~~~~~~~~~

* Lightweight:

  * SQLite
  * File-based (eg YAML)

* Remote:

  * `NASA Common Metadata Repository`_

.. _`NASA Common Metadata Repository`: https://earthdata.nasa.gov/about/science-system-description/eosdis-components/common-metadata-repository

Storage
=======

Applications
============


Data Management
===============

Ingester
--------

Stacker
-------

Sync Tool
---------

CubeDash
--------


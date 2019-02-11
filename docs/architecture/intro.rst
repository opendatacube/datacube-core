.. _dev_arch:

Introduction
************


Assumptions and Design Constraints
==================================

On a :abbr:`HPC (High Performance Computing)` system, the resources to provided to host the database may limited.
During execution of a task across many compute nodes, the database should not be relied upon to serve concurrent access
from all of the compute nodes.

The system must be able to support some particular mandated file and metadata formats on some platforms.
E.g. :term:`NCI` requires data be NetCDF-CF compliant.

3rd-party data can be accessed without being manipulated or reformatted.

Data of differing resolutions and projections can be used together.
E.g. :term:`Landsat`-:term:`MODIS` blending.




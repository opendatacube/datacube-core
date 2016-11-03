Australian Geoscience Data Cube v2
==================================

|Build Status| |Coverage Status| |Documentation Status|

Overview
========

The Australian Geoscience Data Cube provides an integrated gridded data
analysis environment for decades of analysis ready earth observation
satellite and related data from multiple satellite and other acquisition
systems.

In 2014, Geoscience Australia, CSIRO and the NCI established the
Australian Geoscience Data Cube, building on earlier work of Geoscience
Australia and expanding it to include additional earth observation
satellite and other gridded data collections (e.g. MODIS, DEM) in order
to expand the range of integrated data analysis capabilities that were
available. The complete software stack and petabytes of EO are deployed
at the NCI petascale computing facility for use by NCI users.

**The current AGDC v2 implementation is intended as a working
prototype** for a cohesive, sustainable framework for large-scale
multidimensional data management for geoscientific data. This public
development release is intended to foster broader collaboration on the
design and implementation. It is not intended for operational use.

Documentation
=============

See the `user guide <http://agdc-v2.readthedocs.org/en/develop/>`__ for
installation & usage of the datacube, and for documentation of the API.

Requirements
============

System
~~~~~~

-  PostgreSQL 9.5+
-  Python 2.7+ or Python 3.5+

Developer setup
===============

1. Clone:

   -  ``git clone https://github.com/data-cube/agdc-v2.git``

2. Install the native libraries for `GDAL <http://www.gdal.org/>`__ &
   NetCDF4.

   -  This depends on your OS.
   -  Eg. ``yum install gdal``

3. Install Python dependencies:

   ``python setup.py develop``

   Note that the versions must match between GDAL's Python bindings and
   the native GDAL library. If you receive a gdal error when installing
   dependencies, you may need to install a specific version first:

   eg. ``pip install gdal==2.0.1``

4. Run unit tests + PyLint

   ``./check-code.sh``

   (this script approximates what is run by Travis. You can
   alternatively run ``py.test`` yourself)

5. **(or)** Run all tests, including integration tests.

``./check-code.sh integration_tests``

-  Assumes a password-less Postgres database running on localhost called
   ``agdcintegration``

   -  Otherwise copy ``integration_tests/agdcintegration.conf`` to
      ``~/.datacube_integration.conf`` and edit to customise.

.. |Build Status| image:: https://travis-ci.org/data-cube/agdc-v2.svg?branch=develop
   :target: https://travis-ci.org/data-cube/agdc-v2
.. |Coverage Status| image:: https://coveralls.io/repos/data-cube/agdc-v2/badge.svg?branch=develop&service=github
   :target: https://coveralls.io/github/data-cube/agdc-v2?branch=develop
.. |Documentation Status| image:: https://readthedocs.org/projects/agdc-v2/badge/?version=develop
   :target: http://agdc-v2.readthedocs.org/en/develop/

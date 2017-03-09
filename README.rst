Open Data Cube Core
==================================

|Build Status| |Coverage Status| |Documentation Status|

Overview
========

Open Data Cube Core provides an integrated gridded data
analysis environment for decades of analysis ready earth observation
satellite and related data from multiple satellite and other acquisition
systems.

Documentation
=============

See the `user guide <http://datacube-core.readthedocs.io/en/latest/>`__ for
installation & usage of the datacube, and for documentation of the API.

`Join our Slack <https://opendatacube.signup.team/>`__ if you need help
setting up or using Data Cube Core.

Requirements
============

System
~~~~~~

-  PostgreSQL 9.5+
-  Python 2.7+ or Python 3.5+

Developer setup
===============

1. Clone:

   -  ``git clone https://github.com/opendatacube/datacube-core.git``

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

.. |Build Status| image:: https://travis-ci.org/opendatacube/datacube-core.svg?branch=develop
   :target: https://travis-ci.org/opendatacube/datacube-core
.. |Coverage Status| image:: https://coveralls.io/repos/opendatacube/datacube-core/badge.svg?branch=develop&service=github
   :target: https://coveralls.io/github/opendatacube/datacube-core?branch=develop
.. |Documentation Status| image:: https://readthedocs.org/projects/datacube-core/badge/?version=latest
   :target: http://datacube-core.readthedocs.org/en/latest/

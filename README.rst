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

`Join our Slack <http://slack.opendatacube.org>`__ if you need help
setting up or using Data Cube Core.

Requirements
============

System
~~~~~~

-  PostgreSQL 9.5+
-  Python Python 3.5+

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

Docker
======

Docker for Open Data Cube is in the early stages of development, and more documentation and examples of how 
to use it will be forthcoming soon. For now, you can build and run this Docker image from 
this repository as documented below.

Example Usage
~~~~~~~~~~~~~
There are a number of environment variables in use that can be used to configure the OpenDataCube.
Some of these are built into the application itself, and others are specific to Docker, and will 
be used to create a configuration file when the container is launched.

You can build the image with a command like this: 

``docker build --tag opendatacube:local .``

And it can then be run with this command:

``docker run --rm opendatacube:local``

If you don't need to build (and you shouldn't) then you can run it from a pre-built image with:

``docker run --rm opendatacube/datacube-core``

An example of starting a container with environment variables is as follows:

.. code-block:: bash
   
   docker run \
      --rm \
      -e DATACUBE_CONFIG_PATH=/opt/custom-config.conf \
      -e DB_DATABASE=mycube \
      -e DB_HOSTNAME=localhost \
      -e DB_USERNAME=postgres \
      -e DB_PASSWORD=secretpassword \
      -e DB_PORT=5432 \
      opendatacube/datacube-core


Additionally, you can run an Open Data Cube Docker container along with Postgres using the Docker Compose file.
For example, you can run ``docker-compose up`` and it will start up the Postgres server and Open Data Cube next to it. 
To run commands in ODC, you can use ``docker-compose run odc datacube -v system init`` or ``docker-compose run odc datacube --version``.


Environment Variables
~~~~~~~~~~~~~~~~~~~~~
Most of the below environment variables should be self explanatory, and none are required (although
it is recommended that you set them).

- ``DATACUBE_CONFIG_PATH`` - the path for the config file for writing (also used by ODC for reading)
- ``DB_DATABASE`` - the name of the postgres database
- ``DB_HOSTNAME`` - the hostname of the postgres database
- ``DB_USERNAME`` - the username of the postgres database
- ``DB_PASSWORD`` - the password to used for the postgres database
- ``DB_PORT`` - the port that the postgres database is exposed on


.. |Build Status| image:: https://travis-ci.org/opendatacube/datacube-core.svg?branch=develop
   :target: https://travis-ci.org/opendatacube/datacube-core
.. |Coverage Status| image:: https://coveralls.io/repos/opendatacube/datacube-core/badge.svg?branch=develop&service=github
   :target: https://coveralls.io/github/opendatacube/datacube-core?branch=develop
.. |Documentation Status| image:: https://readthedocs.org/projects/datacube-core/badge/?version=latest
   :target: http://datacube-core.readthedocs.org/en/latest/

.. _installation:

Installation
************

These installation instructions build a Data Cube environment that can be used to ingest data (with config files from github) and run analytics processes.

The Data Cube is a set of python code with dependencies including:

* Python 3.5+ (3.6 recommended)
* GDAL
* PostgeSQL database

These dependencies along with the target operating system environment should be considered when deciding how to install Data Cube to meet your system requirements.

The recommended method for installing the Data Cube is to use a container and package manager. The instructions below are for Miniconda and PostgreSQL.

Other methods to build and install the Data Cube are maintained by the community and are available at https://github.com/opendatacube/documentation. These may include docker recipes and operating system specific deployments.

.. toctree::
   :maxdepth: 2

   conda
   windows
   ubuntu
   macosx

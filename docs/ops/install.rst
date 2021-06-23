.. _installation:

Installation
************

These installation instructions build a Data Cube environment that can be used to index and ingest data (with config files from github) and run analytics processes.

The Data Cube is a set of python code with dependencies including:

* Python 3.6+
* GDAL
* PostgreSQL database

Before installing the Data Cube, these dependencies along with the target operating system environment and scale of the workloads you will be running should be considered.

The recommended method for installing the Data Cube is to use a container and a package manager. The instructions below are for Miniconda and PostgreSQL.

Other methods to build and install the Data Cube are maintained by the community and are available at https://github.com/opendatacube/documentation. These may include docker recipes and operating system specific deployments.

.. toctree::
   :maxdepth: 2

   ubuntu
   conda
   windows
   macosx

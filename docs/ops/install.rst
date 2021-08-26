.. _installation:

Installation
************

These installation instructions build an Open Data Cube (ODC) environment that can be used to index and run analyses

The ODC is a Python library with dependencies including:

* Python 3.8+
* GDAL and other geospatial libraries
* A range of Python dependencies, such as Rasterio and XArray
* A PostgreSQL database.

These dependencies along with the target operating system environment should be considered when deciding
how to install the ODC.

The recommended method for installing the ODC is to use Docker. Other options include using
a native installation on Windows, or Miniconda. PostgreSQL can be run as a container or
installed on a host computer server, but production installations often use a managed service.

.. toctree::
  :maxdepth: 2

  conda
  ubuntu
  windows
  macosx

=================
Microsoft Windows
=================

Miniconda
~~~~~~~~~
1. Download and install Miniconda using the following instructions https://conda.io/docs/user-guide/install/windows.html

2. Open the Anaconda Prompt from the Start Menu to execute the following commands.

.. include:: conda_base.rst 

Datacube is now installed and can be used in the Anaconda Prompt by activating the `cubeenv` environment. 


Manual Installation (Fallback)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Only follow these steps if the Miniconda installation does not suit your needs.

Python 3 environment
--------------------

1. Download and install a standard python release from http://www.python.org/
. The :term:`AGDC` supports 3.5 or newer.

.. note::
    If in a restricted environment with no local administrator access, python can be installed by running::

        msiexec /a python-3.6.4.msi TARGETDIR=C:\Python3
    
    Or by launching the version 3.6 installer and selecting **not** to *install for all users* (only single user install).

2. Ensure **pip** is installed::

    cd C:\Python3
    python -m ensurepip

3. Upgrade and Install python virtualenv::

    python -m pip install --upgrade pip setuptools virtualenv

4. Create an AGDC virtualenv::

    mkdir C:\envs
    Scripts\virtualenv C:\envs\agdcv2

.. note::

    **3.5 only workaround**: Copy ``vcruntime140.dll`` from Python install dir into
    virtualenv ``Scripts\`` folder.

5. Activate virtualenv::

    C:\envs\agdcv2\Scripts\activate
    
The python virtual environment isolates this python installation from other python
installations (which may be in use for other application software) to prevent
conflicts between different python module versions.

Python modules
--------------

On windows systems by default there are no ready configured compilers, and so 
libraries needed for some python modules must be obtained in precompiled 
(binary) form.

Download and install binary wheels from http://www.lfd.uci.edu/~gohlke/pythonlibs/

You will need to download at least:

- GDAL
- rasterio
- numpy
- netCDF4
- psycopg2
- numexpr
- scipy
- pandas
- matplotlib

The following may also be useful:

- lxml
- pyzmq
- udunits2

Install these packages by running in your ``Downloads`` directory::

    pip install *.whl

.. note::
    It may be necessary to manually replace ``*.whl`` with the full filenames for each
    .whl file (unless using a unix-like shell instead of the standard windows command line
    console).

.. note::
    **For 3.5 only**

    If there are problems loading libraries. Try::

        copy site-packages/matplotlib/msvcp140.dll site-packages/osgeo/
        
Also, install the python notebook interface for working with datacube example notebooks::

    pip install jupyter

Datacube installation
---------------------

Obtain a current copy of the datacube source code from GitHub. A simple way is to extract 
https://github.com/opendatacube/datacube-core/archive/develop.zip
into a subdirectory of the python environment. 

Install the datacube module by running::

    cd datacube-core-develop
    python setup.py install


Extra instructions for installing Compliance Checker
----------------------------------------------------
::

    pip install cf_units

- Download and install udunits2 from gohlke

- Edit `site-packages/cf_units/etc/site.cfg` with path to udunits2.dll which should be `venv/share/udunits/udunits2.dll`


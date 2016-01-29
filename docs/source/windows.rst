===============================
Windows AGDC Python Environment
===============================

For For Python 2.7 and Python 3.5
---------------------------------

1. Download and install a standard python release from http://www.python.org/ . The AGDC supports versions 2.7 and
3.5.

.. note::
    If in a restricted environment with no local administrator access, python can be installed by running::

        msiexec /a python-2.7.11.msi TARGETDIR=C:\Python27

2. Ensure **pip** is installed::

    cd C:\Python27
    python -m ensurepip

3. Upgrade and Install python virtualenv::

    python -m pip install --upgrade pip setuptools virtualenv

4. Create an AGDC virtualenv::

    mkdir C:\envs
    Scripts\virtualenv C:\envs\agdcv2

.. note::
    **3.5 only workaround**: Copy ``vcruntime140.dll`` from Python install dir into virtualenv

5. Activate virtualenv::

    C:\envs\agdcv2\Scripts\activate

6. Download and install binary wheels from http://www.lfd.uci.edu/~gohlke/pythonlibs/

You will need to download at least:

- GDAL
- rasterio
- numpy
- netCDF4
- psycopg2

The following may also be useful:

- lxml
- matplotlib
- pyzmq
- udunits2
- pandas

Install these packages by running in your ``Downloads`` directory::

    pip install *.whl

.. note::
    **For 3.5 only**

    If there are problems loading libraries. Try::

        cp site-packages/matplotlib/msvcp140.dll site-packages/osgeo/

PostgreSQL Portable
-------------------

An easy to install version of PostgreSQL can be downloaded from http://sourceforge.net/projects/postgresqlportable/ . It can install and run as an unprivileged windows user.

It helps to add `PostgreSQLPortable\App\PgSQL\bin` to your `%PATH%` to make PostgreSQL
admin commands like `dropdb`, `createdb` and `psql` more easily available.



Extra instructions for installing Compliance Checker
----------------------------------------------------
::

    pip install cf_units

- Download and install udunits2 from gohlke

- Edit `site-packages/cf_units/etc/site.cfg` with path to udunits2.dll which should be `venv/share/udunits/udunits2.dll`


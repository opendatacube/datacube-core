
.. This file is included in the Ubuntu and OS X installation instructions
   All the headings should thus be at the correct level for verbatim inclusion.

Python and packages
===================

Python 3.8+ is required.

Conda environment setup
-----------------------

Conda environments are recommended for use in isolating your ODC development environment from your system installation and other Python environments.

We recommend you use Mambaforge to set up your conda virtual environment, as all the required packages are obtained from the conda-forge channel.
Download and install it from `here <https://github.com/conda-forge/miniforge#mambaforge>`_.

Download the latest version of the Open Data Cube from the `repository <https://github.com/opendatacube/datacube-core>`_::

    git clone https://github.com/opendatacube/datacube-core
    cd datacube-core

Create a conda environment named ``cubeenv``::

    mamba env create -f conda-environment.yml

Activate the ``cubeenv`` conda environment::

    conda activate cubeenv

Find out more about conda environments `here <https://docs.conda.io/projects/conda/en/latest/user-guide/concepts/environments.html>`_.


Postgres testing database configuration
=======================================

This configuration supports local development using your login name.

If this is a new installation of Postgres on your system it is probably wise to set the postgres user password. As the local “postgres” Linux user, we are allowed to connect and manipulate the server using the psql command.

In a terminal, type::

    sudo -u postgres psql postgres

Set a password for the "postgres" database role using the command::

    \password postgres

and set the password when prompted. The password text will be hidden from the console for security purposes.

Type **Control+D** or **\\q** to exit the posgreSQL prompt.

By default in Ubuntu, Postgresql is configured to use ``ident sameuser`` authentication for any connections from the same machine which is useful for development. Check out the excellent Postgresql documentation for more information, but essentially this means that if your Ubuntu username is ``foo`` and you add ``foo`` as a Postgresql user then you can connect to a database without requiring a password for many functions.

Since the only user who can connect to a fresh install is the postgres user, here is how to create yourself a database account (which is in this case also a database superuser) with the same name as your login name and then create a password for the user::

    sudo -u postgres createuser --superuser $USER
    sudo -u postgres psql

    postgres=# \password <foo>

Now we can create the ``agdcintegration`` and ``odcintegration`` databases for testing::

    postgres=# create database agdcintegration;
    postgres=# create database odcintegration;
    
Or, directly from the bash terminal::

    createdb agdcintegration
    createdb odcintegration

Connecting to your own database to try out some SQL should now be as easy as::

    psql -d agdcintegration

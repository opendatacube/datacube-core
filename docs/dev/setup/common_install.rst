
.. This file is included in the Ubuntu and OS X installation instructions
   All the headings should thus be at the correct level for verbatim inclusion.

Python and packages
===================

Python 3.5+ is required. Python 3.6 is recommended.

Anaconda Python
---------------

`Install Anaconda Python <https://www.continuum.io/downloads>`_

Add conda-forge to package channels::

    conda config --add channels conda-forge

Conda Environments are recommended for use in isolating your ODC development environment from your system installation and other python evironments.

Install required python packages and create an ``odc`` conda environment.

Python 3.6::

    conda env create -n odc --file .travis/environment.yaml sphinx

Activate ``odc`` python environment::

    source activate odc


Postgres database configuration
===============================

This configuration supports local development using your login name.

If this is a new installation of Postgres on your system it is probably wise to set the postgres user password. As the local “postgres” Linux user, we are allowed to connect and manipulate the server using the psql command.

In a terminal, type::

	sudo -u postgres psql postgres

Set a password for the "postgres" database role using the command::

	\password postgres
	
and set the password when prompted. The password text will be hidden from the console for security purposes.

Type **Control+D** or **\q** to exit the posgreSQL prompt.

By default in Ubuntu, Postgresql is configured to use ``ident sameuser`` authentication for any connections from the same machine which is useful for development. Check out the excellent Postgresql documentation for more information, but essentially this means that if your Ubuntu username is ``foo`` and you add ``foo`` as a Postgresql user then you can connect to a database without requiring a password for many functions.

Since the only user who can connect to a fresh install is the postgres user, here is how to create yourself a database account (which is in this case also a database superuser) with the same name as your login name and then create a password for the user::

     sudo -u postgres createuser --superuser $USER
     sudo -u postgres psql

     postgres=# \password $USER

Now we can create an ``agdcintegration`` database for testing::

    createdb agdcintegration

Connecting to your own database to try out some SQL should now be as easy as::

    psql -d agdcintegration


Open Data Cube source and development configuration
===================================================

Download the latest version of the software from the `repository <https://github.com/opendatacube/datacube-core>`_ ::

    git clone https://github.com/opendatacube/datacube-core
    cd datacube-core

We need to specify the database user and password for the ODC integration testing. To do this::

    cp integration_tests/agdcintegration.conf ~/.datacube_integration.conf

Then edit the ``~/.datacube_integration.conf`` with a text editor and add the following lines replacing ``<foo>`` with your username and ``<foobar>`` with the database user password you set above (not the postgres one, your ``<foo>`` one)::

    [datacube]
    db_hostname: localhost
    db_database: agdcintegration
    db_username: <foo>
    db_password: <foobar>



Verify it all works
===================

Run the integration tests::

    cd datacube-core
    ./check-code.sh integration_tests

Build the documentation::

    cd datacube-core/docs
    make html

Then open :file:`_build/html/index.html` in your browser to view the Documentation.


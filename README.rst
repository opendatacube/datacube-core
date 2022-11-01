Open Data Cube Core
===================

.. image:: https://github.com/opendatacube/datacube-core/workflows/build/badge.svg
    :alt: Build Status
    :target: https://github.com/opendatacube/datacube-core/actions

.. image:: https://codecov.io/gh/opendatacube/datacube-core/branch/develop/graph/badge.svg
    :alt: Coverage Status
    :target: https://codecov.io/gh/opendatacube/datacube-core

.. image:: https://readthedocs.org/projects/datacube-core/badge/?version=latest
    :alt: Documentation Status
    :target: http://datacube-core.readthedocs.org/en/latest/

Overview
========

The Open Data Cube Core provides an integrated gridded data
analysis environment for decades of analysis ready earth observation
satellite and related data from multiple satellite and other acquisition
systems.

Documentation
=============

See the `user guide <http://datacube-core.readthedocs.io/en/latest/>`__ for
installation and usage of the datacube, and for documentation of the API.

`Join our Slack <http://slack.opendatacube.org>`__ if you need help
setting up or using the Open Data Cube.

Please help us to keep the Open Data Cube community open and inclusive by
reading and following our `Code of Conduct <code-of-conduct.md>`__.

Requirements
============

System
~~~~~~

-  PostgreSQL 10+
-  Python 3.8+

Developer setup
===============

1. Clone:

   -  ``git clone https://github.com/opendatacube/datacube-core.git``

2. Create a Python environment for using the ODC.  We recommend `conda <https://docs.conda.io/en/latest/miniconda.html>`__ as the
   easiest way to handle Python dependencies.

::

   conda create -n odc -c conda-forge python=3.8 datacube pre_commit
   conda activate odc

3. Install a develop version of datacube-core.

::

   cd datacube-core
   pip install --upgrade -e .

4. Install the `pre-commit <https://pre-commit.com>`__ hooks to help follow ODC coding
   conventions when committing with git.

::

   pre-commit install

5. Run unit tests + PyLint
   ``./check-code.sh``

   (this script approximates what is run by Travis. You can
   alternatively run ``pytest`` yourself). Some test dependencies may need to be installed, attempt to install these using:

   ``pip install --upgrade -e '.[test]'``

   If install for these fails please lodge them as issues.

6. **(or)** Run all tests, including integration tests.

   ``./check-code.sh integration_tests``

   -  Assumes a password-less Postgres database running on localhost called

   ``agdcintegration``

   -  Otherwise copy ``integration_tests/agdcintegration.conf`` to
      ``~/.datacube_integration.conf`` and edit to customise.


Alternatively one can use the ``opendatacube/datacube-tests`` docker image to run
tests. This docker includes database server pre-configured for running
integration tests. Add ``--with-docker`` command line option as a first argument
to ``./check-code.sh`` script.

::

   ./check-code.sh --with-docker integration_tests


To run individual test in docker container

::

    docker run -ti -v /home/ubuntu/datacube-core:/code opendatacube/datacube-tests:latest pytest integration_tests/test_filename.py::test_function_name


Developer setup on Ubuntu
~~~~~~~~~~~~~~~~~~~~~~~~~

Building a Python virtual environment on Ubuntu suitable for development work.

Install dependencies:

::

    sudo apt-get update
    sudo apt-get install -y \
        autoconf automake build-essential make cmake \
        graphviz \
        python3-venv \
        python3-dev \
        libpq-dev \
        libyaml-dev \
        libnetcdf-dev \
        libudunits2-dev


Build the python virtual environment:

::

    pyenv="${HOME}/.envs/odc"  # Change to suit your needs
    mkdir -p "${pyenv}"
    python3 -m venv "${pyenv}"
    source "${pyenv}/bin/activate"
    pip install -U pip wheel cython numpy
    pip install -e '.[dev]'
    pip install flake8 mypy pylint autoflake black

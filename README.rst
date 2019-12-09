Open Data Cube Core
===================

|Build Status| |Coverage Status| |Documentation Status|

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

-  PostgreSQL 9.5+
-  Python 3.5+

Developer setup
===============

1. Clone:

   -  ``git clone https://github.com/opendatacube/datacube-core.git``

2. Create a Python environment to use ODC within, we recommend `conda <https://docs.conda.io/en/latest/miniconda.html>`__ as the
   easiest way to handle Python dependencies.

::

   conda create -n odc -c conda-forge python=3.6 datacube pre_commit
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


.. |Build Status| image:: https://travis-ci.org/opendatacube/datacube-core.svg?branch=develop
   :target: https://travis-ci.org/opendatacube/datacube-core
.. |Coverage Status| image:: https://coveralls.io/repos/opendatacube/datacube-core/badge.svg?branch=develop&service=github
   :target: https://coveralls.io/github/opendatacube/datacube-core?branch=develop
.. |Documentation Status| image:: https://readthedocs.org/projects/datacube-core/badge/?version=latest
   :target: http://datacube-core.readthedocs.org/en/latest/

"""
Datacube
========

Provides access to multi-dimensional data, with a focus on Earth observations data such as LANDSAT.

To use this module, see the `Developer Guide <http://agdc-v2.readthedocs.io/en/stable/dev/developer.html>`_.

The main class to access the datacube is :py:class:`datacube.Datacube`.

To initialise this class, you will need a config pointing to a database, such as a file with the following::

    [datacube]
    db_hostname: 130.56.244.227
    db_database: democube
    db_username: cube_user

"""
from __future__ import absolute_import
from .version import __version__
from .api import Datacube
from .config import set_options

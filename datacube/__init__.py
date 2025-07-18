# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2025 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
"""
Datacube
========

Provides access to multi-dimensional data, with a focus on Earth observations data such as LANDSAT.

To use this module, see the `Developer Guide <https://opendatacube.readthedocs.io/en/latest/installation/index.html>`_.

The main class to access the datacube is :class:`datacube.Datacube`.

"""

try:
    from ._version import version as __version__
except ImportError:
    __version__ = "Unknown/Not Installed"

from .api import Datacube
from .utils import xarray_geoextensions

__all__ = [
    "Datacube",
    "__version__",
    "xarray_geoextensions",
]

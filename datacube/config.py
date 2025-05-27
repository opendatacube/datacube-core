# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2025 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
"""
User configuration.
"""

import warnings

from datacube.migration import ODC2DeprecationWarning

warnings.warn(
    "The old datacube.config  is no longer supported.  Please use the new datacube.cfg library",
    ODC2DeprecationWarning,
)

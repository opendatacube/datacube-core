# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2025 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
"""
Datacube metadata generation from STAC.
"""

from ._eo3converter import infer_dc_product, stac2ds

__all__ = (
    "infer_dc_product",
    "stac2ds",
)

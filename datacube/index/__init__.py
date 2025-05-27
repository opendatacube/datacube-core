# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2025 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
"""
Modules for interfacing with the index/database.
"""

from datacube.index.abstract import AbstractIndex as Index

from ._api import index_connect
from ._spatial import extract_geom_from_query, strip_all_spatial_fields_from_query
from .exceptions import DuplicateRecordError, IndexSetupError, MissingRecordError
from .fields import UnknownFieldError

__all__ = [
    "DuplicateRecordError",
    "Index",
    "IndexSetupError",
    "MissingRecordError",
    "UnknownFieldError",
    "extract_geom_from_query",
    "index_connect",
    "strip_all_spatial_fields_from_query",
]

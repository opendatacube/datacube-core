# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2025 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
"""
Modules for interfacing with the index/database.
"""

from ._api import index_connect
from ._spatial import strip_all_spatial_fields_from_query, extract_geom_from_query
from .fields import UnknownFieldError
from .exceptions import DuplicateRecordError, MissingRecordError, IndexSetupError
from datacube.index.abstract import AbstractIndex as Index

__all__ = [
    'DuplicateRecordError',
    'Index',
    'IndexSetupError',
    'MissingRecordError',
    'UnknownFieldError',
    'extract_geom_from_query',
    'index_connect',
    'strip_all_spatial_fields_from_query',
]

# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2020 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
"""
Modules for interfacing with the index/database.
"""

from ._api import index_connect
from .fields import UnknownFieldError
from .exceptions import DuplicateRecordError, MissingRecordError, IndexSetupError
from datacube.index.abstract import AbstractIndex as Index

__all__ = [
    'index_connect',
    'Index',

    'DuplicateRecordError',
    'IndexSetupError',
    'MissingRecordError',
    'UnknownFieldError',
]

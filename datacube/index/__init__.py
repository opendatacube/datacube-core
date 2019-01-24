# coding=utf-8
"""
Modules for interfacing with the index/database.
"""

from ._api import index_connect
from .fields import UnknownFieldError
from .exceptions import DuplicateRecordError, MissingRecordError, IndexSetupError
from .index import Index

__all__ = [
    'index_connect',
    'Index',

    'DuplicateRecordError',
    'IndexSetupError',
    'MissingRecordError',
    'UnknownFieldError',
]

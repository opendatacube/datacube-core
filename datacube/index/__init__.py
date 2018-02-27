# coding=utf-8
"""
Modules for interfacing with the index/database.
"""
from __future__ import absolute_import

from ._api import index_connect as index_connect
from .fields import UnknownFieldError
from .exceptions import DuplicateRecordError, MissingRecordError, IndexSetupError

__all__ = ['index_connect', 'IndexSetupError', 'UnknownFieldError', 'DuplicateRecordError', 'MissingRecordError']

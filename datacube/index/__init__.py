# coding=utf-8
"""
Modules for interfacing with the index/database.
"""
from __future__ import absolute_import

from ._data import connect as data_index_connect
from ._management import connect as data_management_connect

__all__ = ['data_index_connect', 'data_management_connect']

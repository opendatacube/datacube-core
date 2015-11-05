# coding=utf-8
"""
Fields that can be queried or indexed within a dataset or storage.
"""
from __future__ import absolute_import

from sqlalchemy import Table

from . import _core


# Name, parse_function, field_type_ref
DATASET_QUERY_FIELD = Table('query_field_type', _core.METADATA)
STORAGE_QUERY_FIELD = Table('storage_query_field', _core.METADATA)

# psql type, python type .. ?
QUERY_FIELD_TYPE = Table('query_field_type', _core.METADATA)

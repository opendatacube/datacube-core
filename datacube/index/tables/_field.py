# coding=utf-8
"""
Module
"""
from __future__ import absolute_import

from sqlalchemy import Table

from . import _core

dataset_query_field = Table('query_field_type', _core.metadata)

# Name, parse_function, field_type_ref
storage_query_field = Table('storage_query_field', _core.metadata)

# psql type ?
query_field_type = Table('query_field_type', _core.metadata)

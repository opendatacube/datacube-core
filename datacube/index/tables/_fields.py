# coding=utf-8
"""
Fields that can be queried or indexed within a dataset or storage.
"""
from __future__ import absolute_import

from sqlalchemy import Table, Column, String, ForeignKey, Integer
from sqlalchemy.dialects import postgresql as postgres

from . import _core

# psql type, python type .. ?
QUERY_FIELD_TYPE = Table(
    'query_field_type', _core.METADATA,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('name', String, nullable=False),
)

# Name, parse_function, field_type_ref
DATASET_QUERY_FIELD = Table(
    'dataset_query_field', _core.METADATA,
    Column('doc_type', String, nullable=False),
    Column('name', String, nullable=False),
    # List of document offsets.
    # A single offset is a list of keys to lookup (eg ['image', 'extent', 'ul', 'lat'])
    Column('doc_offsets', postgres.ARRAY(String), nullable=False),
    Column('field_type_ref', None, ForeignKey(QUERY_FIELD_TYPE.c.id), nullable=False)
)

STORAGE_QUERY_FIELD = Table(
    'storage_query_field', _core.METADATA

)

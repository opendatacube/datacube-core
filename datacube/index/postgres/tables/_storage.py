# coding=utf-8
"""
Tables for indexing the storage of a dataset in a reprojected or new form.

(ie. What NetCDF files do I have of this dataset?)
"""
from __future__ import absolute_import
from sqlalchemy import ForeignKey, SmallInteger
from sqlalchemy import Table, Column, Integer, String, DateTime
from sqlalchemy.dialects import postgres
from sqlalchemy.sql import func

from . import _core
from . import _dataset

# Map a dataset type to how we will store it (storage_type and each measurement/band).
STORAGE_MAPPING = Table(
    'storage_mapping', _core.METADATA,
    Column('id', SmallInteger, primary_key=True, autoincrement=True),

    # A name/label for this mapping (eg. 'LS7 NBAR'). Specified by users.
    Column('name', String, unique=True, nullable=False),

    # Match any datasets whose metadata is a superset of this.
    Column('dataset_metadata', postgres.JSONB, nullable=False),

    Column('descriptor', postgres.JSONB, nullable=False),

    # When it was added and by whom.
    Column('added', DateTime(timezone=True), server_default=func.now(), nullable=False),
    Column('added_by', String, server_default=func.current_user(), nullable=False),
)

STORAGE_UNIT = Table(
    'storage_unit', _core.METADATA,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('storage_mapping_ref', None, ForeignKey(STORAGE_MAPPING.c.id), nullable=False),

    # The collection it belongs to.
    # Denormalised.
    #  - it could be read from the linked datasets instead
    #  - allows per-collection indexes.
    Column('collection_ref', None, ForeignKey(_dataset.COLLECTION.c.id), nullable=False),

    Column('descriptor', postgres.JSONB, nullable=False),

    Column('path', String, unique=True, nullable=False),

    # When it was added and by whom.
    Column('added', DateTime(timezone=True), server_default=func.now(), nullable=False),
    Column('added_by', String, server_default=func.current_user(), nullable=False),
)

DATASET_STORAGE = Table(
    'dataset_storage', _core.METADATA,
    Column('dataset_ref', None, ForeignKey(_dataset.DATASET.c.id), primary_key=True, nullable=False),
    Column('storage_unit_ref', None, ForeignKey(STORAGE_UNIT.c.id), primary_key=True, nullable=False),
)

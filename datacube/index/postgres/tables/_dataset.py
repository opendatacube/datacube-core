# coding=utf-8
"""
Tables for indexing the datasets which were ingested into the AGDC.
"""
from __future__ import absolute_import

import logging
from sqlalchemy import ForeignKey, UniqueConstraint, CheckConstraint, SmallInteger
from sqlalchemy import Table, Column, Integer, String, DateTime
from sqlalchemy.dialects import postgres
from sqlalchemy.sql import func

from . import _core

_LOG = logging.getLogger(__name__)

COLLECTION = Table(
    'collection', _core.METADATA,
    Column('id', SmallInteger, primary_key=True, autoincrement=True),

    Column('name', String, unique=True, nullable=False),

    # Match any datasets whose metadata is a superset of this document.
    Column('dataset_metadata', postgres.JSONB, nullable=False),
    Column('match_priority', Integer, nullable=False, default=999),

    Column('descriptor', postgres.JSONB, nullable=False),

    # When it was added and by whom.
    Column('added', DateTime(timezone=True), server_default=func.now(), nullable=False),
    Column('added_by', String, server_default=func.current_user(), nullable=False),

    # Name must be alphanumeric + underscores.
    CheckConstraint(r"name ~* '^\w+$'", name='alphanumeric_name'),
)

DATASET = Table(
    'dataset', _core.METADATA,
    Column('id', postgres.UUID, primary_key=True),

    # The collection it belongs to.
    Column('collection_ref', None, ForeignKey(COLLECTION.c.id), nullable=False),

    Column('metadata', postgres.JSONB, index=True, nullable=False),

    # Location of ingested metadata file (yaml?).
    #   - Individual file paths can be calculated relative to this.
    #   - May be null if the dataset was not ingested (provenance-only)
    Column('metadata_path', String, nullable=True, unique=False),

    # When it was added and by whom.
    Column('added', DateTime(timezone=True), server_default=func.now(), nullable=False),
    Column('added_by', String, server_default=func.current_user(), nullable=False),
)

# Link datasets to their source datasets.
DATASET_SOURCE = Table(
    'dataset_source', _core.METADATA,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('dataset_ref', None, ForeignKey(DATASET.c.id), nullable=False),

    # An identifier for this source dataset.
    #    -> Usually it's the dataset type ('ortho', 'nbar'...), as there's typically only one source
    #       of each type.
    Column('classifier', String, nullable=False),
    Column('source_dataset_ref', None, ForeignKey(DATASET.c.id), nullable=False),

    UniqueConstraint('dataset_ref', 'classifier'),
    UniqueConstraint('source_dataset_ref', 'dataset_ref'),
)

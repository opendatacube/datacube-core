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

METADATA_TYPE = Table(
    'metadata_type', _core.METADATA,
    Column('id', SmallInteger, primary_key=True, autoincrement=True),

    Column('name', String, unique=True, nullable=False),

    Column('definition', postgres.JSONB, nullable=False),

    # When it was added and by whom.
    Column('added', DateTime(timezone=True), server_default=func.now(), nullable=False),
    Column('added_by', String, server_default=func.current_user(), nullable=False),

    # Name must be alphanumeric + underscores.
    CheckConstraint(r"name ~* '^\w+$'", name='alphanumeric_name'),
)

COLLECTION = Table(
    'collection', _core.METADATA,
    Column('id', SmallInteger, primary_key=True, autoincrement=True),

    Column('name', String, unique=True, nullable=False),

    # All datasets in the collection have this metadata type.
    Column('metadata_type_ref', None, ForeignKey(METADATA_TYPE.c.id), nullable=False),

    # Match any datasets whose metadata is a superset of this document.
    Column('dataset_metadata', postgres.JSONB, nullable=False),
    Column('match_priority', Integer, nullable=False, default=999),

    Column('definition', postgres.JSONB, nullable=False),

    # When it was added and by whom.
    Column('added', DateTime(timezone=True), server_default=func.now(), nullable=False),
    Column('added_by', String, server_default=func.current_user(), nullable=False),

    # Name must be alphanumeric + underscores.
    CheckConstraint(r"name ~* '^\w+$'", name='alphanumeric_name'),
)

DATASET = Table(
    'dataset', _core.METADATA,
    Column('id', postgres.UUID, primary_key=True),

    Column('metadata_type_ref', None, ForeignKey(METADATA_TYPE.c.id), nullable=False),
    Column('collection_ref', None, ForeignKey(COLLECTION.c.id), nullable=False),

    Column('metadata', postgres.JSONB, index=False, nullable=False),

    # When it was added and by whom.
    Column('added', DateTime(timezone=True), server_default=func.now(), nullable=False),
    Column('added_by', String, server_default=func.current_user(), nullable=False),
)

DATASET_LOCATION = Table(
    'dataset_location', _core.METADATA,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('dataset_ref', None, ForeignKey(DATASET.c.id), nullable=False),

    # The base URI to find the dataset.
    #
    # All paths in the dataset metadata can be computed relative to this.
    # (it is often the path of the source metadata file)
    #
    # eg 'file:///g/data/datasets/LS8_NBAR/agdc-metadata.yaml' or 'ftp://eo.something.com/dataset'
    # 'file' is a scheme, '///g/data/datasets/LS8_NBAR/agdc-metadata.yaml' is a body.
    Column('uri_scheme', String, nullable=False),
    Column('uri_body', String, nullable=False),

    # When it was added and by whom.
    Column('added', DateTime(timezone=True), server_default=func.now(), nullable=False),
    Column('added_by', String, server_default=func.current_user(), nullable=False),

    UniqueConstraint('dataset_ref', 'uri_scheme', 'uri_body'),
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

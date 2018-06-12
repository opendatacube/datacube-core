# coding=utf-8
"""
Tables for indexing the datasets which were ingested into the AGDC.
"""
from __future__ import absolute_import

import logging

from sqlalchemy import ForeignKey, UniqueConstraint, PrimaryKeyConstraint, CheckConstraint, SmallInteger
from sqlalchemy import Table, Column, Integer, String, DateTime
from sqlalchemy.dialects import postgresql as postgres
from sqlalchemy.sql import func

from . import sql
from . import _core

_LOG = logging.getLogger(__name__)

METADATA_TYPE = Table(
    'metadata_type', _core.METADATA,
    Column('id', SmallInteger, primary_key=True, autoincrement=True),

    Column('name', String, unique=True, nullable=False),

    Column('definition', postgres.JSONB, nullable=False),

    # When it was added and by whom.
    Column('added', DateTime(timezone=True), server_default=func.now(), nullable=False),
    Column('added_by', sql.PGNAME, server_default=func.current_user(), nullable=False),

    # Name must be alphanumeric + underscores.
    CheckConstraint(r"name ~* '^\w+$'", name='alphanumeric_name'),
)

DATASET_TYPE = Table(
    'dataset_type', _core.METADATA,
    Column('id', SmallInteger, primary_key=True, autoincrement=True),

    # A name/label for this type (eg. 'ls7_nbar'). Specified by users.
    Column('name', String, unique=True, nullable=False),

    # All datasets of this type should contain these fields.
    # (newly-ingested datasets may be matched against these fields to determine the dataset type)
    Column('metadata', postgres.JSONB, nullable=False),

    # The metadata format expected (eg. what fields to search by)
    Column('metadata_type_ref', None, ForeignKey(METADATA_TYPE.c.id), nullable=False),

    Column('definition', postgres.JSONB, nullable=False),

    # When it was added and by whom.
    Column('added', DateTime(timezone=True), server_default=func.now(), nullable=False),
    Column('added_by', sql.PGNAME, server_default=func.current_user(), nullable=False),

    # Name must be alphanumeric + underscores.
    CheckConstraint(r"name ~* '^\w+$'", name='alphanumeric_name'),
)

DATASET = Table(
    'dataset', _core.METADATA,
    Column('id', postgres.UUID(as_uuid=True), primary_key=True),

    Column('metadata_type_ref', None, ForeignKey(METADATA_TYPE.c.id), nullable=False),
    Column('dataset_type_ref', None, ForeignKey(DATASET_TYPE.c.id), index=True, nullable=False),

    Column('metadata', postgres.JSONB, index=False, nullable=False),

    # Date it was archived. Null for active datasets.
    Column('archived', DateTime(timezone=True), default=None, nullable=True),

    # When it was added and by whom.
    Column('added', DateTime(timezone=True), server_default=func.now(), nullable=False),
    Column('added_by', sql.PGNAME, server_default=func.current_user(), nullable=False),
)

DATASET_LOCATION = Table(
    'dataset_location', _core.METADATA,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('dataset_ref', None, ForeignKey(DATASET.c.id), index=True, nullable=False),

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
    Column('added_by', sql.PGNAME, server_default=func.current_user(), nullable=False),

    # Date it was archived. Null for active locations.
    Column('archived', DateTime(timezone=True), default=None, nullable=True),

    UniqueConstraint('uri_scheme', 'uri_body', 'dataset_ref'),
)

# Link datasets to their source datasets.
DATASET_SOURCE = Table(
    'dataset_source', _core.METADATA,
    Column('dataset_ref', None, ForeignKey(DATASET.c.id), nullable=False),

    # An identifier for this source dataset.
    #    -> Usually it's the dataset type ('ortho', 'nbar'...), as there's typically only one source
    #       of each type.
    Column('classifier', String, nullable=False),
    Column('source_dataset_ref', None, ForeignKey(DATASET.c.id), nullable=False),

    PrimaryKeyConstraint('dataset_ref', 'classifier'),
    UniqueConstraint('source_dataset_ref', 'dataset_ref'),
)

# Metadata for spatial extents.
EXTENT_META = Table(
    'extent_meta', _core.METADATA,
    Column('id', SmallInteger, primary_key=True, autoincrement=True),
    Column('dataset_type_ref', None, ForeignKey(DATASET_TYPE.c.id), nullable=False),

    # The start and end times for period index for periodic extents
    Column('start', DateTime(timezone=True), nullable=False),
    Column('end', DateTime(timezone=True), nullable=False),

    # Python Pandas library style offset alias string indicating the length of each period
    Column('offset_alias', String, nullable=False),

    # The projection string
    Column('crs', String, nullable=True),

    # When it was added and by whom.
    Column('time_added', DateTime(timezone=True), server_default=func.now(), nullable=False),
    Column('added_by', sql.PGNAME, server_default=func.current_user(), nullable=False),

    UniqueConstraint('dataset_type_ref', 'offset_alias')
)


# The spatial extent geometry for various time periods. Refer to extent_meta table to
# obtain length of time period.
EXTENT = Table(
    'extent', _core.METADATA,
    Column('id', postgres.UUID(as_uuid=True), primary_key=True),
    Column('extent_meta_ref', None, ForeignKey(EXTENT_META.c.id), nullable=False),

    # The start time of this period
    Column('start', DateTime(timezone=True), nullable=False),

    # The spatial extent geometry
    Column('geometry', postgres.JSONB, nullable=True),

    UniqueConstraint('extent_meta_ref', 'start')
)

# Time min/max and rectangular spatial bounds for products
RANGES = Table(
    'ranges', _core.METADATA,
    Column('id', SmallInteger, primary_key=True, autoincrement=True),
    Column('dataset_type_ref', None, ForeignKey(DATASET_TYPE.c.id), nullable=False),

    # The time min and max at the time indicated by 'time_added' field for the product
    Column('time_min', DateTime(timezone=True), nullable=True),
    Column('time_max', DateTime(timezone=True), nullable=True),

    # The rectangular spatial bounds and the crs projection used at the time
    # indicated by 'time_added' field for the product
    Column('bounds', postgres.JSONB, nullable=True),
    Column('crs', String, nullable=True),

    # When it was added and by whom.
    Column('time_added', DateTime(timezone=True), server_default=func.now(), nullable=False),
    Column('added_by', sql.PGNAME, server_default=func.current_user(), nullable=False)
)

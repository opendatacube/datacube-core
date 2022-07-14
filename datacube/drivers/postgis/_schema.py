# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2020 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
"""
Tables for indexing the datasets which were ingested into the AGDC.
"""

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

    # Note that the `updated` column is not included here to maintain backwards-compatibility
    # with pre-1.8.3 datacubes (and it is not used by any internal ODC functionality yet anyway)

    # Name must be alphanumeric + underscores.
    CheckConstraint(r"name ~* '^\w+$'", name='alphanumeric_name'),
)

PRODUCT = Table(
    'product', _core.METADATA,
    Column('id', SmallInteger, primary_key=True, autoincrement=True),

    # A name/label for this type (eg. 'ls7_nbar'). Specified by users.
    Column('name', String, unique=True, nullable=False),

    # All datasets of this type should contain these fields.
    # (newly-ingested datasets may be matched against these fields to determine the dataset type)
    Column('metadata', postgres.JSONB, nullable=False),

    # The metadata format expected (eg. what fields to search by)
    #   Typing note: sqlalchemy-stubs doesn't handle this legitimate calling pattern.
    Column('metadata_type_ref', None, ForeignKey(METADATA_TYPE.c.id), nullable=False),  # type: ignore[call-overload]

    Column('definition', postgres.JSONB, nullable=False),

    # When it was added and by whom.
    Column('added', DateTime(timezone=True), server_default=func.now(), nullable=False),
    Column('added_by', sql.PGNAME, server_default=func.current_user(), nullable=False),

    # Note that the `updated` column is not included here to maintain backwards-compatibility
    # with pre-1.8.3 datacubes (and it is not used by any internal ODC functionality yet anyway)

    # Name must be alphanumeric + underscores.
    CheckConstraint(r"name ~* '^\w+$'", name='alphanumeric_name'),
)

DATASET = Table(
    'dataset', _core.METADATA,
    Column('id', postgres.UUID(as_uuid=True), primary_key=True),

    #   Typing note: sqlalchemy-stubs doesn't handle this legitimate calling pattern.
    Column('metadata_type_ref', None, ForeignKey(METADATA_TYPE.c.id), nullable=False),  # type: ignore[call-overload]
    #   Typing note: sqlalchemy-stubs doesn't handle this legitimate calling pattern.
    Column('product_ref', None, ForeignKey(PRODUCT.c.id), index=True, nullable=False),  # type: ignore[call-overload]  # noqa: E501

    Column('metadata', postgres.JSONB, index=False, nullable=False),

    # Date it was archived. Null for active datasets.
    Column('archived', DateTime(timezone=True), default=None, nullable=True),

    # When it was added and by whom.
    Column('added', DateTime(timezone=True), server_default=func.now(), nullable=False),
    Column('added_by', sql.PGNAME, server_default=func.current_user(), nullable=False),

    # Note that the `updated` column is not included here to maintain backwards-compatibility
    # with pre-1.8.3 datacubes (and it is not used by any internal ODC functionality yet anyway)
)

DATASET_LOCATION = Table(
    'dataset_location', _core.METADATA,
    Column('id', Integer, primary_key=True, autoincrement=True),
    #   Typing note: sqlalchemy-stubs doesn't handle this legitimate calling pattern.
    Column('dataset_ref', None, ForeignKey(DATASET.c.id), index=True, nullable=False),  # type: ignore[call-overload]

    # The base URI to find the dataset.
    #
    # All paths in the dataset metadata can be computed relative to this.
    # (it is often the path of the source metadata file)
    #
    # eg 'file:///g/data/datasets/LS8_NBAR/odc-metadata.yaml' or 'ftp://eo.something.com/dataset'
    # 'file' is a scheme, '///g/data/datasets/LS8_NBAR/odc-metadata.yaml' is a body.
    Column('uri_scheme', String, nullable=False),
    Column('uri_body', String, nullable=False),

    # When it was added and by whom.
    Column('added', DateTime(timezone=True), server_default=func.now(), nullable=False),
    Column('added_by', sql.PGNAME, server_default=func.current_user(), nullable=False),

    # Date it was archived. Null for active locations.
    Column('archived', DateTime(timezone=True), default=None, nullable=True),

    UniqueConstraint('uri_scheme', 'uri_body', 'dataset_ref'),

    # Note that the `updated` column is not included here to maintain backwards-compatibility
    # with pre-1.8.3 datacubes (and it is not used by any internal ODC functionality yet anyway)
)

# Link datasets to their source datasets.
DATASET_SOURCE = Table(
    'dataset_source', _core.METADATA,
    #   Typing note: sqlalchemy-stubs doesn't handle this legitimate calling pattern.
    Column('dataset_ref', None, ForeignKey(DATASET.c.id), nullable=False),  # type: ignore[call-overload]

    # An identifier for this source dataset.
    #    -> Usually it's the dataset type ('ortho', 'nbar'...), as there's typically only one source
    #       of each type.
    Column('classifier', String, nullable=False),
    #   Typing note: sqlalchemy-stubs doesn't handle this legitimate calling pattern.
    Column('source_dataset_ref', None, ForeignKey(DATASET.c.id), nullable=False),  # type: ignore[call-overload]

    PrimaryKeyConstraint('dataset_ref', 'classifier'),
    UniqueConstraint('source_dataset_ref', 'dataset_ref'),

    # Note that the `updated` column is not included here to maintain backwards-compatibility
    # with pre-1.8.3 datacubes (and it is not used by any internal ODC functionality yet anyway)

    # This table is immutable and uses a migrations based `added` column to keep track of new
    # dataset locations being added. The added column defaults to `now()`
)

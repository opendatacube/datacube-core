# coding=utf-8
"""
Tables for indexing the datasets which were ingested into the AGDC.
"""
from __future__ import absolute_import

import logging

from sqlalchemy import ForeignKey, UniqueConstraint
from sqlalchemy import Table, Column, Integer, String, DateTime
from sqlalchemy.dialects import postgres
from sqlalchemy.sql import func

from . import _core

_LOG = logging.getLogger(__name__)

DATASET = Table(
    'dataset', _core.METADATA,
    Column('id', postgres.UUID, primary_key=True),
    # (typically) product type: 'nbar', 'ortho' etc.
    Column('type', String, nullable=False, doc=''),

    Column('metadata', postgres.JSONB, nullable=False),

    # Location of ingested metadata file (yaml?).
    #   - Individual file paths can be calculated relative to this.
    #   - May be null if the dataset was not ingested (provenance-only)
    Column('metadata_path', String, nullable=True, unique=True),

    Column('added', DateTime(timezone=True), server_default=func.now()),

)

# Link datasets to their source datasets.
DATASET_SOURCE = Table(
    'dataset_source', _core.METADATA,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('dataset_ref', None, ForeignKey(DATASET.c.id)),

    # An identifier for this source dataset.
    #    -> Usually it's the dataset type ('ortho', 'nbar'...), as there's typically only one source
    #       of each type.
    Column('classifier', String, nullable=False),
    Column('source_dataset_ref', None, ForeignKey(DATASET.c.id)),

    Column('added', DateTime(timezone=True), server_default=func.now()),

    UniqueConstraint('dataset_ref', 'classifier'),
    UniqueConstraint('source_dataset_ref', 'dataset_ref'),
)

_EXAMPLE_DATASETS_MATCHING = {
    'product_type': 'NBAR',
    'platform': {
        'code': 'LANDSAT_7'
    }
}
_EXAMPLE_DATASET_TYPE_MEASUREMENTS = {
    '10': {
        'dtype': 'int16',
        'fill_value': -999,
        'interpolation': 'nearest'
    },
    '20': {
        'dtype': 'int16',
        'fill_value': -999,
        'interpolation': 'cubic'
    }

}

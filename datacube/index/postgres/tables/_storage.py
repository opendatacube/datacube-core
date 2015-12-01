# coding=utf-8
"""
Tables for indexing the storage of a dataset in a reprojected or new form.

(ie. What NetCDF files do I have of this dataset?)
"""
from __future__ import absolute_import

from sqlalchemy import ForeignKey, UniqueConstraint, BigInteger
from sqlalchemy import Table, Column, Integer, String, DateTime
from sqlalchemy.dialects import postgres
from sqlalchemy.sql import func

from . import _core
from . import _dataset

# Modern equivalent of 'tile_type' (how to store on disk)
# -> Serialises to 'storage_config.yaml' documents
STORAGE_TYPE = Table(
    'storage_type', _core.METADATA,
    Column('id', Integer, primary_key=True, autoincrement=True),

    # The storage "driver" to use: eg. 'NetCDF CF', 'GeoTIFF'...
    Column('driver', String, nullable=False),

    # A name/label for this type (eg. '30m bands'). Specified by users.
    Column('name', String, nullable=False, unique=True),
    # A human-readable, potentially multi-line, description for display on the UI.
    Column('description', String),

    # See "_EXAMPLE_STORAGE_TYPE_DESCRIPTOR" below
    Column('descriptor', postgres.JSONB, nullable=False),

    # When it was added and by whom.
    Column('added', DateTime(timezone=True), server_default=func.now(), nullable=False),
    Column('added_by', String, server_default=func.current_user(), nullable=False),

)

# Map a dataset type to how we will store it (storage_type and each measurement/band).
STORAGE_MAPPING = Table(
    'storage_mapping', _core.METADATA,
    Column('id', Integer, primary_key=True, autoincrement=True),

    # The storage type to use.
    Column('storage_type_ref', ForeignKey(STORAGE_TYPE.c.id), nullable=False),

    # A name/label for this mapping (eg. 'LS7 NBAR'). Specified by users.
    Column('name', String, nullable=False),
    # A human-readable, potentially multi-line, description for display on the UI.
    Column('description', String),

    # The name of the location where the storage units should be stored. Specified by users.
    Column('location_name', String, nullable=False),

    # The offset relative to location where the storage units should be stored. Specified by users.
    Column('file_path_template', String, nullable=False),

    # Match any datasets whose metadata is a superset of this.
    # See "_EXAMPLE_DATASETS_MATCHING" below
    Column('dataset_metadata', postgres.JSONB, nullable=False),

    # Storage config for each measurement.
    # The expected values depend on the storage driver (eg. NetCDF).
    #
    # Eg.
    # '10':
    #   dtype: int16
    #   nodata: -999
    #   interpolation: nearest
    # '20':
    #   dtype: int16
    #   nodata: -999
    #   interpolation: cubic
    # See "_EXAMPLE_DATASET_TYPE_MEASUREMENTS" below.
    Column('measurements', postgres.JSONB),

    # When it was added and by whom.
    Column('added', DateTime(timezone=True), server_default=func.now(), nullable=False),
    Column('added_by', String, server_default=func.current_user(), nullable=False),

    UniqueConstraint('storage_type_ref', 'name'),
)

STORAGE_UNIT = Table(
    'storage_unit', _core.METADATA,
    Column('id', BigInteger, primary_key=True, autoincrement=True),
    Column('storage_mapping_ref', None, ForeignKey(STORAGE_MAPPING.c.id), nullable=False),

    # The collection it belongs to.
    # This isn't normalised: it could be read from the linked datasets.
    #
    # It's a performance optimisation: we can create per-collection indexes.
    Column('collection_ref', None, ForeignKey(_dataset.COLLECTION.c.id), nullable=False),

    # See "_EXAMPLE_STORAGE_DESCRIPTOR" below.
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

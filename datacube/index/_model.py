# coding=utf-8
"""
Database model.
"""
from __future__ import absolute_import

import logging

from sqlalchemy import ForeignKey, UniqueConstraint
from sqlalchemy import Table, Column, Integer, String, DateTime, MetaData
from sqlalchemy.dialects import postgres
from sqlalchemy.schema import CreateSchema
from sqlalchemy.sql import func

_LOG = logging.getLogger(__name__)

convention = {
    "ix": 'ix_%(column_0_label)s',
    "uq": "uq_%(table_name)s_%(column_0_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s"
}
SCHEMA_NAME = 'agdc'
metadata = MetaData(naming_convention=convention, schema=SCHEMA_NAME)

dataset = Table(
    'dataset', metadata,
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
dataset_source = Table(
    'dataset_source', metadata,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('dataset_ref', None, ForeignKey(dataset.c.id)),

    # An identifier for this source dataset.
    #    -> Usually it's the dataset type ('ortho', 'nbar'...), as there's typically only one source
    #       of each type.
    Column('classifier', String, nullable=False),
    Column('source_dataset_ref', None, ForeignKey(dataset.c.id)),

    Column('added', DateTime(timezone=True), server_default=func.now()),

    UniqueConstraint('dataset_ref', 'classifier'),
    UniqueConstraint('source_dataset_ref', 'dataset_ref'),
)

# Modern equivalent of 'tile_type' (how to stor
# -> Serialises to 'storage_config.yaml' documents
storage_type = Table(
    'storage_type', metadata,
    Column('id', Integer, primary_key=True, autoincrement=True),
    # 'NetCDF CF', 'GeoTIFF' etc...
    Column('type', String, unique=False),

    Column('descriptor', postgres.JSONB),
)
example_storage_descriptor = {
    'base_path': '/tmp/v10/dra547/',
    'chunking': {'t': 1, 'x': 500, 'y': 500},
    'dimension_order': ['t', 'y', 'x'],
    'filename_format': '{platform[code]}_{instrument[name]}_{lons[0]}_{lats[0]}_{creation_dt:%Y-%m-%dT%H-%M-%S.%f}.nc',
    'id': 'sdfklsdm34itsdv',
    'name': 'foo',
    'projection': {
        'spatial_ref': 'GEOGCS["WGS 84",\n'
                       '    DATUM["WGS_1984",\n'
                       '        SPHEROID["WGS 84",6378137,298.257223563,\n'
                       '            AUTHORITY["EPSG","7030"]],\n'
                       '        AUTHORITY["EPSG","6326"]],\n'
                       '    PRIMEM["Greenwich",0,\n'
                       '        AUTHORITY["EPSG","8901"]],\n'
                       '    UNIT["degree",0.0174532925199433,\n'
                       '        AUTHORITY["EPSG","9122"]],\n'
                       '    AUTHORITY["EPSG","4326"]]\n'
    },
    'resolution': {'x': 0.00025, 'y': -0.00025},
    'tile_size': {'x': 1.0, 'y': -1.0},
    'type': 'NetCDF CF'
}

# Map a dataset type to how we will store it.
dataset_storage = Table(
    'dataset_storage', metadata,
    Column('id', Integer, primary_key=True, autoincrement=True),

    # Match any datasets whose metadata is a superset of this.
    Column('datasets_matching', postgres.JSONB, nullable=False),

    # Where in the dataset metadata to find a dictionary of measurements.
    # For EO datasets this is "bands".
    # (Non-EO dataset types may have different document structures.)
    #
    # It expects to find a dictionary, where:
    #       - keys are band ids.
    #       - each value is a dictionary containing measurement information.
    Column('dataset_measurements_key', postgres.ARRAY(String), default='bands'),

    # The storage type to use.
    Column('storage_type_ref', ForeignKey(storage_type.c.id),
           nullable=False),

    # Storage config for each measurement.
    # The value depends on the storage type (eg. NetCDF CF).
    #
    # Eg.
    # '10':
    #   dtype: int16
    #   fill_value: -999
    #   interpolation: nearest
    # '20':
    #   dtype: int16
    #   fill_value: -999
    #   interpolation: cubic
    Column('measurements', postgres.JSONB)
)

example_datasets_matching = {
    'product_type': 'NBAR',
    'platform': {
        'code': 'LANDSAT_7'
    }
}
example_dataset_type_measurements = {
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

# Which storage units our dataset is in.
# Unique: (dataset, representation)?
storage = Table(
    'storage', metadata,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('dataset_ref', None, ForeignKey(dataset.c.id)),
    Column('dataset_storage_ref', None, ForeignKey(dataset_storage.c.id)),

    # TODO: Define this (from Damien's code?). It says which subset of the dataset is stored here.
    Column('descriptor', postgres.JSONB, nullable=False),

    Column('path', String, nullable=False),

    Column('added', DateTime(timezone=True), server_default=func.now()),
)


def ensure_db(connection, engine):
    if not engine.dialect.has_schema(connection, SCHEMA_NAME):
        engine.execute(CreateSchema(SCHEMA_NAME))
        metadata.create_all(engine)

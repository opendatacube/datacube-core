# coding=utf-8
"""
Tables for indexing the storage of a dataset in a reprojected or new form.

(ie. What NetCDF files do I have of this dataset?)
"""
from __future__ import absolute_import

from sqlalchemy import ForeignKey
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
    # 'NetCDF CF', 'GeoTIFF' etc...
    Column('type', String, unique=False),

    Column('descriptor', postgres.JSONB),
)

# Map a dataset type to how we will store it (storage_type and each measurement/band).
STORAGE_MAPPING = Table(
    'storage_mapping', _core.METADATA,
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
    Column('storage_type_ref', ForeignKey(STORAGE_TYPE.c.id), nullable=False),

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

# Which storage units our dataset is in.
# Unique: (dataset, representation)?
STORAGE = Table(
    'storage', _core.METADATA,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('dataset_ref', None, ForeignKey(_dataset.DATASET.c.id)),
    Column('storage_mapping_ref', None, ForeignKey(STORAGE_MAPPING.c.id)),

    # TODO: Define this (from Damien's code?). It says which subset of the dataset is stored here.
    Column('descriptor', postgres.JSONB, nullable=False),

    Column('path', String, nullable=False),

    Column('added', DateTime(timezone=True), server_default=func.now()),
)

_EXAMPLE_STORAGE_TYPE_DESCRIPTOR = {
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

_EXAMPLE_STORAGE_DESCRIPTOR = {
    'path': '/tmp/something.nc',
    'size_bytes': 34534534345,

    'extents': [
        {
            'x': {
                'from': 1.0,
                'to': 23
            },

            't': {
                'from': 5345345345
            }
        }
    ]
}

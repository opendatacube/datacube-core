# coding=utf-8
"""
Module
"""
from __future__ import absolute_import

import copy

import pytest

from datacube.model import Dataset


_15M_STORAGE_TYPE = {
    'name': '15m_bands',
    'driver': 'NetCDF CF',
    'base_path': '/short/v10/dra547/tmp/5/',
    'chunking': {'t': 1, 'x': 500, 'y': 500},
    'dimension_order': ['t', 'y', 'x'],
    'filename_format': '{platform[code]}_{instrument[name]}_{lons[0]}_{lats[0]}_{creation_dt:%Y-%m-%dT%H-%M-%S.%f}.nc',
    'projection': {
        'spatial_ref': 'epsg:1234'
    },
    'resolution': {'x': 0.00015, 'y': -0.00015},
    'tile_size': {'x': 1.0, 'y': -1.0}
}

_STORAGE_MAPPING = {
    'name': 'LS5 NBAR',
    'match': {
        'metadata':
            {
                'instrument': {'name': 'TM'},
                'platform': {'code': 'LANDSAT_5'},
                'product_type': 'NBAR'
            }
    },
    'location_name': 'eotiles',
    'file_path_template': '/file_path_template/file.nc',
    'measurements': {
        '1': {'dtype': 'int16',
              'fill_value': -999,
              'resampling_method': 'cubic',
              'varname': 'band_1'},
        '2': {'dtype': 'int16',
              'fill_value': -999,
              'resampling_method': 'cubic',
              'varname': 'band_2'},
        '3': {'dtype': 'int16',
              'fill_value': -999,
              'resampling_method': 'cubic',
              'varname': 'band_3'},
    },
    'storage': {
        'driver': 'NetCDF CF',
        'chunking': {'t': 1, 'x': 500, 'y': 500},
        'dimension_order': ['t', 'y', 'x'],
        'crs': 'GEOGCS["WGS 84",\n'
               '    DATUM["WGS_1984",\n'
               '        SPHEROID["WGS 84",6378137,298.257223563,\n'
               '            AUTHORITY["EPSG","7030"]],\n'
               '        AUTHORITY["EPSG","6326"]],\n'
               '    PRIMEM["Greenwich",0,\n'
               '        AUTHORITY["EPSG","8901"]],\n'
               '    UNIT["degree",0.0174532925199433,\n'
               '        AUTHORITY["EPSG","9122"]],\n'
               '    AUTHORITY["EPSG","4326"]]\n',
        'resolution': {'x': 0.00025, 'y': -0.00025},
        'tile_size': {'x': 1.0, 'y': -1.0}
    }
}

_DATASET_METADATA = {
    'id': 'f7018d80-8807-11e5-aeaa-1040f381a756',
    'instrument': {'name': 'TM'},
    'platform': {
        'code': 'LANDSAT_5',
        'label': 'Landsat 5'
    },
    'size_bytes': 4550,
    'product_type': 'NBAR',
    'bands': {
        '1': {
            'type': 'reflective',
            'cell_size': 25.0,
            'path': 'product/LS8_OLITIRS_NBAR_P54_GALPGS01-002_112_079_20140126_B1.tif',
            'label': 'Coastal Aerosol',
            'number': '1'
        },
        '2': {
            'type': 'reflective',
            'cell_size': 25.0,
            'path': 'product/LS8_OLITIRS_NBAR_P54_GALPGS01-002_112_079_20140126_B2.tif',
            'label': 'Visible Blue',
            'number': '2'
        },
        '3': {
            'type': 'reflective',
            'cell_size': 25.0,
            'path': 'product/LS8_OLITIRS_NBAR_P54_GALPGS01-002_112_079_20140126_B3.tif',
            'label': 'Visible Green',
            'number': '3'
        },
    }
}


def test_idempotent_add_mapping(index, local_config):
    """
    :type local_config: datacube.config.LocalConfig
    :type index: datacube.index._api.Index
    """
    index.mappings.add(_STORAGE_MAPPING)
    # Second time, no effect, because it's equal.
    index.mappings.add(_STORAGE_MAPPING)

    # But if we add the same mapping with differing properties we should get an error:
    different_storage_mapping = copy.deepcopy(_STORAGE_MAPPING)
    different_storage_mapping['location_name'] = 'new_location'
    with pytest.raises(ValueError):
        index.mappings.add(different_storage_mapping)


def test_collection_indexes_views_exist(db, telemetry_collection):
    """
    :type db: datacube.index.postgres._api.PostgresDb
    :type telemetry_collection: datacube.model.Collection
    """
    # Ensure indexes were created for the telemetry collection (following the naming conventions):
    val = db._connection.execute("SELECT to_regclass('agdc.ix_field_landsat_telemetry_dataset_satellite')").scalar()
    assert val == 'agdc.ix_field_landsat_telemetry_dataset_satellite'

    # Ensure view was created (following naming conventions)
    val = db._connection.execute("SELECT to_regclass('agdc.landsat_telemetry_dataset')").scalar()
    assert val == 'agdc.landsat_telemetry_dataset'


def test_idempotent_add_collection(index, telemetry_collection, telemetry_collection_doc):
    """
    :type telemetry_collection: datacube.model.Collection
    :type index: datacube.index._api.Index
    """
    # Re-add should have no effect, because it's equal to the current one.
    index.collections.add(telemetry_collection_doc)

    # But if we add the same collection with differing properties we should get an error:
    different_telemetry_collection = copy.deepcopy(telemetry_collection_doc)
    different_telemetry_collection['match']['metadata']['ga_label'] = 'something'
    with pytest.raises(ValueError):
        index.collections.add(different_telemetry_collection)

        # TODO: Support for adding/changing search fields?

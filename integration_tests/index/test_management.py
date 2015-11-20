# coding=utf-8
"""
Module
"""
from __future__ import absolute_import

from datacube.config import SystemConfig
from datacube.index._management import DataManagement
from datacube.model import Dataset
from integration_tests.index._common import init_db

_STORAGE_TYPE = {
    'driver': 'NetCDF CF',
    'base_path': '/short/v10/dra547/tmp/7/',
    'chunking': {'t': 1, 'x': 500, 'y': 500},
    'dimension_order': ['t', 'y', 'x'],
    'filename_format': '{platform[code]}_{instrument[name]}_{lons[0]}_{lats[0]}_{creation_dt:%Y-%m-%dT%H-%M-%S.%f}.nc',
    'name': '30m_bands',
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
    'tile_size': {'x': 1.0, 'y': -1.0}
}

_STORAGE_MAPPING = {
    'driver': 'NetCDF CF',
    'name': 'LS5 NBAR',
    'match': {
        'metadata':
            {
                'instrument': {'name': 'TM'},
                'platform': {'code': 'LANDSAT_5'},
                'product_type': 'NBAR'
            }
    },
    'storage': [
        {

            'name': '30m_bands',
            'location_name': 'gdata',
            'location_offset': '/location_offset/file.nc',
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
            }
        }
    ]
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


def test_add_storage_type():
    dm = DataManagement(init_db(), SystemConfig.find([]))

    dataset = Dataset('eo', _DATASET_METADATA, '/tmp/somepath.yaml')

    storage_mappings = dm.get_storage_mappings_for_dataset(dataset)
    assert len(storage_mappings) == 0

    dm.ensure_storage_type(_STORAGE_TYPE)
    dm.ensure_storage_mapping(_STORAGE_MAPPING)

    # The properties of the dataset should match.
    storage_mappings = dm.get_storage_mappings_for_dataset(dataset)
    assert len(storage_mappings) == 1

    mapping = storage_mappings[0]
    assert mapping.name == 'LS5 NBAR'
    assert mapping.storage_pattern == 'file:///g/data/location_offset/file.nc'
    assert mapping.match.metadata == _STORAGE_MAPPING['match']['metadata']
    assert mapping.measurements == _STORAGE_MAPPING['storage'][0]['measurements']

    storage_type = mapping.storage_type
    assert storage_type.name == '30m_bands'
    assert storage_type.driver == 'NetCDF CF'
    assert storage_type.descriptor == _STORAGE_TYPE

    # A different dataset should not match our storage types
    dataset = Dataset('eo', {
        'instrument': {'name': 'OLI'},
        'platform': {'code': 'LANDSAT_8'},
        'product_type': 'NBAR'
    }, '/tmp/other.yaml')
    storage_mappings = dm.get_storage_mappings_for_dataset(dataset)
    assert len(storage_mappings) == 0

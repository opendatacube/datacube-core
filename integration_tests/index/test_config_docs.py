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
    'filename_format': '{platform[code]}_{instrument[name]}_{tile_index[0]}_{tile_index[1]}_{start_time}.nc',
    'projection': {
        'spatial_ref': 'epsg:1234'
    },
    'resolution': {'x': 0.00015, 'y': -0.00015},
    'tile_size': {'x': 1.0, 'y': -1.0}
}

_STORAGE_TYPE = {
    'name': 'ls5_nbar',
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
        'blue': {'dtype': 'int16',
                 'nodata': -999,
                 'units': '1',
                 'resampling_method': 'cubic',
                 'src_varname': '1'},
        'green': {'dtype': 'int16',
                  'nodata': -999,
                  'units': '1',
                  'resampling_method': 'cubic',
                  'src_varname': '2'},
        'red': {'dtype': 'int16',
                'nodata': -999,
                'units': '1',
                'resampling_method': 'cubic',
                'src_varname': '3'},
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


def test_metadata_indexes_views_exist(db, default_metadata_type):
    """
    :type db: datacube.index.postgres._api.PostgresDb
    :type default_metadata_type: datacube.model.MetadataType
    """
    # Ensure indexes were created for the eo metadata type (following the naming conventions):
    val = db._connection.execute(
        "SELECT to_regclass('agdc.dix_field_eo_dataset_platform')").scalar()
    assert val == 'agdc.dix_field_eo_dataset_platform'

    # Ensure view was created (following naming conventions)
    val = db._connection.execute("SELECT to_regclass('agdc.eo_dataset')").scalar()
    assert val == 'agdc.eo_dataset'


def test_idempotent_add_dataset_type(index, ls5_nbar_gtiff_type, ls5_nbar_gtiff_doc):
    """
    :type ls5_nbar_gtiff_type: datacube.model.DatasetType
    :type index: datacube.index._api.Index
    """
    assert index.datasets.types.get_by_name(ls5_nbar_gtiff_type.name) is not None

    # Re-add should have no effect, because it's equal to the current one.
    index.datasets.types.add_document(ls5_nbar_gtiff_doc)

    # But if we add the same type with differing properties we should get an error:
    different_telemetry_type = copy.deepcopy(ls5_nbar_gtiff_doc)
    different_telemetry_type['metadata']['ga_label'] = 'something'
    with pytest.raises(ValueError):
        index.datasets.types.add_document(different_telemetry_type)

        # TODO: Support for adding/changing search fields?

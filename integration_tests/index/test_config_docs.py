# coding=utf-8
"""
Module
"""
from __future__ import absolute_import

import copy

import pytest


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

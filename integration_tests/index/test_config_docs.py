# coding=utf-8
"""
Module
"""
from __future__ import absolute_import

import copy

import pytest

from datacube.model import Range

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
    assert _object_exists(db, 'dix_eo_platform')

    # Ensure view was created (following naming conventions)
    assert _object_exists(db, 'dv_eo_dataset')


def test_dataset_indexes_views_exist(db, ls5_nbar_gtiff_type):
    """
    :type db: datacube.index.postgres._api.PostgresDb
    :type ls5_nbar_gtiff_type: datacube.model.DatasetType
    """
    assert ls5_nbar_gtiff_type.name == 'ls5_nbart_p54_gtiff'

    # Ensure field indexes were created for the dataset type (following the naming conventions):
    assert _object_exists(db, "dix_ls5_nbart_p54_gtiff_orbit")

    # Ensure it does not create a 'platform' index, because that's a fixed field
    # (ie. identical in every dataset of the type)
    assert not _object_exists(db, "dix_ls5_nbart_p54_gtiff_platform")

    # Ensure view was created (following naming conventions)
    assert _object_exists(db, 'dv_ls5_nbart_p54_gtiff_dataset')


def test_dataset_conposit_indexes_exist(db, ls5_nbar_gtiff_type):
    # This type has fields named lat/lon/time, so composite indexes should now exist for them:
    # (following the naming conventions)
    assert _object_exists(db, "dix_ls5_nbart_p54_gtiff_time_lat_lon")
    assert _object_exists(db, "dix_ls5_nbart_p54_gtiff_lat_lon_time")

    # But no individual field indexes for these
    assert not _object_exists(db, "dix_ls5_nbart_p54_gtiff_lat")
    assert not _object_exists(db, "dix_ls5_nbart_p54_gtiff_lon")
    assert not _object_exists(db, "dix_ls5_nbart_p54_gtiff_time")


def _object_exists(db, index_name):
    val = db._connection.execute("SELECT to_regclass('agdc.%s')" % index_name).scalar()
    return val == ('agdc.%s' % index_name)


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


def test_filter_types_by_fields(index, ls5_nbar_gtiff_type):
    """
    :type ls5_nbar_gtiff_type: datacube.model.DatasetType
    :type index: datacube.index._api.Index
    """
    assert index.datasets.types
    res = list(index.datasets.types.get_with_fields(['lat', 'lon', 'platform']))
    assert res == [ls5_nbar_gtiff_type]

    res = list(index.datasets.types.get_with_fields(['lat', 'lon', 'platform', 'favorite_icecream']))
    assert len(res) == 0


def test_filter_types_by_search(index, ls5_nbar_gtiff_type):
    """
    :type ls5_nbar_gtiff_type: datacube.model.DatasetType
    :type index: datacube.index._api.Index
    """
    assert index.datasets.types

    # No arguments, return all.
    res = list(index.datasets.types.search())
    assert res == [ls5_nbar_gtiff_type]

    # Matching fields
    res = list(index.datasets.types.search(
        product_type='nbart',
        product='ls5_nbart_p54_gtiff'
    ))
    assert res == [ls5_nbar_gtiff_type]

    # Matching fields and non-available fields
    res = list(index.datasets.types.search(
        product_type='nbart',
        product='ls5_nbart_p54_gtiff',
        lat=Range(142.015625, 142.015625),
        lon=Range(-12.046875, -12.046875)
    ))
    assert res == []

    # Matching fields and available fields
    [(res, q)] = list(index.datasets.types.search_robust(
        product_type='nbart',
        product='ls5_nbart_p54_gtiff',
        lat=Range(142.015625, 142.015625),
        lon=Range(-12.046875, -12.046875)
    ))
    assert res == ls5_nbar_gtiff_type
    assert 'lat' in q
    assert 'lon' in q

    # Or expression test
    res = list(index.datasets.types.search(
        product_type=['nbart', 'nbar'],
    ))
    assert res == [ls5_nbar_gtiff_type]

    # Mismatching fields
    res = list(index.datasets.types.search(
        product_type='nbar',
    ))
    assert res == []

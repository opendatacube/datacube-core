# coding=utf-8
"""
Module
"""
from __future__ import absolute_import

import copy

import pytest
from datacube.index.postgres._fields import NumericRangeDocField
from datacube.model import Range, Dataset
from datacube.utils import changes

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
    # Metadata indexes should no longer exist.
    assert not _object_exists(db, 'dix_eo_platform')

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

    # Ensure view was created (following naming conventions)
    assert not _object_exists(db, 'dix_ls5_nbart_p54_gtiff_gsi'), "indexed=false field gsi shouldn't have an index"


def test_dataset_composit_indexes_exist(db, ls5_nbar_gtiff_type):
    # This type has fields named lat/lon/time, so composite indexes should now exist for them:
    # (following the naming conventions)
    assert _object_exists(db, "dix_ls5_nbart_p54_gtiff_time_lat_lon")
    assert _object_exists(db, "dix_ls5_nbart_p54_gtiff_lat_lon_time")

    # But no individual field indexes for these
    assert not _object_exists(db, "dix_ls5_nbart_p54_gtiff_lat")
    assert not _object_exists(db, "dix_ls5_nbart_p54_gtiff_lon")
    assert not _object_exists(db, "dix_ls5_nbart_p54_gtiff_time")


def _object_exists(db, index_name):
    with db.connect() as connection:
        val = connection._connection.execute("SELECT to_regclass('agdc.%s')" % index_name).scalar()
    return val == ('agdc.%s' % index_name)


def test_idempotent_add_dataset_type(index, ls5_nbar_gtiff_type, ls5_nbar_gtiff_doc):
    """
    :type ls5_nbar_gtiff_type: datacube.model.DatasetType
    :type index: datacube.index._api.Index
    """
    assert index.products.get_by_name(ls5_nbar_gtiff_type.name) is not None

    # Re-add should have no effect, because it's equal to the current one.
    index.products.add_document(ls5_nbar_gtiff_doc)

    # But if we add the same type with differing properties we should get an error:
    different_telemetry_type = copy.deepcopy(ls5_nbar_gtiff_doc)
    different_telemetry_type['metadata']['ga_label'] = 'something'
    with pytest.raises(ValueError):
        index.products.add_document(different_telemetry_type)

        # TODO: Support for adding/changing search fields?


def test_update_dataset(index, ls5_nbar_gtiff_doc, example_ls5_nbar_metadata_doc):
    """
    :type index: datacube.index._api.Index
    """
    ls5_nbar_gtiff_type = index.products.add_document(ls5_nbar_gtiff_doc)
    assert ls5_nbar_gtiff_type

    example_ls5_nbar_metadata_doc['lineage']['source_datasets'] = {}
    dataset = Dataset(ls5_nbar_gtiff_type, example_ls5_nbar_metadata_doc, 'file:///test/doc.yaml')
    dataset = index.datasets.add(dataset)
    assert dataset

    # update with the same doc should do nothing
    index.datasets.update(dataset)
    updated = index.datasets.get(dataset.id)
    assert updated.local_uri == 'file:///test/doc.yaml'

    # update location
    assert index.datasets.get(dataset.id).local_uri == 'file:///test/doc.yaml'
    update = Dataset(ls5_nbar_gtiff_type, example_ls5_nbar_metadata_doc, 'file:///test/doc2.yaml')
    index.datasets.update(update)
    updated = index.datasets.get(dataset.id)
    assert updated.local_uri == 'file:///test/doc2.yaml'

    # adding more metadata should always be allowed
    doc = copy.deepcopy(updated.metadata_doc)
    doc['test1'] = {'some': 'thing'}
    update = Dataset(ls5_nbar_gtiff_type, doc, updated.local_uri)
    index.datasets.update(update)
    updated = index.datasets.get(dataset.id)
    assert updated.metadata_doc['test1'] == {'some': 'thing'}
    assert updated.local_uri == 'file:///test/doc2.yaml'

    # adding more metadata and changing location
    doc = copy.deepcopy(updated.metadata_doc)
    doc['test2'] = {'some': 'other thing'}
    update = Dataset(ls5_nbar_gtiff_type, doc, 'file:///test/doc3.yaml')
    index.datasets.update(update)
    updated = index.datasets.get(dataset.id)
    assert updated.metadata_doc['test1'] == {'some': 'thing'}
    assert updated.metadata_doc['test2'] == {'some': 'other thing'}
    assert updated.local_uri == 'file:///test/doc3.yaml'

    # changing stuff isn't allowed by default
    doc = copy.deepcopy(updated.metadata_doc)
    doc['product_type'] = 'foobar'
    update = Dataset(ls5_nbar_gtiff_type, doc, 'file:///test/doc4.yaml')
    with pytest.raises(ValueError):
        index.datasets.update(update)
    updated = index.datasets.get(dataset.id)
    assert updated.metadata_doc['test1'] == {'some': 'thing'}
    assert updated.metadata_doc['test2'] == {'some': 'other thing'}
    assert updated.metadata_doc['product_type'] == 'nbar'
    assert updated.local_uri == 'file:///test/doc3.yaml'

    # allowed changes go through
    doc = copy.deepcopy(updated.metadata_doc)
    doc['product_type'] = 'foobar'
    update = Dataset(ls5_nbar_gtiff_type, doc, 'file:///test/doc5.yaml')
    index.datasets.update(update, {('product_type',): changes.allow_any})
    updated = index.datasets.get(dataset.id)
    assert updated.metadata_doc['test1'] == {'some': 'thing'}
    assert updated.metadata_doc['test2'] == {'some': 'other thing'}
    assert updated.metadata_doc['product_type'] == 'foobar'
    assert updated.local_uri == 'file:///test/doc5.yaml'


def test_update_dataset_type(index, ls5_nbar_gtiff_type, ls5_nbar_gtiff_doc, default_metadata_type_doc):
    """
    :type ls5_nbar_gtiff_type: datacube.model.DatasetType
    :type index: datacube.index._api.Index
    """
    assert index.products.get_by_name(ls5_nbar_gtiff_type.name) is not None

    # Update with a new description
    ls5_nbar_gtiff_doc['description'] = "New description"
    index.products.update_document(ls5_nbar_gtiff_doc)
    # Ensure was updated
    assert index.products.get_by_name(ls5_nbar_gtiff_type.name).definition['description'] == "New description"

    # Remove some match rules (looser rules -- that match more datasets -- should be allowed)
    assert 'format' in ls5_nbar_gtiff_doc['metadata']
    del ls5_nbar_gtiff_doc['metadata']['format']['name']
    del ls5_nbar_gtiff_doc['metadata']['format']
    index.products.update_document(ls5_nbar_gtiff_doc)
    # Ensure was updated
    updated_type = index.products.get_by_name(ls5_nbar_gtiff_type.name)
    assert updated_type.definition['metadata'] == ls5_nbar_gtiff_doc['metadata']

    # Specifying metadata type definition (rather than name) should be allowed
    full_doc = copy.deepcopy(ls5_nbar_gtiff_doc)
    full_doc['metadata_type'] = default_metadata_type_doc
    index.products.update_document(full_doc)

    # Remove fixed field, forcing a new index to be created (as datasets can now differ for the field).
    assert not _object_exists(index._db, 'dix_ls5_nbart_p54_gtiff_product_type')
    del ls5_nbar_gtiff_doc['metadata']['product_type']
    index.products.update_document(ls5_nbar_gtiff_doc)
    # Ensure was updated
    assert _object_exists(index._db, 'dix_ls5_nbart_p54_gtiff_product_type')
    updated_type = index.products.get_by_name(ls5_nbar_gtiff_type.name)
    assert updated_type.definition['metadata'] == ls5_nbar_gtiff_doc['metadata']

    # But if we make metadata more restrictive we get an error:
    different_telemetry_type = copy.deepcopy(ls5_nbar_gtiff_doc)
    assert 'ga_label' not in different_telemetry_type['metadata']
    different_telemetry_type['metadata']['ga_label'] = 'something'
    with pytest.raises(ValueError):
        index.products.update_document(different_telemetry_type)
    # Check was not updated.
    updated_type = index.products.get_by_name(ls5_nbar_gtiff_type.name)
    assert 'ga_label' not in updated_type.definition['metadata']

    # But works when unsafe updates are allowed.
    index.products.update_document(different_telemetry_type, allow_unsafe_updates=True)
    updated_type = index.products.get_by_name(ls5_nbar_gtiff_type.name)
    assert updated_type.definition['metadata']['ga_label'] == 'something'


def test_update_metadata_type(index, default_metadata_type_docs, default_metadata_type):
    """
    :type default_metadata_type_docs: list[dict]
    :type index: datacube.index._api.Index
    """
    mt_doc = [d for d in default_metadata_type_docs if d['name'] == default_metadata_type.name][0]

    assert index.metadata_types.get_by_name(mt_doc['name']) is not None

    # Update with no changes should work.
    index.metadata_types.update_document(mt_doc)

    # Add search field
    mt_doc['dataset']['search_fields']['testfield'] = {
        'description': "Field added for testing",
        'offset': ['test']
    }

    # TODO: Able to remove fields?
    # Indexes will be difficult to handle, as dropping them may affect other users. But leaving them there may
    # lead to issues if a different field is created with the same name.

    index.metadata_types.update_document(mt_doc)
    # Ensure was updated
    updated_type = index.metadata_types.get_by_name(mt_doc['name'])
    assert 'testfield' in updated_type.dataset_fields

    # But if we change an existing field type we get an error:
    different_mt_doc = copy.deepcopy(mt_doc)
    different_mt_doc['dataset']['search_fields']['time']['type'] = 'numeric-range'
    with pytest.raises(ValueError):
        index.metadata_types.update_document(different_mt_doc)

    # But works when unsafe updates are allowed.
    index.metadata_types.update_document(different_mt_doc, allow_unsafe_updates=True)
    updated_type = index.metadata_types.get_by_name(mt_doc['name'])
    assert isinstance(updated_type.dataset_fields['time'], NumericRangeDocField)


def test_filter_types_by_fields(index, ls5_nbar_gtiff_type):
    """
    :type ls5_nbar_gtiff_type: datacube.model.DatasetType
    :type index: datacube.index._api.Index
    """
    assert index.products
    res = list(index.products.get_with_fields(['lat', 'lon', 'platform']))
    assert res == [ls5_nbar_gtiff_type]

    res = list(index.products.get_with_fields(['lat', 'lon', 'platform', 'favorite_icecream']))
    assert len(res) == 0


def test_filter_types_by_search(index, ls5_nbar_gtiff_type):
    """
    :type ls5_nbar_gtiff_type: datacube.model.DatasetType
    :type index: datacube.index._api.Index
    """
    assert index.products

    # No arguments, return all.
    res = list(index.products.search())
    assert res == [ls5_nbar_gtiff_type]

    # Matching fields
    res = list(index.products.search(
        product_type='nbart',
        product='ls5_nbart_p54_gtiff'
    ))
    assert res == [ls5_nbar_gtiff_type]

    # Matching fields and non-available fields
    res = list(index.products.search(
        product_type='nbart',
        product='ls5_nbart_p54_gtiff',
        lat=Range(142.015625, 142.015625),
        lon=Range(-12.046875, -12.046875)
    ))
    assert res == []

    # Matching fields and available fields
    [(res, q)] = list(index.products.search_robust(
        product_type='nbart',
        product='ls5_nbart_p54_gtiff',
        lat=Range(142.015625, 142.015625),
        lon=Range(-12.046875, -12.046875)
    ))
    assert res == ls5_nbar_gtiff_type
    assert 'lat' in q
    assert 'lon' in q

    # Or expression test
    res = list(index.products.search(
        product_type=['nbart', 'nbar'],
    ))
    assert res == [ls5_nbar_gtiff_type]

    # Mismatching fields
    res = list(index.products.search(
        product_type='nbar',
    ))
    assert res == []

# coding=utf-8
"""
Module
"""

import copy
import pytest
import yaml

from datacube.drivers.postgres._fields import NumericRangeDocField, PgField
from datacube.index.index import Index
from datacube.index._metadata_types import default_metadata_type_docs
from datacube.model import MetadataType, DatasetType
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


def test_metadata_indexes_views_exist(initialised_postgres_db, default_metadata_type):
    """
    :type initialised_postgres_db: datacube.drivers.postgres._connections.PostgresDb
    :type default_metadata_type: datacube.model.MetadataType
    """
    # Metadata indexes should no longer exist.
    assert not _object_exists(initialised_postgres_db, 'dix_eo_platform')

    # Ensure view was created (following naming conventions)
    assert _object_exists(initialised_postgres_db, 'dv_eo_dataset')


def test_dataset_indexes_views_exist(initialised_postgres_db, ls5_telem_type):
    """
    :type initialised_postgres_db: datacube.drivers.postgres._connections.PostgresDb
    :type ls5_telem_type: datacube.model.DatasetType
    """
    assert ls5_telem_type.name == 'ls5_telem_test'

    # Ensure field indexes were created for the dataset type (following the naming conventions):
    assert _object_exists(initialised_postgres_db, "dix_ls5_telem_test_orbit")

    # Ensure it does not create a 'platform' index, because that's a fixed field
    # (ie. identical in every dataset of the type)
    assert not _object_exists(initialised_postgres_db, "dix_ls5_telem_test_platform")

    # Ensure view was created (following naming conventions)
    assert _object_exists(initialised_postgres_db, 'dv_ls5_telem_test_dataset')

    # Ensure view was created (following naming conventions)
    assert not _object_exists(initialised_postgres_db,
                              'dix_ls5_telem_test_gsi'), "indexed=false field gsi shouldn't have an index"


def test_dataset_composite_indexes_exist(initialised_postgres_db, ls5_telem_type):
    # This type has fields named lat/lon/time, so composite indexes should now exist for them:
    # (following the naming conventions)
    assert _object_exists(initialised_postgres_db, "dix_ls5_telem_test_sat_path_sat_row_time")

    # But no individual field indexes for these
    assert not _object_exists(initialised_postgres_db, "dix_ls5_telem_test_sat_path")
    assert not _object_exists(initialised_postgres_db, "dix_ls5_telem_test_sat_row")
    assert not _object_exists(initialised_postgres_db, "dix_ls5_telem_test_time")


def test_field_expression_unchanged(default_metadata_type, telemetry_metadata_type):
    # type: (MetadataType, MetadataType) -> None

    # We're checking for accidental changes here in our field-to-SQL code

    # If we started outputting a different expression they would quietly no longer match the expression
    # indexes that exist in our DBs.

    # The time field on the default 'eo' metadata type.
    field = default_metadata_type.dataset_fields['time']
    assert isinstance(field, PgField)
    assert field.sql_expression == (
        "tstzrange("
        "least("
        "agdc.common_timestamp(agdc.dataset.metadata #>> '{extent, from_dt}'), "
        "agdc.common_timestamp(agdc.dataset.metadata #>> '{extent, center_dt}')"
        "), greatest("
        "agdc.common_timestamp(agdc.dataset.metadata #>> '{extent, to_dt}'), "
        "agdc.common_timestamp(agdc.dataset.metadata #>> '{extent, center_dt}')"
        "), '[]')"
    )

    field = default_metadata_type.dataset_fields['lat']
    assert isinstance(field, PgField)
    assert field.sql_expression == (
        "agdc.float8range("
        "least("
        "CAST(agdc.dataset.metadata #>> '{extent, coord, ur, lat}' AS DOUBLE PRECISION), "
        "CAST(agdc.dataset.metadata #>> '{extent, coord, lr, lat}' AS DOUBLE PRECISION), "
        "CAST(agdc.dataset.metadata #>> '{extent, coord, ul, lat}' AS DOUBLE PRECISION), "
        "CAST(agdc.dataset.metadata #>> '{extent, coord, ll, lat}' AS DOUBLE PRECISION)), "
        "greatest("
        "CAST(agdc.dataset.metadata #>> '{extent, coord, ur, lat}' AS DOUBLE PRECISION), "
        "CAST(agdc.dataset.metadata #>> '{extent, coord, lr, lat}' AS DOUBLE PRECISION), "
        "CAST(agdc.dataset.metadata #>> '{extent, coord, ul, lat}' AS DOUBLE PRECISION), "
        "CAST(agdc.dataset.metadata #>> '{extent, coord, ll, lat}' AS DOUBLE PRECISION)"
        "), '[]')"
    )

    # A single string value
    field = default_metadata_type.dataset_fields['platform']
    assert isinstance(field, PgField)
    assert field.sql_expression == (
        "agdc.dataset.metadata #>> '{platform, code}'"
    )

    # A single integer value
    field = telemetry_metadata_type.dataset_fields['orbit']
    assert isinstance(field, PgField)
    assert field.sql_expression == (
        "CAST(agdc.dataset.metadata #>> '{acquisition, platform_orbit}' AS INTEGER)"
    )


def _object_exists(db, index_name):
    with db.connect() as connection:
        val = connection._connection.execute("SELECT to_regclass('agdc.%s')" % index_name).scalar()
    return val == ('agdc.%s' % index_name)


def test_idempotent_add_dataset_type(index, ls5_telem_type, ls5_telem_doc):
    """
    :type ls5_telem_type: datacube.model.DatasetType
    :type index: datacube.index._api.Index
    """
    assert index.products.get_by_name(ls5_telem_type.name) is not None

    # Re-add should have no effect, because it's equal to the current one.
    index.products.add_document(ls5_telem_doc)

    # But if we add the same type with differing properties we should get an error:
    different_telemetry_type = copy.deepcopy(ls5_telem_doc)
    different_telemetry_type['metadata']['ga_label'] = 'something'
    with pytest.raises(changes.DocumentMismatchError):
        index.products.add_document(different_telemetry_type)

        # TODO: Support for adding/changing search fields?


def test_update_dataset(index, ls5_telem_doc, example_ls5_nbar_metadata_doc):
    """
    :type index: datacube.index._api.Index
    """
    ls5_telem_type = index.products.add_document(ls5_telem_doc)
    assert ls5_telem_type

    example_ls5_nbar_metadata_doc['lineage']['source_datasets'] = {}
    dataset = Dataset(ls5_telem_type, example_ls5_nbar_metadata_doc, uris=['file:///test/doc.yaml'], sources={})
    dataset = index.datasets.add(dataset)
    assert dataset

    # update with the same doc should do nothing
    index.datasets.update(dataset)
    updated = index.datasets.get(dataset.id)
    assert updated.local_uri == 'file:///test/doc.yaml'
    assert updated.uris == ['file:///test/doc.yaml']

    # update location
    assert index.datasets.get(dataset.id).local_uri == 'file:///test/doc.yaml'
    update = Dataset(ls5_telem_type, example_ls5_nbar_metadata_doc, uris=['file:///test/doc2.yaml'], sources={})
    index.datasets.update(update)
    updated = index.datasets.get(dataset.id)

    # New locations are appended on update.
    # They may be indexing the same dataset from a different location: we don't want to remove the original location.
    # Returns the most recently added
    assert updated.local_uri == 'file:///test/doc2.yaml'
    # But both still exist (newest-to-oldest order)
    assert updated.uris == ['file:///test/doc2.yaml', 'file:///test/doc.yaml']

    # adding more metadata should always be allowed
    doc = copy.deepcopy(updated.metadata_doc)
    doc['test1'] = {'some': 'thing'}
    update = Dataset(ls5_telem_type, doc, uris=updated.uris)
    index.datasets.update(update)
    updated = index.datasets.get(dataset.id)
    assert updated.metadata_doc['test1'] == {'some': 'thing'}
    assert updated.local_uri == 'file:///test/doc2.yaml'
    assert len(updated.uris) == 2

    # adding more metadata and changing location
    doc = copy.deepcopy(updated.metadata_doc)
    doc['test2'] = {'some': 'other thing'}
    update = Dataset(ls5_telem_type, doc, uris=['file:///test/doc3.yaml'])
    index.datasets.update(update)
    updated = index.datasets.get(dataset.id)
    assert updated.metadata_doc['test1'] == {'some': 'thing'}
    assert updated.metadata_doc['test2'] == {'some': 'other thing'}
    assert updated.local_uri == 'file:///test/doc3.yaml'
    assert len(updated.uris) == 3

    # changing existing metadata fields isn't allowed by default
    doc = copy.deepcopy(updated.metadata_doc)
    doc['product_type'] = 'foobar'
    update = Dataset(ls5_telem_type, doc, uris=['file:///test/doc4.yaml'])
    with pytest.raises(ValueError):
        index.datasets.update(update)
    updated = index.datasets.get(dataset.id)
    assert updated.metadata_doc['test1'] == {'some': 'thing'}
    assert updated.metadata_doc['test2'] == {'some': 'other thing'}
    assert updated.metadata_doc['product_type'] == 'nbar'
    assert updated.local_uri == 'file:///test/doc3.yaml'
    assert len(updated.uris) == 3

    # allowed changes go through
    doc = copy.deepcopy(updated.metadata_doc)
    doc['product_type'] = 'foobar'


def test_update_dataset_type(index, ls5_telem_type, ls5_telem_doc, ga_metadata_type_doc):
    """
    :type ls5_telem_type: datacube.model.DatasetType
    :type index: datacube.index._api.Index
    """
    assert index.products.get_by_name(ls5_telem_type.name) is not None

    # Update with a new description
    ls5_telem_doc['description'] = "New description"
    index.products.update_document(ls5_telem_doc)
    # Ensure was updated
    assert index.products.get_by_name(ls5_telem_type.name).definition['description'] == "New description"

    # Remove some match rules (looser rules -- that match more datasets -- should be allowed)
    assert 'format' in ls5_telem_doc['metadata']
    del ls5_telem_doc['metadata']['format']['name']
    del ls5_telem_doc['metadata']['format']
    index.products.update_document(ls5_telem_doc)
    # Ensure was updated
    updated_type = index.products.get_by_name(ls5_telem_type.name)
    assert updated_type.definition['metadata'] == ls5_telem_doc['metadata']

    # Specifying metadata type definition (rather than name) should be allowed
    full_doc = copy.deepcopy(ls5_telem_doc)
    full_doc['metadata_type'] = ga_metadata_type_doc
    index.products.update_document(full_doc)

    # Remove fixed field, forcing a new index to be created (as datasets can now differ for the field).
    assert not _object_exists(index._db, 'dix_ls5_telem_test_product_type')
    del ls5_telem_doc['metadata']['product_type']
    index.products.update_document(ls5_telem_doc)
    # Ensure was updated
    assert _object_exists(index._db, 'dix_ls5_telem_test_product_type')
    updated_type = index.products.get_by_name(ls5_telem_type.name)
    assert updated_type.definition['metadata'] == ls5_telem_doc['metadata']

    # But if we make metadata more restrictive we get an error:
    different_telemetry_type = copy.deepcopy(ls5_telem_doc)
    assert 'ga_label' not in different_telemetry_type['metadata']
    different_telemetry_type['metadata']['ga_label'] = 'something'
    with pytest.raises(ValueError):
        index.products.update_document(different_telemetry_type)
    # Check was not updated.
    updated_type = index.products.get_by_name(ls5_telem_type.name)
    assert 'ga_label' not in updated_type.definition['metadata']

    # But works when unsafe updates are allowed.
    index.products.update_document(different_telemetry_type, allow_unsafe_updates=True)
    updated_type = index.products.get_by_name(ls5_telem_type.name)
    assert updated_type.definition['metadata']['ga_label'] == 'something'


def test_product_update_cli(index: Index,
                            clirunner,
                            ls5_telem_type: DatasetType,
                            ls5_telem_doc: dict,
                            ga_metadata_type: MetadataType,
                            tmpdir) -> None:
    """
    Test updating products via cli
    """

    def run_update_product(file_path, allow_unsafe=False):
        if allow_unsafe:
            allow_unsafe = ['--allow-unsafe']
        else:
            allow_unsafe = []

        return clirunner(
            [
                'product', 'update', str(file_path)
            ] + allow_unsafe, catch_exceptions=False,
            expect_success=False
        )

    def get_current(index, product_doc):
        # It's calling out to a separate instance to update the product (through the cli),
        # so we need to clear our local index object's cache to get the updated one.
        index.products.get_by_name_unsafe.cache_clear()

        return index.products.get_by_name(product_doc['name']).definition

    # Update an unchanged file, should be unchanged.
    file_path = tmpdir.join('unmodified-product.yaml')
    file_path.write(_to_yaml(ls5_telem_doc))
    result = run_update_product(file_path)
    assert str('Updated "ls5_telem_test"') in result.output
    assert get_current(index, ls5_telem_doc) == ls5_telem_doc
    assert result.exit_code == 0

    # Try to add an unknown property: this should be forbidden by validation of dataset-type-schema.yaml
    modified_doc = copy.deepcopy(ls5_telem_doc)
    modified_doc['newly_added_property'] = {}
    file_path = tmpdir.join('invalid-product.yaml')
    file_path.write(_to_yaml(modified_doc))
    result = run_update_product(file_path)

    # The error message differs between jsonschema versions, but should always mention the invalid property name.
    assert "newly_added_property" in result.output
    # Return error code for failure!
    assert result.exit_code == 1
    assert get_current(index, ls5_telem_doc) == ls5_telem_doc

    # Use of a numeric key in the document
    # (This has thrown errors in the past. all dict keys are strings after json conversion, but some old docs use
    # numbers as keys in yaml)
    modified_doc = copy.deepcopy(ls5_telem_doc)
    modified_doc['metadata'][42] = 'hello'
    file_path = tmpdir.join('unsafe-change-to-product.yaml')
    file_path.write(_to_yaml(modified_doc))
    result = run_update_product(file_path)
    assert "Unsafe change in metadata.42 from missing to 'hello'" in result.output
    # Return error code for failure!
    assert result.exit_code == 1
    # Unchanged
    assert get_current(index, ls5_telem_doc) == ls5_telem_doc

    # But if we set allow-unsafe==True, this one will work.
    result = run_update_product(file_path, allow_unsafe=True)
    assert "Unsafe change in metadata.42 from missing to 'hello'" in result.output
    assert result.exit_code == 0
    # Has changed, and our key is now a string (json only allows string keys)
    modified_doc = copy.deepcopy(ls5_telem_doc)
    modified_doc['metadata']['42'] = 'hello'
    assert get_current(index, ls5_telem_doc) == modified_doc


def _to_yaml(ls5_telem_doc):
    # Need to explicitly allow unicode in Py2
    return yaml.safe_dump(ls5_telem_doc, allow_unicode=True)


def test_update_metadata_type(index, default_metadata_type):
    """
    :type default_metadata_type_docs: list[dict]
    :type index: datacube.index._api.Index
    """
    mt_doc = [d for d in default_metadata_type_docs() if d['name'] == default_metadata_type.name][0]

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


def test_filter_types_by_fields(index, ls5_telem_type):
    """
    :type ls5_telem_type: datacube.model.DatasetType
    :type index: datacube.index._api.Index
    """
    assert index.products
    res = list(index.products.get_with_fields(['sat_path', 'sat_row', 'platform']))
    assert res == [ls5_telem_type]

    res = list(index.products.get_with_fields(['sat_path', 'sat_row', 'platform', 'favorite_icecream']))
    assert len(res) == 0


def test_filter_types_by_search(index, ls5_telem_type):
    """
    :type ls5_telem_type: datacube.model.DatasetType
    :type index: datacube.index._api.Index
    """
    assert index.products

    # No arguments, return all.
    res = list(index.products.search())
    assert res == [ls5_telem_type]

    # Matching fields
    res = list(index.products.search(
        product_type='satellite_telemetry_data',
        product='ls5_telem_test'
    ))
    assert res == [ls5_telem_type]

    # Matching fields and non-available fields
    res = list(index.products.search(
        product_type='satellite_telemetry_data',
        product='ls5_telem_test',
        lat=Range(142.015625, 142.015625),
        lon=Range(-12.046875, -12.046875)
    ))
    assert res == []

    # Matching fields and available fields
    [(res, q)] = list(index.products.search_robust(
        product_type='satellite_telemetry_data',
        product='ls5_telem_test',
        sat_path=Range(142.015625, 142.015625),
        sat_row=Range(-12.046875, -12.046875)
    ))
    assert res == ls5_telem_type
    assert 'sat_path' in q
    assert 'sat_row' in q

    # Or expression test
    res = list(index.products.search(
        product_type=['satellite_telemetry_data', 'nbar'],
    ))
    assert res == [ls5_telem_type]

    # Mismatching fields
    res = list(index.products.search(
        product_type='nbar',
    ))
    assert res == []


def test_update_metadata_type_doc(initialised_postgres_db, index, ls5_telem_type):
    type_doc = copy.deepcopy(ls5_telem_type.metadata_type.definition)
    type_doc['dataset']['search_fields']['test_indexed'] = {
        'description': 'indexed test field',
        'offset': ['test', 'indexed']
    }
    type_doc['dataset']['search_fields']['test_not_indexed'] = {
        'description': 'not indexed test field',
        'offset': ['test', 'not', 'indexed'],
        'indexed': False
    }

    index.metadata_types.update_document(type_doc)

    assert ls5_telem_type.name == 'ls5_telem_test'
    assert _object_exists(initialised_postgres_db, "dix_ls5_telem_test_test_indexed")
    assert not _object_exists(initialised_postgres_db, "dix_ls5_telem_test_test_not_indexed")

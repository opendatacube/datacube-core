# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2024 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
"""
Module
"""

import copy
import pytest
import yaml
from sqlalchemy import text
from unittest import mock

from datacube.drivers.postgres._fields import NumericRangeDocField as PgrNumericRangeDocField, PgField as PgrPgField
from datacube.drivers.postgis._fields import NumericRangeDocField as PgsNumericRangeDocField, PgField as PgsPgField
from datacube.index import Index
from datacube.index.abstract import default_metadata_type_docs
from datacube.model import MetadataType, Product
from datacube.model import Range, Not, Dataset
from datacube.utils import changes
from datacube.utils.documents import documents_equal
from datacube.testutils import sanitise_doc

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


@pytest.mark.parametrize('datacube_env_name', ('datacube', ))
def test_metadata_indexes_views_exist(index, default_metadata_type):
    """
    :type initialised_postgres_db: datacube.drivers.postgres._connections.PostgresDb
    :type default_metadata_type: datacube.model.MetadataType
    """
    # Metadata indexes should no longer exist.
    assert not _object_exists(index, 'dix_eo_platform')

    # Ensure view was created (following naming conventions)
    assert _object_exists(index, 'dv_eo_dataset')


@pytest.mark.parametrize('datacube_env_name', ('datacube', ))
def test_dataset_indexes_views_exist(index, ls5_telem_type):
    """
    :type initialised_postgres_db: datacube.drivers.postgres._connections.PostgresDb
    :type ls5_telem_type: datacube.model.DatasetType
    """
    assert ls5_telem_type.name == 'ls5_telem_test'

    # Ensure field indexes were created for the dataset type (following the naming conventions):
    assert _object_exists(index, "dix_ls5_telem_test_orbit")

    # Ensure it does not create a 'platform' index, because that's a fixed field
    # (ie. identical in every dataset of the type)
    assert not _object_exists(index, "dix_ls5_telem_test_platform")

    # Ensure view was created (following naming conventions)
    assert _object_exists(index, 'dv_ls5_telem_test_dataset')

    # Ensure view was created (following naming conventions)
    assert not _object_exists(index,
                              'dix_ls5_telem_test_gsi'), "indexed=false field gsi shouldn't have an index"


@pytest.mark.parametrize('datacube_env_name', ('datacube', ))
def test_dataset_composite_indexes_exist(index, ls5_telem_type):
    # This type has fields named lat/lon/time, so composite indexes should now exist for them:
    # (following the naming conventions)
    assert _object_exists(index, "dix_ls5_telem_test_sat_path_sat_row_time")

    # But no individual field indexes for these
    assert not _object_exists(index, "dix_ls5_telem_test_sat_path")
    assert not _object_exists(index, "dix_ls5_telem_test_sat_row")
    assert not _object_exists(index, "dix_ls5_telem_test_time")


@pytest.mark.parametrize('datacube_env_name', ('datacube', ))
def test_field_expression_unchanged(default_metadata_type: MetadataType, telemetry_metadata_type: MetadataType) -> None:
    # We're checking for accidental changes here in our field-to-SQL code

    # If we started outputting a different expression they would quietly no longer match the expression
    # indexes that exist in our DBs.

    # The time field on the default 'eo' metadata type.
    field = default_metadata_type.dataset_fields['time']
    assert isinstance(field, PgrPgField) or isinstance(field, PgsPgField)
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
    assert isinstance(field, PgrPgField) or isinstance(field, PgsPgField)
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
    assert isinstance(field, PgrPgField) or isinstance(field, PgsPgField)
    assert field.sql_expression == (
        "agdc.dataset.metadata #>> '{platform, code}'"
    )

    # A single integer value
    field = telemetry_metadata_type.dataset_fields['orbit']
    assert isinstance(field, PgrPgField) or isinstance(field, PgsPgField)
    assert field.sql_expression == (
        "CAST(agdc.dataset.metadata #>> '{acquisition, platform_orbit}' AS INTEGER)"
    )


def _object_exists(index, index_name):
    if index._db.driver_name == "postgis":
        schema_name = "odc"
    else:
        schema_name = "agdc"
    with index._active_connection() as connection:
        val = connection._connection.execute(text(f"SELECT to_regclass('{schema_name}.{index_name}')")).scalar()
    return val in (index_name, f'{schema_name}.{index_name}')


def test_idempotent_add_dataset_type(index, ls8_eo3_product, extended_eo3_product_doc):
    """
    :type ls5_telem_type: datacube.model.DatasetType
    :type index: datacube.index.Index
    """
    assert index.products.get_by_name(ls8_eo3_product.name) is not None

    # Re-add should have no effect, because it's equal to the current one.
    index.products.add_document(extended_eo3_product_doc)

    # But if we add the same type with differing properties we should get an error:
    different_product = copy.deepcopy(extended_eo3_product_doc)
    different_product['metadata']['properties']["eo:platform"] = 'spamsat-13-and-a-half'
    with pytest.raises(changes.DocumentMismatchError):
        index.products.add_document(different_product)

        # TODO: Support for adding/changing search fields?


@pytest.mark.parametrize('datacube_env_name', ('datacube', ))
def test_update_dataset(index, ls5_telem_doc, example_ls5_nbar_metadata_doc):
    """
    :type index: datacube.index.Index
    """
    ls5_telem_type = index.products.add_document(ls5_telem_doc)
    assert ls5_telem_type

    example_ls5_nbar_metadata_doc['lineage']['source_datasets'] = {}
    dataset = Dataset(ls5_telem_type, example_ls5_nbar_metadata_doc, uri='file:///test/doc.yaml', sources={})
    dataset = index.datasets.add(dataset)
    assert dataset

    # update with the same doc should do nothing
    index.datasets.update(dataset)
    updated = index.datasets.get(dataset.id)
    assert updated.local_uri == 'file:///test/doc.yaml'
    assert updated.uri == 'file:///test/doc.yaml'

    # update location
    assert index.datasets.get(dataset.id).local_uri == 'file:///test/doc.yaml'
    update = Dataset(ls5_telem_type, example_ls5_nbar_metadata_doc, uri='file:///test/doc2.yaml', sources={})
    index.datasets.update(update)
    updated = index.datasets.get(dataset.id)

    # New locations are no longer appended on update.
    # They may be indexing the same dataset from a different location: we don't want to remove the original location.
    # Returns the most recently added
    assert updated.local_uri == 'file:///test/doc2.yaml'
    assert updated._uris == ['file:///test/doc2.yaml']

    # adding more metadata should always be allowed
    doc = copy.deepcopy(updated.metadata_doc)
    doc['test1'] = {'some': 'thing'}
    update = Dataset(ls5_telem_type, doc, uri=updated.uri)
    index.datasets.update(update)
    updated = index.datasets.get(dataset.id)
    assert updated.metadata_doc['test1'] == {'some': 'thing'}
    assert updated.local_uri == 'file:///test/doc2.yaml'
    assert len(updated._uris) == 1

    # adding more metadata and changing location
    doc = copy.deepcopy(updated.metadata_doc)
    doc['test2'] = {'some': 'other thing'}
    update = Dataset(  # Test deprecated functionality
        ls5_telem_type, doc,
        uris=['file:///test/doc3.yaml', 'file:///test/doc3a.yaml']
    )
    index.datasets.update(update)
    updated = index.datasets.get(dataset.id)
    assert updated.metadata_doc['test1'] == {'some': 'thing'}
    assert updated.metadata_doc['test2'] == {'some': 'other thing'}
    assert updated.local_uri == 'file:///test/doc3.yaml'
    assert len(updated.uris) == 3  # Test deprecated property

    # changing existing metadata fields isn't allowed by default
    doc = copy.deepcopy(updated.metadata_doc)
    doc['product_type'] = 'foobar'
    update = Dataset(ls5_telem_type, doc, uri='file:///test/doc4.yaml')
    with pytest.raises(ValueError):
        index.datasets.update(update)
    updated = index.datasets.get(dataset.id)
    assert updated.metadata_doc['test1'] == {'some': 'thing'}
    assert updated.metadata_doc['test2'] == {'some': 'other thing'}
    assert updated.metadata_doc['product_type'] == 'nbar'
    assert updated.local_uri == 'file:///test/doc3.yaml'
    assert len(updated._uris) == 3

    # allowed changes go through
    doc = copy.deepcopy(updated.metadata_doc)
    doc['product_type'] = 'foobar'


@pytest.mark.parametrize('datacube_env_name', ('datacube', ))
def test_update_product_type(index, ls5_telem_type, ls5_telem_doc, ga_metadata_type_doc):
    """
    :type ls5_telem_type: datacube.model.Product
    :type index: datacube.index.Index
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
    assert not _object_exists(index, 'dix_ls5_telem_test_product_type')
    del ls5_telem_doc['metadata']['product_type']
    index.products.update_document(ls5_telem_doc)
    # Ensure was updated
    assert _object_exists(index, 'dix_ls5_telem_test_product_type')
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
                            ls8_eo3_product: Product,
                            extended_eo3_product_doc: dict,
                            extended_eo3_metadata_type: MetadataType,
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

        return sanitise_doc(index.products.get_by_name(product_doc['name']).definition)
    # Update an unchanged file, should be unchanged.
    file_path = tmpdir.join('unmodified-product.yaml')
    file_path.write(_to_yaml(extended_eo3_product_doc))
    result = run_update_product(file_path)
    assert str(f'Updated "{extended_eo3_product_doc["name"]}"') in result.output
    fresh = get_current(index, extended_eo3_product_doc)
    assert documents_equal(fresh, extended_eo3_product_doc)
    assert result.exit_code == 0

    # Try to add an unknown property: this should be forbidden by validation of dataset-type-schema.yaml
    modified_doc = copy.deepcopy(extended_eo3_product_doc)
    modified_doc['newly_added_property'] = {}
    file_path = tmpdir.join('invalid-product.yaml')
    file_path.write(_to_yaml(modified_doc))
    result = run_update_product(file_path)

    # The error message differs between jsonschema versions, but should always mention the invalid property name.
    assert "newly_added_property" in result.output
    # Return error code for failure!
    assert result.exit_code == 1
    fresh = get_current(index, extended_eo3_product_doc)
    assert documents_equal(fresh, extended_eo3_product_doc)

    # Use of a numeric key in the document
    # (This has thrown errors in the past. all dict keys are strings after json conversion, but some old docs use
    # numbers as keys in yaml)
    modified_doc = copy.deepcopy(extended_eo3_product_doc)
    modified_doc['metadata'][42] = 'hello'
    file_path = tmpdir.join('unsafe-change-to-product.yaml')
    file_path.write(_to_yaml(modified_doc))
    result = run_update_product(file_path)
    assert "Unsafe change in metadata.42 from missing to 'hello'" in result.output
    # Return error code for failure!
    assert result.exit_code == 1
    # Unchanged
    fresh = get_current(index, extended_eo3_product_doc)
    assert documents_equal(fresh, extended_eo3_product_doc)

    # But if we set allow-unsafe==True, this one will work.
    result = run_update_product(file_path, allow_unsafe=True)
    assert "Unsafe change in metadata.42 from missing to 'hello'" in result.output
    assert result.exit_code == 0
    # Has changed, and our key is now a string (json only allows string keys)
    modified_doc = copy.deepcopy(extended_eo3_product_doc)
    modified_doc['metadata']['42'] = 'hello'
    fresh = get_current(index, extended_eo3_product_doc)
    assert documents_equal(fresh, modified_doc)


@pytest.mark.parametrize('datacube_env_name', ('datacube', ))
def test_product_delete(index, ls8_eo3_product: Product):
    # test that postgres dynamic indexes and views are deleted
    assert index.products.get_by_name(ls8_eo3_product.name) is not None
    assert _object_exists(index, "dix_ga_ls8c_ard_3_region_code")
    assert _object_exists(index, "dv_ga_ls8c_ard_3_dataset")

    index.products.delete([ls8_eo3_product])

    index.products.get_by_name_unsafe.cache_clear()
    assert index.products.get_by_name(ls8_eo3_product.name) is None
    assert not _object_exists(index, "dix_ga_ls8c_ard_3_region_code")
    assert not _object_exists(index, "dv_ga_ls8c_ard_3_dataset")


def test_product_delete_cli(index: Index,
                            clirunner,
                            ls8_eo3_product: Product,
                            ls8_eo3_dataset, ls8_eo3_dataset2,
                            wo_eo3_product) -> None:
    from pathlib import Path
    TESTDIR = Path(__file__).parent.parent / "data" / "eo3"
    # product with some archived and some active datasets
    clirunner(['dataset', 'archive', 'c21648b1-a6fa-4de0-9dc3-9c445d8b295a'])

    runner = clirunner(['product', 'delete', 'ga_ls8c_ard_3', 'ga_ls_wo_3'], verbose_flag=False, expect_success=False)
    assert "1 out of 2 products successfully deleted" in runner.output
    assert "ga_ls_wo_3 could not be deleted" not in runner.output
    assert "ga_ls8c_ard_3 could not be deleted" in runner.output
    assert runner.exit_code == 0

    index.products.get_by_name_unsafe.cache_clear()
    assert index.products.get_by_name("ga_ls8c_ard_3") is not None
    assert index.products.get_by_name("ga_ls_wo_3") is None

    # force without confirmation
    runner = clirunner(['product', 'delete', 'ga_ls8c_ard_3', '--force'], verbose_flag=False, expect_success=False)
    assert "Proceed?" in runner.output
    assert runner.exit_code == 1

    # adding back product should cause error since it still exists
    add = clirunner(['product', 'add', str(TESTDIR / "ard_ls8.odc-product.yaml")])
    assert "is already in the database" in add.output

    # ensure deletion involving active datasets works with force
    with mock.patch('click.confirm', return_value=True):
        runner = clirunner(['product', 'delete', 'ga_ls8c_ard_3', '--force'], verbose_flag=False)
        assert "Completed product deletion" in runner.output
        assert runner.exit_code == 0

    runner = clirunner(['dataset', 'archive', '4a30d008-4e82-4d67-99af-28bc1629f766'],
                       verbose_flag=False, expect_success=False)
    assert "No dataset found with id" in runner.output

    index.products.get_by_name_unsafe.cache_clear()
    assert index.products.get_by_name("ga_ls8c_ard_3") is None

    # should be able to add product back now
    add = clirunner(['product', 'add', str(TESTDIR / "ard_ls8.odc-product.yaml")])
    assert "is already in the database" not in add.output


def _to_yaml(ls5_telem_doc):
    # Need to explicitly allow unicode in Py2
    return yaml.safe_dump(ls5_telem_doc, allow_unicode=True)


def test_update_metadata_type(index, default_metadata_type):
    """
    :type default_metadata_type_docs: list[dict]
    :type index: datacube.index.Index
    """
    mt_doc = [d for d in default_metadata_type_docs() if d['name'] == default_metadata_type.name][0]

    assert index.metadata_types.get_by_name(mt_doc['name']) is not None

    # Update with no changes should work.
    index.metadata_types.update_document(mt_doc)

    # Add search field
    mt_doc['dataset']['search_fields']['testfield'] = {
        'description': "Field added for testing",
        'offset': ['properties', 'test'],
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
    assert (
        isinstance(updated_type.dataset_fields['time'], PgrNumericRangeDocField)
        or isinstance(updated_type.dataset_fields['time'], PgsNumericRangeDocField)
    )


def test_filter_types_by_fields(index, wo_eo3_product):
    """
    :type ls5_telem_type: datacube.model.DatasetType
    :type index: datacube.index.Index
    """
    res = list(index.metadata_types.get_with_fields(['platform', 'instrument', 'region_code']))
    assert res == [wo_eo3_product.metadata_type]

    res = list(index.metadata_types.get_with_fields(['platform', 'instrument', 'region_code', 'favorite_icecream']))
    assert len(res) == 0

    res = list(index.products.get_with_fields(['platform', 'instrument', 'region_code']))
    assert res == [wo_eo3_product]

    res = list(index.products.get_with_fields(['platform', 'instrument', 'region_code', 'favorite_icecream']))
    assert len(res) == 0


def test_filter_products_by_types(index, wo_eo3_product):
    """
    :type ls5_telem_type: datacube.model.DatasetType
    :type index: datacube.index.Index
    """
    res = list(index.products.get_with_types([wo_eo3_product.metadata_type]))
    assert res == [wo_eo3_product]


def test_filter_types_by_search(index, wo_eo3_product, ls8_eo3_product):
    """
    :type ls5_telem_type: datacube.model.DatasetType
    :type index: datacube.index.Index
    """
    assert index.products

    # No arguments, return all.
    res = list(index.products.search())
    assert res == [ls8_eo3_product, wo_eo3_product]

    # Matching fields
    res = list(index.products.search(
        product_family='wo',
        product='ga_ls_wo_3'
    ))
    assert res == [wo_eo3_product]

    # Matching fields and non-available fields
    res = list(index.products.search(
        product_family='wo',
        product='ga_ls_wo_3',
        lat=Range(142.015625, 142.015625),
        lon=Range(-12.046875, -12.046875)
    ))
    assert res == []

    # Matching fields and available fields
    [(res, q)] = list(index.products.search_robust(
        product_family='wo',
        product='ga_ls_wo_3',
        cloud_cover=Range(0.015625, 0.2015625),
        dataset_maturity="final"
    ))
    assert res == wo_eo3_product
    assert 'cloud_cover' in q
    assert 'dataset_maturity' in q

    # Or expression test
    res = list(index.products.search(
        product_family=['wo', 'spam'],
    ))
    assert res == [wo_eo3_product]

    # Not expression test
    res = list(index.products.search(
        product_family=Not("wo"),
    ))
    assert res == [ls8_eo3_product]

    # Mismatching fields
    res = list(index.products.search(
        product_family='spam',
    ))
    assert res == []


@pytest.mark.parametrize('datacube_env_name', ('datacube', ))
def test_update_metadata_type_doc(index, ls5_telem_type):
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
    assert _object_exists(index, "dix_ls5_telem_test_test_indexed")
    assert not _object_exists(index, "dix_ls5_telem_test_test_not_indexed")

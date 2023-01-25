# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2022 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
"""
Module
"""
import datetime
from typing import Any

import pytest
import yaml
from dateutil import tz

import datacube.scripts.search_tool
from datacube.config import LocalConfig
from datacube.drivers.postgres._connections import DEFAULT_DB_USER
from datacube.index import Index
from datacube.model import Dataset
from datacube.model import Product
from datacube.model import Range

from datacube import Datacube
from .search_utils import _cli_csv_search, _csv_search_raw, _load_product_query
from datacube.utils.dates import tz_as_utc


def test_search_by_metadata(index: Index, ls8_eo3_product, wo_eo3_product):
    lds = list(index.products.search_by_metadata({"properties": {"product_family": "ard"}}))
    assert len(lds) == 0
    lds = list(index.products.search_by_metadata({"properties": {"odc:product_family": "ard"}}))
    assert len(lds) == 1
    lds = list(index.products.search_by_metadata({"properties": {"platform": "landsat-8"}}))
    assert len(lds) == 0
    lds = list(index.products.search_by_metadata({"properties": {"eo:platform": "landsat-8"}}))
    assert len(lds) == 1


def test_search_dataset_equals_eo3(index: Index, ls8_eo3_dataset: Dataset):
    datasets = index.datasets.search_eager(
        platform='landsat-8'
    )
    assert len(datasets) == 1
    assert datasets[0].id == ls8_eo3_dataset.id

    datasets = index.datasets.search_eager(
        platform='landsat-8',
        instrument='OLI_TIRS'
    )
    assert len(datasets) == 1
    assert datasets[0].id == ls8_eo3_dataset.id

    # Wrong product family
    with pytest.raises(ValueError):
        datasets = index.datasets.search_eager(
            platform='landsat-8',
            product_family='splunge',
        )


def test_search_dataset_by_metadata_eo3(index: Index, ls8_eo3_dataset: Dataset) -> None:
    datasets = index.datasets.search_by_metadata(
        {"properties": {"eo:platform": "landsat-8", "eo:instrument": "OLI_TIRS"}}
    )
    datasets = list(datasets)
    assert len(datasets) == 1
    assert datasets[0].id == ls8_eo3_dataset.id

    datasets = index.datasets.search_by_metadata(
        {"properties": {"eo:platform": "landsat-5", "eo:instrument": "TM"}}
    )
    datasets = list(datasets)
    assert len(datasets) == 0


def test_search_day_eo3(index: Index, ls8_eo3_dataset: Dataset) -> None:
    # Matches day
    datasets = index.datasets.search_eager(
        time=datetime.date(2016, 5, 12)
    )
    assert len(datasets) == 1
    assert datasets[0].id == ls8_eo3_dataset.id

    # Different day: no match
    datasets = index.datasets.search_eager(
        time=datetime.date(2016, 5, 13)
    )
    assert len(datasets) == 0


def test_search_dataset_ranges_eo3(index: Index, ls8_eo3_dataset: Dataset) -> None:
    # In the lat bounds.
    datasets = index.datasets.search_eager(
        lat=Range(-37.5, -36.5),
        time=Range(
            datetime.datetime(2016, 5, 12, 23, 0, 0),
            datetime.datetime(2016, 5, 12, 23, 59, 59)
        )
    )
    assert len(datasets) == 1
    assert datasets[0].id == ls8_eo3_dataset.id

    # Out of the lat bounds.
    datasets = index.datasets.search_eager(
        lat=Range(28, 32),
        time=Range(
            datetime.datetime(2016, 5, 12, 23, 0, 0),
            datetime.datetime(2016, 5, 12, 23, 59, 59)
        )
    )
    assert len(datasets) == 0

    # Out of the time bounds
    datasets = index.datasets.search_eager(
        lat=Range(-37.5, -36.5),
        time=Range(
            datetime.datetime(2014, 7, 26, 21, 48, 0),
            datetime.datetime(2014, 7, 26, 21, 50, 0)
        )
    )
    assert len(datasets) == 0

    # A dataset that overlaps but is not fully contained by the search bounds.
    # Should we distinguish between 'contains' and 'overlaps'?
    datasets = index.datasets.search_eager(
        lat=Range(-40, -37.1)
    )
    assert len(datasets) == 1
    assert datasets[0].id == ls8_eo3_dataset.id

    # Single point search
    datasets = index.datasets.search_eager(
        lat=-37.0,
        time=Range(
            datetime.datetime(2016, 5, 12, 23, 0, 0),
            datetime.datetime(2016, 5, 12, 23, 59, 59)
        )
    )
    assert len(datasets) == 1
    assert datasets[0].id == ls8_eo3_dataset.id

    datasets = index.datasets.search_eager(
        lat=30.0,
        time=Range(
            datetime.datetime(2016, 5, 12, 23, 0, 0),
            datetime.datetime(2016, 5, 12, 23, 59, 59)
        )
    )
    assert len(datasets) == 0

    # Single timestamp search
    datasets = index.datasets.search_eager(
        lat=Range(-37.5, -36.5),
        time=datetime.datetime(2016, 5, 12, 23, 50, 40),
    )
    assert len(datasets) == 1
    assert datasets[0].id == ls8_eo3_dataset.id

    datasets = index.datasets.search_eager(
        lat=Range(-37.5, -36.5),
        time=datetime.datetime(2016, 5, 12, 23, 0, 0)
    )
    assert len(datasets) == 0


def test_zero_width_range_search(index: Index, ls8_eo3_dataset4: Dataset) -> None:
    # Test time search against zero-width time metadata
    datasets = index.datasets.search_eager(time=Range(
        begin=datetime.datetime(2013, 7, 21, 0, 57, 26, 432563, tzinfo=datetime.timezone.utc),
        end=datetime.datetime(2013, 7, 21, 0, 57, 26, 432563, tzinfo=datetime.timezone.utc)
    ))
    assert len(datasets) == 1

    datasets = index.datasets.search_eager(time=Range(
        begin=datetime.datetime(2013, 7, 21, 0, 57, 26, 432563, tzinfo=datetime.timezone.utc),
        end=datetime.datetime(2013, 7, 21, 0, 57, 27, 432563, tzinfo=datetime.timezone.utc)
    ))
    assert len(datasets) == 1

    datasets = index.datasets.search_eager(time=Range(
        begin=datetime.datetime(2013, 7, 21, 0, 57, 25, 432563, tzinfo=datetime.timezone.utc),
        end=datetime.datetime(2013, 7, 21, 0, 57, 26, 432563, tzinfo=datetime.timezone.utc)
    ))
    assert len(datasets) == 1


def test_search_globally_eo3(index: Index, ls8_eo3_dataset: Dataset) -> None:
    # No expressions means get all.
    results = list(index.datasets.search())
    assert len(results) == 1

    # Dataset sources aren't loaded by default
    assert results[0].sources is None


def test_search_by_product_eo3(index: Index,
                               base_eo3_product_doc: Product,
                               ls8_eo3_dataset: Dataset,
                               wo_eo3_dataset: Dataset) -> None:
    # Query all the test data, the counts should match expected
    results = _load_product_query(index.datasets.search_by_product())
    assert len(results) == 2
    dataset_count = sum(len(ds) for ds in results.values())
    assert dataset_count == 2

    # Query one product
    products = _load_product_query(index.datasets.search_by_product(
        platform='landsat-8',
        product_family='wo'
    ))
    assert len(products) == 1
    [dataset] = products[base_eo3_product_doc["name"]]
    assert dataset.id == wo_eo3_dataset.id
    assert dataset.is_eo3
    assert dataset.type == dataset.product


def test_search_limit_eo3(index, ls8_eo3_dataset, ls8_eo3_dataset2, wo_eo3_dataset):
    prod = ls8_eo3_dataset.product.name
    datasets = list(index.datasets.search(product=prod))
    assert len(datasets) == 2
    datasets = list(index.datasets.search(limit=1, product=prod))
    ids = [ds.id for ds in datasets]
    assert len(ids) == 1
    assert len(datasets) == 1
    datasets = list(index.datasets.search(limit=0, product=prod))
    assert len(datasets) == 0
    datasets = list(index.datasets.search(limit=5, product=prod))
    assert len(datasets) == 2

    datasets = list(index.datasets.search_returning(('id',), product=prod))
    assert len(datasets) == 2
    datasets = list(index.datasets.search_returning(('id',), limit=1, product=prod))
    assert len(datasets) == 1
    datasets = list(index.datasets.search_returning(('id',), limit=0, product=prod))
    assert len(datasets) == 0
    datasets = list(index.datasets.search_returning(('id',), limit=5, product=prod))
    assert len(datasets) == 2

    # Limit is per product not overall.  (But why?!?)
    datasets = list(index.datasets.search())
    assert len(datasets) == 3
    datasets = list(index.datasets.search(limit=1))
    assert len(datasets) == 2
    datasets = list(index.datasets.search(limit=0))
    assert len(datasets) == 0
    datasets = list(index.datasets.search(limit=5))
    assert len(datasets) == 3

    datasets = list(index.datasets.search_returning(('id',)))
    assert len(datasets) == 3
    datasets = list(index.datasets.search_returning(('id',), limit=1))
    assert len(datasets) == 2
    datasets = list(index.datasets.search_returning(('id',), limit=0))
    assert len(datasets) == 0
    datasets = list(index.datasets.search_returning(('id',), limit=5))
    assert len(datasets) == 3


def test_search_or_expressions_eo3(index: Index,
                                   ls8_eo3_dataset: Dataset,
                                   ls8_eo3_dataset2: Dataset,
                                   wo_eo3_dataset: Dataset) -> None:
    # Three EO3 datasets:
    # - two landsat8 ard
    # - one wo

    all_datasets = index.datasets.search_eager()
    assert len(all_datasets) == 3
    all_ids = set(dataset.id for dataset in all_datasets)

    # OR all instruments: should return all datasets
    datasets = index.datasets.search_eager(
        instrument=['WOOLI_TIRS', 'OLI_TIRS', 'OLI_TIRS2']
    )
    assert len(datasets) == 3
    ids = set(dataset.id for dataset in datasets)
    assert ids == all_ids

    # OR expression with only one clause.
    datasets = index.datasets.search_eager(
        instrument=['OLI_TIRS']
    )
    assert len(datasets) == 1
    assert datasets[0].id == ls8_eo3_dataset.id

    # OR both products: return all
    datasets = index.datasets.search_eager(
        product=[ls8_eo3_dataset.product.name, wo_eo3_dataset.product.name]
    )
    assert len(datasets) == 3
    ids = set(dataset.id for dataset in datasets)
    assert ids == all_ids

    # eo OR eo3: return all
    datasets = index.datasets.search_eager(
        metadata_type=[
            # LS5 + children
            ls8_eo3_dataset.metadata_type.name,
            # Nothing
            # LS8 dataset
            wo_eo3_dataset.metadata_type.name
        ]
    )
    assert len(datasets) == 3
    ids = set(dataset.id for dataset in datasets)
    assert ids == all_ids

    # Redundant ORs should have no effect.
    datasets = index.datasets.search_eager(
        product=[wo_eo3_dataset.product.name, wo_eo3_dataset.product.name, wo_eo3_dataset.product.name]
    )
    assert len(datasets) == 1
    assert datasets[0].id == wo_eo3_dataset.id


def test_search_returning_eo3(index: Index,
                              local_config: LocalConfig,
                              ls8_eo3_dataset: Dataset,
                              ls8_eo3_dataset2: Dataset,
                              wo_eo3_dataset: Dataset) -> None:
    assert index.datasets.count() == 3, "Expected three test datasets"

    # Expect one product with our one dataset.
    results = list(index.datasets.search_returning(
        ('id', 'region_code', 'dataset_maturity'),
        platform='landsat-8',
        instrument='OLI_TIRS',
    ))
    assert len(results) == 1
    id_, region_code, maturity = results[0]
    assert id_ == ls8_eo3_dataset.id
    assert region_code == '090086'
    assert maturity == 'final'

    results = list(index.datasets.search_returning(
        ('id', 'metadata_doc',),
        platform='landsat-8',
        instrument='OLI_TIRS',
    ))
    assert len(results) == 1
    id_, document = results[0]
    assert id_ == ls8_eo3_dataset.id
    assert document == ls8_eo3_dataset.metadata_doc

    my_username = local_config.get('db_username', DEFAULT_DB_USER)

    # Mixture of document and native fields
    results = list(index.datasets.search_returning(
        ('id', 'creation_time', 'format', 'label'),
        platform='landsat-8',
        instrument='OLI_TIRS',
        indexed_by=my_username,
    ))
    assert len(results) == 1

    id_, creation_time, format_, label = results[0]

    assert id_ == ls8_eo3_dataset.id
    assert format_ == 'GeoTIFF'

    # It's always UTC in the document
    expected_time = creation_time.astimezone(tz.tzutc()).replace(tzinfo=None)
    assert expected_time.isoformat() == ls8_eo3_dataset.metadata.creation_dt
    assert label == ls8_eo3_dataset.metadata.label


def test_search_returning_rows_eo3(index,
                                   eo3_ls8_dataset_doc,
                                   eo3_ls8_dataset2_doc,
                                   ls8_eo3_dataset, ls8_eo3_dataset2):
    dataset = ls8_eo3_dataset
    uri = eo3_ls8_dataset_doc[1]
    uri3 = eo3_ls8_dataset2_doc[1]
    results = list(index.datasets.search_returning(
        ('id', 'uri'),
        platform='landsat-8',
        instrument='OLI_TIRS',
    ))
    assert len(results) == 1
    assert results == [(dataset.id, uri)]

    index.datasets.archive_location(dataset.id, uri)
    index.datasets.remove_location(dataset.id, uri)

    # If returning a field like uri, there will be one result per location.
    # No locations
    results = list(index.datasets.search_returning(
        ('id', 'uri'),
        platform='landsat-8',
        instrument='OLI_TIRS',
    ))
    assert len(results) == 0

    # Add a second location and we should get two results
    index.datasets.add_location(dataset.id, uri)
    uri2 = 'file:///tmp/test2'
    index.datasets.add_location(dataset.id, uri2)
    results = set(index.datasets.search_returning(
        ('id', 'uri'),
        platform='landsat-8',
        instrument='OLI_TIRS',
    ))
    assert len(results) == 2
    assert results == {
        (dataset.id, uri),
        (dataset.id, uri2)
    }

    # A second dataset already has a location:
    results = set(index.datasets.search_returning(
        ('id', 'uri'),
        platform='landsat-8',
        dataset_maturity='final',
    ))
    assert len(results) == 3
    assert results == {
        (dataset.id, uri),
        (dataset.id, uri2),
        (ls8_eo3_dataset2.id, uri3),
    }


def test_searches_only_type_eo3(index: Index,
                                wo_eo3_dataset: Dataset,
                                ls8_eo3_dataset: Dataset) -> None:
    assert ls8_eo3_dataset.metadata_type.name != wo_eo3_dataset.metadata_type.name

    # One result in the product
    datasets = index.datasets.search_eager(
        product=wo_eo3_dataset.product.name,
        platform='landsat-8'
    )
    assert len(datasets) == 1
    assert datasets[0].id == wo_eo3_dataset.id

    # One result in the metadata type
    datasets = index.datasets.search_eager(
        metadata_type="eo3",
        platform='landsat-8'
    )
    assert len(datasets) == 1
    assert datasets[0].id == wo_eo3_dataset.id

    # No results when searching for a different dataset type.
    with pytest.raises(ValueError):
        datasets = index.datasets.search_eager(
            product="spam_and_eggs",
            platform='landsat-8'
        )

    # Two result when no types specified.
    datasets = index.datasets.search_eager(
        platform='landsat-8'
    )
    assert len(datasets) == 2
    assert set(ds.id for ds in datasets) == {ls8_eo3_dataset.id, wo_eo3_dataset.id}

    # No results for different metadata type.
    with pytest.raises(ValueError):
        datasets = index.datasets.search_eager(
            metadata_type='spam_type',
            platform='landsat-8',
        )


def test_search_special_fields_eo3(index: Index,
                                   ls8_eo3_dataset: Dataset,
                                   wo_eo3_dataset: Dataset) -> None:
    # 'product' is a special case
    datasets = index.datasets.search_eager(
        product=ls8_eo3_dataset.product.name
    )
    assert len(datasets) == 1
    assert datasets[0].id == ls8_eo3_dataset.id

    # Unknown field: no results
    with pytest.raises(ValueError):
        datasets = index.datasets.search_eager(
            platform='landsat-8',
            flavour='vanilla',
        )


def test_search_by_uri_eo3(index, ls8_eo3_dataset, ls8_eo3_dataset2, eo3_ls8_dataset_doc):
    datasets = index.datasets.search_eager(product=ls8_eo3_dataset.product.name,
                                           uri=eo3_ls8_dataset_doc[1])
    assert len(datasets) == 1

    datasets = index.datasets.search_eager(product=ls8_eo3_dataset.product.name,
                                           uri='file:///x/yz')
    assert len(datasets) == 0


def test_search_conflicting_types(index, ls8_eo3_dataset):
    # Should return no results.
    with pytest.raises(ValueError):
        index.datasets.search_eager(
            product=ls8_eo3_dataset.product.name,
            # The ls8 type is not of type storage_unit.
            metadata_type='storage_unit'
        )


def test_fetch_all_of_md_type(index: Index, ls8_eo3_dataset: Dataset) -> None:
    # Get every dataset of the md type.
    assert ls8_eo3_dataset.metadata_type is not None  # to shut up mypy
    results = index.datasets.search_eager(
        metadata_type=ls8_eo3_dataset.metadata_type.name
    )
    assert len(results) == 1
    assert results[0].id == ls8_eo3_dataset.id
    # Get every dataset of the type.
    results = index.datasets.search_eager(
        product=ls8_eo3_dataset.product.name
    )
    assert len(results) == 1
    assert results[0].id == ls8_eo3_dataset.id

    # No results for another.
    with pytest.raises(ValueError):
        results = index.datasets.search_eager(
            metadata_type='spam_and_eggs'
        )


def test_count_searches(index: Index,
                        ls8_eo3_dataset: Dataset) -> None:
    # One result in the telemetry type
    datasets = index.datasets.count(
        product=ls8_eo3_dataset.product.name,
        platform='landsat-8',
        instrument='OLI_TIRS',
    )
    assert datasets == 1

    # One result in the metadata type
    datasets = index.datasets.count(
        metadata_type=ls8_eo3_dataset.metadata_type.name,
        platform='landsat-8',
        instrument='OLI_TIRS',
    )
    assert datasets == 1

    # No results when searching for a different dataset type.
    datasets = index.datasets.count(
        product="spam_and_eggs",
        platform='landsat-8',
        instrument='OLI_TIRS'
    )
    assert datasets == 0

    # One result when no types specified.
    datasets = index.datasets.count(
        platform='landsat-8',
        instrument='OLI_TIRS',
    )
    assert datasets == 1

    # No results for different metadata type.
    datasets = index.datasets.count(
        metadata_type='spam_and_eggs',
        platform='landsat-8',
        instrument='OLI_TIRS'
    )
    assert datasets == 0


def test_count_by_product_searches_eo3(index: Index,
                                       ls8_eo3_dataset: Dataset,
                                       ls8_eo3_dataset2: Dataset,
                                       wo_eo3_dataset: Dataset) -> None:
    # Two result in the ls8 type
    products = tuple(index.datasets.count_by_product(
        product=ls8_eo3_dataset.product.name,
        platform='landsat-8'
    ))
    assert products == ((ls8_eo3_dataset.product, 2),)

    # Two results in the metadata type
    products = tuple(index.datasets.count_by_product(
        metadata_type=ls8_eo3_dataset.metadata_type.name,
        platform='landsat-8',
    ))
    assert products == ((ls8_eo3_dataset.product, 2),)

    # No results when searching for a different dataset type.
    products = tuple(index.datasets.count_by_product(
        product="spam_and_eggs",
        platform='landsat-8'
    ))
    assert products == ()

    # Three results over 2 products when no types specified.
    products = set(index.datasets.count_by_product(
        platform='landsat-8',
    ))
    assert products == {(ls8_eo3_dataset.product, 2), (wo_eo3_dataset.product, 1)}

    # No results for different metadata type.
    products = tuple(index.datasets.count_by_product(
        metadata_type='spam_and_eggs',
    ))
    assert products == ()


def test_count_time_groups(index: Index,
                           ls8_eo3_dataset: Dataset) -> None:
    timeline = list(index.datasets.count_product_through_time(
        '1 day',
        product=ls8_eo3_dataset.product.name,
        time=Range(
            datetime.datetime(2016, 5, 11, tzinfo=tz.tzutc()),
            datetime.datetime(2016, 5, 13, tzinfo=tz.tzutc())
        )
    ))

    assert len(timeline) == 2
    assert timeline == [
        (
            Range(datetime.datetime(2016, 5, 11, tzinfo=tz.tzutc()),
                  datetime.datetime(2016, 5, 12, tzinfo=tz.tzutc())),
            0
        ),
        (
            Range(datetime.datetime(2016, 5, 12, tzinfo=tz.tzutc()),
                  datetime.datetime(2016, 5, 13, tzinfo=tz.tzutc())),
            1
        )
    ]


def test_count_time_groups_cli(clirunner: Any,
                               ls8_eo3_dataset: Dataset) -> None:
    result = clirunner(
        [
            'product-counts',
            '1 day',
            'time in [2016-05-11, 2016-05-13]'
        ], cli_method=datacube.scripts.search_tool.cli,
        verbose_flag=''
    )
    expected_out = (
        f'{ls8_eo3_dataset.product.name}\n'
        '    2016-05-11: 0\n'
        '    2016-05-12: 1\n'
    )
    assert result.output.endswith(expected_out)


def test_search_cli_basic(clirunner: Any,
                          ls8_eo3_dataset: Dataset) -> None:
    """
    Search datasets using the cli.
    """
    result = clirunner(
        [
            # No search arguments: return all datasets.
            'datasets'
        ], cli_method=datacube.scripts.search_tool.cli
    )
    assert str(ls8_eo3_dataset.id) in result.output
    assert str(ls8_eo3_dataset.metadata_type.name) in result.output
    assert result.exit_code == 0


def test_cli_info_eo3(index: Index,
                      clirunner: Any,
                      ls8_eo3_dataset: Dataset,
                      ls8_eo3_dataset2: Dataset,
                      eo3_ls8_dataset_doc) -> None:
    """
    Search datasets using the cli.
    """
    index.datasets.add_location(ls8_eo3_dataset.id, 'file:///tmp/location1')

    opts = [
        'dataset', 'info', str(ls8_eo3_dataset.id)
    ]
    result = clirunner(opts, verbose_flag='')

    output = result.output
    # Remove WARNING messages for experimental driver
    output_lines = [line for line in output.splitlines() if "WARNING:" not in line]
    output = "\n".join(output_lines)

    # Should be a valid yaml
    yaml_docs = list(yaml.safe_load_all(output))
    assert len(yaml_docs) == 1

    # We output properties in order for readability:
    output_lines = set(line for line in output_lines)
    expected_lines = [
        "id: " + str(ls8_eo3_dataset.id),
        'product: ga_ls8c_ard_3',
        'status: active',
        'locations:',
        '- file:///tmp/location1',
        f'- {eo3_ls8_dataset_doc[1]}',
        'fields:',
        '    creation_time: 2019-10-07 20:19:19.218290',
        '    format: GeoTIFF',
        '    instrument: OLI_TIRS',
        '    label: ga_ls8c_ard_3-0-0_090086_2016-05-12_final',
        '    landsat_product_id: LC08_L1TP_090086_20160512_20180203_01_T1',
        '    landsat_scene_id: LC80900862016133LGN02',
        '    lat: {begin: -38.53221689818913, end: -36.41618895501644}',
        '    lon: {begin: 147.65992717003462, end: 150.3003802932316}',
        '    platform: landsat-8',
        '    product_family: ard',
        '    region_code: 090086',
        "    time: {begin: '2016-05-12T23:50:23.054165+00:00', end: '2016-05-12T23:50:52.031499+00:00'}",
    ]
    for line in expected_lines:
        assert line in output_lines

    # Check indexed time separately, as we don't care what timezone it's displayed in.
    indexed_time = yaml_docs[0]['indexed']
    assert isinstance(indexed_time, datetime.datetime)
    assert tz_as_utc(indexed_time) == tz_as_utc(ls8_eo3_dataset.indexed_time)

    # Request two, they should have separate yaml documents
    opts.append(str(ls8_eo3_dataset2.id))

    result = clirunner(opts)
    yaml_docs = list(yaml.safe_load_all(result.output))
    assert len(yaml_docs) == 2, "Two datasets should produce two sets of info"
    assert yaml_docs[0]['id'] == str(ls8_eo3_dataset.id)
    assert yaml_docs[1]['id'] == str(ls8_eo3_dataset2.id)


def test_find_duplicates_eo3(index,
                             ls8_eo3_dataset, ls8_eo3_dataset2,
                             ls8_eo3_dataset3, ls8_eo3_dataset4,
                             wo_eo3_dataset):
    # Our four ls8 datasets and one wo.
    all_datasets = index.datasets.search_eager()
    assert len(all_datasets) == 5

    # First two ls8 datasets have the same path/row, last two have a different row.
    expected_ls8_path_row_duplicates = [
        (
            ('090086', 'final'),
            {ls8_eo3_dataset.id, ls8_eo3_dataset2.id}
        ),
        (
            ('101077', 'final'),
            {ls8_eo3_dataset3.id, ls8_eo3_dataset4.id}
        ),

    ]

    # Specifying groups as fields:
    f = ls8_eo3_dataset.metadata_type.dataset_fields.get
    field_res = sorted(index.datasets.search_product_duplicates(
        ls8_eo3_dataset.product,
        f('region_code'), f('dataset_maturity')
    ))
    assert field_res == expected_ls8_path_row_duplicates
    # Field names as strings
    product_res = sorted(index.datasets.search_product_duplicates(
        ls8_eo3_dataset.product,
        'region_code', 'dataset_maturity'
    ))
    assert product_res == expected_ls8_path_row_duplicates

    # No WO duplicates: there's only one
    sat_res = sorted(index.datasets.search_product_duplicates(
        wo_eo3_dataset.product,
        'region_code', 'dataset_maturity'
    ))
    assert sat_res == []


def test_csv_search_via_cli_eo3(clirunner: Any,
                                ls8_eo3_dataset: Dataset,
                                ls8_eo3_dataset2: Dataset) -> None:
    """
    Search datasets via the cli with csv output
    """
    def matches_both(*args):
        rows = _cli_csv_search(('datasets',) + args, clirunner)
        assert len(rows) == 2
        assert {rows[0]['id'], rows[1]['id']} == {str(ls8_eo3_dataset.id), str(ls8_eo3_dataset2.id)}

    def matches_1(*args):
        rows = _cli_csv_search(('datasets',) + args, clirunner)
        assert len(rows) == 1
        assert rows[0]['id'] == str(ls8_eo3_dataset.id)

    def matches_none(*args):
        rows = _cli_csv_search(('datasets',) + args, clirunner)
        assert len(rows) == 0

    def no_such_product(*args):
        with pytest.raises(ValueError):
            _cli_csv_search(('datasets',) + args, clirunner)

    matches_both('lat in [-40, -10]')
    matches_both('product=' + ls8_eo3_dataset.product.name)

    # Don't return on a mismatch
    matches_none('lat in [150, 160]')

    # Match only a single dataset using multiple fields
    matches_1('platform=landsat-8', 'time in [2016-05-11, 2016-05-13]')

    # One matching field, one non-matching
    no_such_product('time in [2016-05-11, 2014-05-13]', 'platform=landsat-5')

    # Test date shorthand
    matches_both('time in [2016-05, 2016-05]')
    matches_none('time in [2014-06, 2014-06]')

    matches_both('time in 2016-05')
    matches_none('time in 2014-08')
    matches_both('time in 2016')
    matches_none('time in 2015')

    matches_both('time in [2016, 2016]')
    matches_both('time in [2015, 2017]')
    matches_none('time in [2015, 2015]')
    matches_none('time in [2013, 2013]')

    matches_both('time in [2016-4, 2016-8]')
    matches_none('time in [2016-1, 2016-3]')
    matches_both('time in [2005, 2017]')


_EXT_AND_BASE_EO3_OUTPUT_HEADER = [
    'id',
    'crs_raw',
    'dataset_maturity',
    'eo_gsd', 'eo_sun_azimuth', 'eo_sun_elevation',
    'cloud_cover', 'fmask_clear', 'fmask_cloud_shadow', 'fmask_snow', 'fmask_water',
    'format',
    'gqa', 'gqa_abs_iterative_mean_x', 'gqa_abs_iterative_mean_xy', 'gqa_abs_iterative_mean_y',
    'gqa_abs_x,gqa_abs_xy', 'gqa_abs_y', 'gqa_cep90',
    'gqa_iterative_mean_x', 'gqa_iterative_mean_xy', 'gqa_iterative_mean_y',
    'gqa_iterative_stddev_x', 'gqa_iterative_stddev_xy', 'gqa_iterative_stddev_y',
    'gqa_mean_x', 'gqa_mean_xy',
    'gqa_mean_y,gqa_stddev_x', 'gqa_stddev_xy', 'gqa_stddev_y',
    'creation_time', 'indexed_by', 'indexed_time',
    'instrument', 'label',
    'landsat_product_id', 'landsat_scene_id',
    'lat', 'lon',
    'metadata_doc', 'metadata_type', 'metadata_type_id',
    'platform', 'product', 'product_family',
    'region_code', 'time', 'uri'
]


def test_csv_structure_eo3(clirunner, ls8_eo3_dataset, ls8_eo3_dataset2):
    output = _csv_search_raw(['datasets', ' lat in [-40, -10]'], clirunner)
    lines = [line.strip() for line in output.split('\n') if line]
    # A header and two dataset rows
    assert len(lines) == 3
    header_line = lines[0]
    for header in _EXT_AND_BASE_EO3_OUTPUT_HEADER:
        assert header in header_line


def test_query_dataset_multi_product_eo3(index: Index, ls8_eo3_dataset, wo_eo3_dataset):
    # We have one ls5 level1 and its child nbar
    dc = Datacube(index)

    # Can we query a single product name?
    datasets = dc.find_datasets(product="ga_ls8c_ard_3")
    assert len(datasets) == 1

    # Can we query multiple products?
    datasets = dc.find_datasets(product=['ga_ls8c_ard_3', 'ga_ls_wo_3'])
    assert len(datasets) == 2

    # Can we query multiple products in a tuple
    datasets = dc.find_datasets(product=('ga_ls8c_ard_3', 'ga_ls_wo_3'))
    assert len(datasets) == 2

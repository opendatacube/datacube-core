# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2020 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
"""
Module
"""
import copy
import csv
import datetime
import io
import uuid
from decimal import Decimal
from uuid import UUID
from typing import List, Iterable, Dict, Any, Tuple

import pytest
import yaml
from dateutil import tz
from psycopg2._range import NumericRange

import datacube.scripts.cli_app
import datacube.scripts.search_tool
from datacube.config import LocalConfig
from datacube.drivers.postgres import PostgresDb
from datacube.drivers.postgres._connections import DEFAULT_DB_USER
from datacube.index import Index
from datacube.model import Dataset
from datacube.model import DatasetType
from datacube.model import MetadataType
from datacube.model import Range

from datacube.testutils import load_dataset_definition

from datacube import Datacube


@pytest.fixture
def pseudo_ls8_type(index, ga_metadata_type):
    index.products.add_document({
        'name': 'ls8_telemetry',
        'description': 'telemetry test',
        'metadata': {
            'product_type': 'pseudo_ls8_data',
            'platform': {
                'code': 'LANDSAT_8'
            },
            'instrument': {
                'name': 'OLI_TIRS'
            },
            'format': {
                'name': 'PSEUDOMD'
            }
        },
        'metadata_type': ga_metadata_type.name
    })
    return index.products.get_by_name('ls8_telemetry')


@pytest.fixture
def pseudo_ls8_dataset(index, initialised_postgres_db, pseudo_ls8_type):
    id_ = str(uuid.uuid4())
    with initialised_postgres_db.connect() as connection:
        was_inserted = connection.insert_dataset(
            {
                'id': id_,
                'product_type': 'pseudo_ls8_data',
                'checksum_path': 'package.sha1',
                'ga_label': 'LS8_OLITIRS_STD-MD_P00_LC81160740742015089ASA00_'
                            '116_074_20150330T022553Z20150330T022657',

                'ga_level': 'P00',
                'size_bytes': 637660782,
                'platform': {
                    'code': 'LANDSAT_8'
                },
                # We're unlikely to have extent info for a raw dataset, we'll use it for search tests.
                'extent': {
                    'from_dt': datetime.datetime(2014, 7, 26, 23, 48, 0, 343853),
                    'to_dt': datetime.datetime(2014, 7, 26, 23, 52, 0, 343853),
                    'coord': {
                        'll': {'lat': -31.33333, 'lon': 149.78434},
                        'lr': {'lat': -31.37116, 'lon': 152.20094},
                        'ul': {'lat': -29.23394, 'lon': 149.85216},
                        'ur': {'lat': -29.26873, 'lon': 152.21782}
                    }
                },
                'image': {
                    'satellite_ref_point_start': {'x': 116, 'y': 74},
                    'satellite_ref_point_end': {'x': 116, 'y': 84},
                },
                'creation_dt': datetime.datetime(2015, 4, 22, 6, 32, 4),
                'instrument': {'name': 'OLI_TIRS'},
                'format': {
                    'name': 'PSEUDOMD'
                },
                'lineage': {
                    'source_datasets': {}
                }
            },
            id_,
            pseudo_ls8_type.id
        )
    assert was_inserted
    d = index.datasets.get(id_)
    # The dataset should have been matched to the telemetry type.
    assert d.type.id == pseudo_ls8_type.id

    return d


@pytest.fixture
def pseudo_ls8_dataset2(index, initialised_postgres_db, pseudo_ls8_type):
    # Like the previous dataset, but a day later in time.
    id_ = str(uuid.uuid4())
    with initialised_postgres_db.connect() as connection:
        was_inserted = connection.insert_dataset(
            {
                'id': id_,
                'product_type': 'pseudo_ls8_data',
                'checksum_path': 'package.sha1',
                'ga_label': 'LS8_OLITIRS_STD-MD_P00_LC81160740742015089ASA00_'
                            '116_074_20150330T022553Z20150330T022657',

                'ga_level': 'P00',
                'size_bytes': 637660782,
                'platform': {
                    'code': 'LANDSAT_8'
                },
                'image': {
                    'satellite_ref_point_start': {'x': 116, 'y': 74},
                    'satellite_ref_point_end': {'x': 116, 'y': 84},
                },
                # We're unlikely to have extent info for a raw dataset, we'll use it for search tests.
                'extent': {
                    'from_dt': datetime.datetime(2014, 7, 27, 23, 48, 0, 343853),
                    'to_dt': datetime.datetime(2014, 7, 27, 23, 52, 0, 343853),
                    'coord': {
                        'll': {'lat': -31.33333, 'lon': 149.78434},
                        'lr': {'lat': -31.37116, 'lon': 152.20094},
                        'ul': {'lat': -29.23394, 'lon': 149.85216},
                        'ur': {'lat': -29.26873, 'lon': 152.21782}
                    }
                },
                'creation_dt': datetime.datetime(2015, 4, 22, 6, 32, 4),
                'instrument': {'name': 'OLI_TIRS'},
                'format': {
                    'name': 'PSEUDOMD'
                },
                'lineage': {
                    'source_datasets': {}
                }
            },
            id_,
            pseudo_ls8_type.id
        )
    assert was_inserted
    d = index.datasets.get(id_)
    # The dataset should have been matched to the telemetry type.
    assert d.type.id == pseudo_ls8_type.id

    return d


# Datasets 3 and 4 mirror 1 and 2 but have a different path/row.
@pytest.fixture
def pseudo_ls8_dataset3(index: Index,
                        initialised_postgres_db: PostgresDb,
                        pseudo_ls8_type: DatasetType,
                        pseudo_ls8_dataset: Dataset) -> Dataset:
    # Same as 1, but a different path/row
    id_ = str(uuid.uuid4())
    dataset_doc = copy.deepcopy(pseudo_ls8_dataset.metadata_doc)
    dataset_doc['id'] = id_
    dataset_doc['image'] = {
        'satellite_ref_point_start': {'x': 116, 'y': 85},
        'satellite_ref_point_end': {'x': 116, 'y': 87},
    }

    with initialised_postgres_db.connect() as connection:
        was_inserted = connection.insert_dataset(
            dataset_doc,
            id_,
            pseudo_ls8_type.id
        )
    assert was_inserted
    d = index.datasets.get(id_)
    # The dataset should have been matched to the telemetry type.
    assert d.type.id == pseudo_ls8_type.id
    return d


@pytest.fixture
def pseudo_ls8_dataset4(index: Index,
                        initialised_postgres_db: PostgresDb,
                        pseudo_ls8_type: DatasetType,
                        pseudo_ls8_dataset2: Dataset) -> Dataset:
    # Same as 2, but a different path/row
    id_ = str(uuid.uuid4())
    dataset_doc = copy.deepcopy(pseudo_ls8_dataset2.metadata_doc)
    dataset_doc['id'] = id_
    dataset_doc['image'] = {
        'satellite_ref_point_start': {'x': 116, 'y': 85},
        'satellite_ref_point_end': {'x': 116, 'y': 87},
    }

    with initialised_postgres_db.connect() as connection:
        was_inserted = connection.insert_dataset(
            dataset_doc,
            id_,
            pseudo_ls8_type.id
        )
        assert was_inserted
        d = index.datasets.get(id_)
        # The dataset should have been matched to the telemetry type.
        assert d.type.id == pseudo_ls8_type.id
        return d


@pytest.fixture
def ls5_dataset_w_children(index, clirunner, example_ls5_dataset_path, indexed_ls5_scene_products):
    clirunner(['dataset', 'add', str(example_ls5_dataset_path)])
    doc = load_dataset_definition(example_ls5_dataset_path)
    return index.datasets.get(doc.id, include_sources=True)


@pytest.fixture
def ls5_dataset_nbar_type(ls5_dataset_w_children: Dataset,
                          indexed_ls5_scene_products: List[DatasetType]) -> DatasetType:
    for dataset_type in indexed_ls5_scene_products:
        if dataset_type.name == ls5_dataset_w_children.type.name:
            return dataset_type
    else:
        raise RuntimeError("LS5 type was not among types")


def test_search_dataset_equals(index: Index, pseudo_ls8_dataset: Dataset):
    datasets = index.datasets.search_eager(
        platform='LANDSAT_8'
    )
    assert len(datasets) == 1
    assert datasets[0].id == pseudo_ls8_dataset.id

    datasets = index.datasets.search_eager(
        platform='LANDSAT_8',
        instrument='OLI_TIRS'
    )
    assert len(datasets) == 1
    assert datasets[0].id == pseudo_ls8_dataset.id

    # Wrong sensor name
    with pytest.raises(ValueError):
        datasets = index.datasets.search_eager(
            platform='LANDSAT-8',
            instrument='TM',
        )


def test_search_dataset_by_metadata(index: Index, pseudo_ls8_dataset: Dataset) -> None:
    datasets = index.datasets.search_by_metadata(
        {"platform": {"code": "LANDSAT_8"}, "instrument": {"name": "OLI_TIRS"}}
    )
    datasets = list(datasets)
    assert len(datasets) == 1
    assert datasets[0].id == pseudo_ls8_dataset.id

    datasets = index.datasets.search_by_metadata(
        {"platform": {"code": "LANDSAT_5"}, "instrument": {"name": "TM"}}
    )
    datasets = list(datasets)
    assert len(datasets) == 0


def test_search_day(index: Index, pseudo_ls8_dataset: Dataset) -> None:
    # Matches day
    datasets = index.datasets.search_eager(
        time=datetime.date(2014, 7, 26)
    )
    assert len(datasets) == 1
    assert datasets[0].id == pseudo_ls8_dataset.id

    # Different day: no match
    datasets = index.datasets.search_eager(
        time=datetime.date(2014, 7, 27)
    )
    assert len(datasets) == 0


def test_search_dataset_ranges(index: Index, pseudo_ls8_dataset: Dataset) -> None:
    # In the lat bounds.
    datasets = index.datasets.search_eager(
        lat=Range(-30.5, -29.5),
        time=Range(
            datetime.datetime(2014, 7, 26, 23, 0, 0),
            datetime.datetime(2014, 7, 26, 23, 59, 0)
        )
    )
    assert len(datasets) == 1
    assert datasets[0].id == pseudo_ls8_dataset.id

    # Out of the lat bounds.
    datasets = index.datasets.search_eager(
        lat=Range(28, 32),
        time=Range(
            datetime.datetime(2014, 7, 26, 23, 48, 0),
            datetime.datetime(2014, 7, 26, 23, 50, 0)
        )
    )
    assert len(datasets) == 0

    # Out of the time bounds
    datasets = index.datasets.search_eager(
        lat=Range(-30.5, -29.5),
        time=Range(
            datetime.datetime(2014, 7, 26, 21, 48, 0),
            datetime.datetime(2014, 7, 26, 21, 50, 0)
        )
    )
    assert len(datasets) == 0

    # A dataset that overlaps but is not fully contained by the search bounds.
    # TODO: Do we want overlap as the default behaviour?
    # Should we distinguish between 'contains' and 'overlaps'?
    datasets = index.datasets.search_eager(
        lat=Range(-40, -30)
    )
    assert len(datasets) == 1
    assert datasets[0].id == pseudo_ls8_dataset.id

    # Single point search
    datasets = index.datasets.search_eager(
        lat=-30.0,
        time=Range(
            datetime.datetime(2014, 7, 26, 23, 0, 0),
            datetime.datetime(2014, 7, 26, 23, 59, 0)
        )
    )
    assert len(datasets) == 1
    assert datasets[0].id == pseudo_ls8_dataset.id

    datasets = index.datasets.search_eager(
        lat=30.0,
        time=Range(
            datetime.datetime(2014, 7, 26, 23, 0, 0),
            datetime.datetime(2014, 7, 26, 23, 59, 0)
        )
    )
    assert len(datasets) == 0

    # Single timestamp search
    datasets = index.datasets.search_eager(
        lat=Range(-30.5, -29.5),
        time=datetime.datetime(2014, 7, 26, 23, 50, 0)
    )
    assert len(datasets) == 1
    assert datasets[0].id == pseudo_ls8_dataset.id

    datasets = index.datasets.search_eager(
        lat=Range(-30.5, -29.5),
        time=datetime.datetime(2014, 7, 26, 23, 30, 0)
    )
    assert len(datasets) == 0


def test_search_globally(index: Index, pseudo_ls8_dataset: Dataset) -> None:
    # Insert dataset. It should be matched to the telemetry collection.
    # No expressions means get all.
    results = list(index.datasets.search())
    assert len(results) == 1

    # Dataset sources aren't loaded by default
    assert results[0].sources is None


def _load_product_query(
        lazy_results: Iterable[Tuple[DatasetType, Iterable[Dataset]]]
) -> Dict[str, List[Dataset]]:
    """
    search_by_product() returns two levels of laziness. load them all into memory
    for easy comparison/counts
    """
    products = {}  # type: Dict[str, List[Dataset]]
    for product, datasets in lazy_results:
        assert product.name not in products, "search_by_product() returned a product twice"
        products[product.name] = list(datasets)
    return products


# Current formulation of this test relies on non-EO3 test data
@pytest.mark.parametrize('datacube_env_name', ('datacube', ))
def test_search_by_product(index: Index,
                           pseudo_ls8_type: DatasetType,
                           pseudo_ls8_dataset: Dataset,
                           indexed_ls5_scene_products,
                           ls5_dataset_w_children: Dataset) -> None:
    # Query all the test data, the counts should match expected
    results = _load_product_query(index.datasets.search_by_product())
    assert len(results) == 7
    dataset_count = sum(len(ds) for ds in results.values())
    assert dataset_count == 4

    # Query one product
    products = _load_product_query(index.datasets.search_by_product(
        platform='LANDSAT_8',
        instrument='OLI_TIRS',
    ))
    assert len(products) == 1
    [dataset] = products[pseudo_ls8_type.name]
    assert dataset.id == pseudo_ls8_dataset.id


def test_search_limit(index, pseudo_ls8_dataset, pseudo_ls8_dataset2):
    datasets = list(index.datasets.search())
    assert len(datasets) == 2
    datasets = list(index.datasets.search(limit=1))
    assert len(datasets) == 1
    datasets = list(index.datasets.search(limit=0))
    assert len(datasets) == 0
    datasets = list(index.datasets.search(limit=5))
    assert len(datasets) == 2

    datasets = list(index.datasets.search_returning(('id',)))
    assert len(datasets) == 2
    datasets = list(index.datasets.search_returning(('id',), limit=1))
    assert len(datasets) == 1
    datasets = list(index.datasets.search_returning(('id',), limit=0))
    assert len(datasets) == 0
    datasets = list(index.datasets.search_returning(('id',), limit=5))
    assert len(datasets) == 2


# Current formulation of this test relies on non-EO3 test data
@pytest.mark.parametrize('datacube_env_name', ('datacube', ))
def test_search_or_expressions(index: Index,
                               pseudo_ls8_type: DatasetType,
                               pseudo_ls8_dataset: Dataset,
                               ls5_dataset_nbar_type: DatasetType,
                               ls5_dataset_w_children: Dataset,
                               default_metadata_type: MetadataType,
                               telemetry_metadata_type: MetadataType) -> None:
    # Four datasets:
    # Our standard LS8
    # - type=ls8_telemetry
    # LS5 with children:
    # - type=ls5_nbar_scene
    # - type=ls5_level1_scene
    # - type=ls5_satellite_telemetry_data

    all_datasets = index.datasets.search_eager()
    assert len(all_datasets) == 4
    all_ids = set(dataset.id for dataset in all_datasets)

    # OR all platforms: should return all datasets
    datasets = index.datasets.search_eager(
        platform=['LANDSAT_5', 'LANDSAT_7', 'LANDSAT_8']
    )
    assert len(datasets) == 4
    ids = set(dataset.id for dataset in datasets)
    assert ids == all_ids

    # OR expression with only one clause.
    datasets = index.datasets.search_eager(
        platform=['LANDSAT_8']
    )
    assert len(datasets) == 1
    assert datasets[0].id == pseudo_ls8_dataset.id

    # OR two products: return two
    datasets = index.datasets.search_eager(
        product=[pseudo_ls8_type.name, ls5_dataset_nbar_type.name]
    )
    assert len(datasets) == 2
    ids = set(dataset.id for dataset in datasets)
    assert ids == {pseudo_ls8_dataset.id, ls5_dataset_w_children.id}

    # eo OR telemetry: return all
    datasets = index.datasets.search_eager(
        metadata_type=[
            # LS5 + children
            default_metadata_type.name,
            # Nothing
            telemetry_metadata_type.name,
            # LS8 dataset
            pseudo_ls8_type.metadata_type.name
        ]
    )
    assert len(datasets) == 4
    ids = set(dataset.id for dataset in datasets)
    assert ids == all_ids

    # Redundant ORs should have no effect.
    datasets = index.datasets.search_eager(
        product=[pseudo_ls8_type.name, pseudo_ls8_type.name, pseudo_ls8_type.name]
    )
    assert len(datasets) == 1
    assert datasets[0].id == pseudo_ls8_dataset.id


# Current formulation of this test relies on non-EO3 test data
@pytest.mark.parametrize('datacube_env_name', ('datacube', ))
def test_search_returning(index: Index,
                          local_config: LocalConfig,
                          pseudo_ls8_type: DatasetType,
                          pseudo_ls8_dataset: Dataset,
                          ls5_dataset_w_children) -> None:

    assert index.datasets.count() == 4, "Expected four test datasets"

    # Expect one product with our one dataset.
    results = list(index.datasets.search_returning(
        ('id', 'sat_path', 'sat_row'),
        platform='LANDSAT_8',
        instrument='OLI_TIRS',
    ))
    assert len(results) == 1
    id_, path_range, sat_range = results[0]
    assert id_ == pseudo_ls8_dataset.id
    # TODO: output nicer types?
    assert path_range == NumericRange(Decimal('116'), Decimal('116'), '[]')
    assert sat_range == NumericRange(Decimal('74'), Decimal('84'), '[]')

    results = list(index.datasets.search_returning(
        ('id', 'metadata_doc',),
        platform='LANDSAT_8',
        instrument='OLI_TIRS',
    ))
    assert len(results) == 1
    id_, document = results[0]
    assert id_ == pseudo_ls8_dataset.id
    assert document == pseudo_ls8_dataset.metadata_doc

    my_username = local_config.get('db_username', DEFAULT_DB_USER)

    # Mixture of document and native fields
    results = list(index.datasets.search_returning(
        ('id', 'creation_time', 'format', 'label'),
        platform='LANDSAT_8',
        indexed_by=my_username,
    ))
    assert len(results) == 1

    id_, creation_time, format_, label = results[0]

    assert id_ == pseudo_ls8_dataset.id
    assert format_ == 'PSEUDOMD'

    # It's always UTC in the document
    expected_time = creation_time.astimezone(tz.tzutc()).replace(tzinfo=None)
    assert expected_time.isoformat() == pseudo_ls8_dataset.metadata_doc['creation_dt']
    assert label == pseudo_ls8_dataset.metadata_doc['ga_label']


def test_search_returning_rows(index, pseudo_ls8_type,
                               pseudo_ls8_dataset, pseudo_ls8_dataset2,
                               indexed_ls5_scene_products):
    dataset = pseudo_ls8_dataset

    # If returning a field like uri, there will be one result per location.

    # No locations
    results = list(index.datasets.search_returning(
        ('id', 'uri'),
        platform='LANDSAT_8',
        instrument='OLI_TIRS',
    ))
    assert len(results) == 0

    # Add a location to the dataset and we should get one result
    test_uri = 'file:///tmp/test1'
    index.datasets.add_location(dataset.id, test_uri)
    results = list(index.datasets.search_returning(
        ('id', 'uri'),
        platform='LANDSAT_8',
        instrument='OLI_TIRS',
    ))
    assert len(results) == 1
    assert results == [(dataset.id, test_uri)]

    # Add a second location and we should get two results
    test_uri2 = 'file:///tmp/test2'
    index.datasets.add_location(dataset.id, test_uri2)
    results = set(index.datasets.search_returning(
        ('id', 'uri'),
        platform='LANDSAT_8',
        instrument='OLI_TIRS',
    ))
    assert len(results) == 2
    assert results == {
        (dataset.id, test_uri),
        (dataset.id, test_uri2)
    }

    # A second dataset now has a location too:
    test_uri3 = 'mdss://c10/tmp/something'
    index.datasets.add_location(pseudo_ls8_dataset2.id, test_uri3)
    # Datasets and locations should still correctly match up...
    results = set(index.datasets.search_returning(
        ('id', 'uri'),
        platform='LANDSAT_8',
        instrument='OLI_TIRS',
    ))
    assert len(results) == 3
    assert results == {
        (dataset.id, test_uri),
        (dataset.id, test_uri2),
        (pseudo_ls8_dataset2.id, test_uri3),
    }


def test_searches_only_type(index: Index,
                            pseudo_ls8_type: DatasetType,
                            pseudo_ls8_dataset: Dataset,
                            ls5_telem_type) -> None:
    # The dataset should have been matched to the telemetry type.
    assert pseudo_ls8_dataset.type.id == pseudo_ls8_type.id
    assert index.datasets.search_eager()

    # One result in the telemetry type
    datasets = index.datasets.search_eager(
        product=pseudo_ls8_type.name,
        platform='LANDSAT_8',
        instrument='OLI_TIRS',
    )
    assert len(datasets) == 1
    assert datasets[0].id == pseudo_ls8_dataset.id

    # One result in the metadata type
    datasets = index.datasets.search_eager(
        metadata_type=pseudo_ls8_type.metadata_type.name,
        platform='LANDSAT_8',
        instrument='OLI_TIRS',
    )
    assert len(datasets) == 1
    assert datasets[0].id == pseudo_ls8_dataset.id

    # No results when searching for a different dataset type.
    with pytest.raises(ValueError):
        datasets = index.datasets.search_eager(
            product=ls5_telem_type.name,
            platform='LANDSAT_8',
            instrument='OLI_TIRS'
        )

    # One result when no types specified.
    datasets = index.datasets.search_eager(
        platform='LANDSAT_8',
        instrument='OLI_TIRS'
    )
    assert len(datasets) == 1
    assert datasets[0].id == pseudo_ls8_dataset.id

    # No results for different metadata type.
    with pytest.raises(ValueError):
        datasets = index.datasets.search_eager(
            metadata_type='telemetry',
            platform='LANDSAT_8',
            instrument='OLI_TIRS'
        )


# Current formulation of this test relies on non-EO3 test data
@pytest.mark.parametrize('datacube_env_name', ('datacube', ))
def test_search_special_fields(index: Index,
                               pseudo_ls8_type: DatasetType,
                               pseudo_ls8_dataset: Dataset,
                               ls5_dataset_w_children) -> None:
    # 'product' is a special case
    datasets = index.datasets.search_eager(
        product=pseudo_ls8_type.name
    )
    assert len(datasets) == 1
    assert datasets[0].id == pseudo_ls8_dataset.id

    # Unknown field: no results
    with pytest.raises(ValueError):
        datasets = index.datasets.search_eager(
            platform='LANDSAT_8',
            flavour='chocolate',
        )


# Current formulation of this test relies on non-EO3 test data
@pytest.mark.parametrize('datacube_env_name', ('datacube', ))
def test_search_by_uri(index, ls5_dataset_w_children):
    datasets = index.datasets.search_eager(product=ls5_dataset_w_children.type.name,
                                           uri=ls5_dataset_w_children.local_uri)
    assert len(datasets) == 1

    datasets = index.datasets.search_eager(product=ls5_dataset_w_children.type.name,
                                           uri='file:///x/yz')
    assert len(datasets) == 0


def test_search_conflicting_types(index, pseudo_ls8_dataset, pseudo_ls8_type):
    # Should return no results.
    with pytest.raises(ValueError):
        index.datasets.search_eager(
            product=pseudo_ls8_type.name,
            # The telemetry type is not of type storage_unit.
            metadata_type='storage_unit'
        )


def test_fetch_all_of_md_type(index: Index, pseudo_ls8_dataset: Dataset) -> None:
    # Get every dataset of the md type.
    assert pseudo_ls8_dataset.metadata_type is not None  # to shut up mypy
    results = index.datasets.search_eager(
        metadata_type=pseudo_ls8_dataset.metadata_type.name
    )
    assert len(results) == 1
    assert results[0].id == pseudo_ls8_dataset.id
    # Get every dataset of the type.
    results = index.datasets.search_eager(
        product=pseudo_ls8_dataset.type.name
    )
    assert len(results) == 1
    assert results[0].id == pseudo_ls8_dataset.id

    # No results for another.
    with pytest.raises(ValueError):
        results = index.datasets.search_eager(
            metadata_type='telemetry'
        )


def test_count_searches(index: Index,
                        pseudo_ls8_type: DatasetType,
                        pseudo_ls8_dataset: Dataset,
                        ls5_telem_type) -> None:
    # The dataset should have been matched to the telemetry type.
    assert pseudo_ls8_dataset.type.id == pseudo_ls8_type.id
    assert index.datasets.search_eager()

    # One result in the telemetry type
    datasets = index.datasets.count(
        product=pseudo_ls8_type.name,
        platform='LANDSAT_8',
        instrument='OLI_TIRS',
    )
    assert datasets == 1

    # One result in the metadata type
    datasets = index.datasets.count(
        metadata_type=pseudo_ls8_type.metadata_type.name,
        platform='LANDSAT_8',
        instrument='OLI_TIRS',
    )
    assert datasets == 1

    # No results when searching for a different dataset type.
    datasets = index.datasets.count(
        product=ls5_telem_type.name,
        platform='LANDSAT_8',
        instrument='OLI_TIRS'
    )
    assert datasets == 0

    # One result when no types specified.
    datasets = index.datasets.count(
        platform='LANDSAT_8',
        instrument='OLI_TIRS',
    )
    assert datasets == 1

    # No results for different metadata type.
    datasets = index.datasets.count(
        metadata_type='telemetry',
        platform='LANDSAT_8',
        instrument='OLI_TIRS'
    )
    assert datasets == 0


# Current formulation of this test relies on non-EO3 test data
@pytest.mark.parametrize('datacube_env_name', ('datacube', ))
def test_get_dataset_with_children(index: Index, ls5_dataset_w_children: Dataset) -> None:
    id_ = ls5_dataset_w_children.id
    assert isinstance(id_, UUID)

    # Sources not loaded by default
    d = index.datasets.get(id_)
    assert d.sources is None

    # Ask for all sources
    d = index.datasets.get(id_, include_sources=True)
    assert list(d.sources.keys()) == ['level1']
    level1 = d.sources['level1']
    assert list(level1.sources.keys()) == ['satellite_telemetry_data']
    assert list(level1.sources['satellite_telemetry_data'].sources) == []

    # It should also work with a string id
    d = index.datasets.get(str(id_), include_sources=True)
    assert list(d.sources.keys()) == ['level1']
    level1 = d.sources['level1']
    assert list(level1.sources.keys()) == ['satellite_telemetry_data']
    assert list(level1.sources['satellite_telemetry_data'].sources) == []


def test_count_by_product_searches(index: Index,
                                   pseudo_ls8_type: DatasetType,
                                   pseudo_ls8_dataset: Dataset,
                                   ls5_telem_type: DatasetType) -> None:
    # The dataset should have been matched to the telemetry type.
    assert pseudo_ls8_dataset.type.id == pseudo_ls8_type.id
    assert index.datasets.search_eager()

    # One result in the telemetry type
    products = tuple(index.datasets.count_by_product(
        product=pseudo_ls8_type.name,
        platform='LANDSAT_8',
        instrument='OLI_TIRS',
    ))
    assert products == ((pseudo_ls8_type, 1),)

    # One result in the metadata type
    products = tuple(index.datasets.count_by_product(
        metadata_type=pseudo_ls8_type.metadata_type.name,
        platform='LANDSAT_8',
        instrument='OLI_TIRS',
    ))
    assert products == ((pseudo_ls8_type, 1),)

    # No results when searching for a different dataset type.
    products = tuple(index.datasets.count_by_product(
        product=ls5_telem_type.name,
        platform='LANDSAT_8',
        instrument='OLI_TIRS'
    ))
    assert products == ()

    # One result when no types specified.
    products = tuple(index.datasets.count_by_product(
        platform='LANDSAT_8',
        instrument='OLI_TIRS',
    ))
    assert products == ((pseudo_ls8_type, 1),)

    # Only types with datasets should be returned (these params match ls5_gtiff too)
    products = tuple(index.datasets.count_by_product())
    assert products == ((pseudo_ls8_type, 1),)

    # No results for different metadata type.
    products = tuple(index.datasets.count_by_product(
        metadata_type='telemetry',
    ))
    assert products == ()


def test_count_time_groups(index: Index,
                           pseudo_ls8_type: DatasetType,
                           pseudo_ls8_dataset: Dataset) -> None:
    # 'from_dt': datetime.datetime(2014, 7, 26, 23, 48, 0, 343853),
    # 'to_dt': datetime.datetime(2014, 7, 26, 23, 52, 0, 343853),
    timeline = list(index.datasets.count_product_through_time(
        '1 day',
        product=pseudo_ls8_type.name,
        time=Range(
            datetime.datetime(2014, 7, 25, tzinfo=tz.tzutc()),
            datetime.datetime(2014, 7, 27, tzinfo=tz.tzutc())
        )
    ))

    assert len(timeline) == 2
    assert timeline == [
        (
            Range(datetime.datetime(2014, 7, 25, tzinfo=tz.tzutc()),
                  datetime.datetime(2014, 7, 26, tzinfo=tz.tzutc())),
            0
        ),
        (
            Range(datetime.datetime(2014, 7, 26, tzinfo=tz.tzutc()),
                  datetime.datetime(2014, 7, 27, tzinfo=tz.tzutc())),
            1
        )
    ]


# Current formulation of this test relies on non-EO3 test data
@pytest.mark.parametrize('datacube_env_name', ('datacube', ))
@pytest.mark.usefixtures('ga_metadata_type',
                         'indexed_ls5_scene_products')
def test_source_filter(clirunner, index, example_ls5_dataset_path):
    clirunner(
        [
            'dataset',
            'add',
            str(example_ls5_dataset_path)
        ]
    )

    all_nbar = index.datasets.search_eager(product='ls5_nbar_scene')
    assert len(all_nbar) == 1
    all_level1 = index.datasets.search_eager(product='ls5_level1_scene')
    assert len(all_level1) == 1
    assert all_level1[0].metadata.gsi == 'ASA'

    dss = index.datasets.search_eager(
        product='ls5_nbar_scene',
        source_filter={'product': 'ls5_level1_scene', 'gsi': 'ASA'}
    )
    assert dss == all_nbar
    dss = index.datasets.search_eager(
        product='ls5_nbar_scene',
        source_filter={'product': 'ls5_level1_scene', 'gsi': 'GREG'}
    )
    assert dss == []

    with pytest.raises(RuntimeError):
        dss = index.datasets.search_eager(
            product='ls5_nbar_scene',
            source_filter={'gsi': 'ASA'}
        )


def test_count_time_groups_cli(clirunner: Any,
                               pseudo_ls8_type: DatasetType,
                               pseudo_ls8_dataset: Dataset) -> None:
    result = clirunner(
        [
            'product-counts',
            '1 day',
            'time in [2014-07-25, 2014-07-27]'
        ], cli_method=datacube.scripts.search_tool.cli,
        verbose_flag=''
    )

    expected_out = (
        '{}\n'
        '    2014-07-25: 0\n'
        '    2014-07-26: 1\n'
    ).format(pseudo_ls8_type.name)

    assert result.output.endswith(expected_out)


def test_search_cli_basic(clirunner: Any,
                          telemetry_metadata_type: MetadataType,
                          pseudo_ls8_dataset: Dataset) -> None:
    """
    Search datasets using the cli.
    """
    result = clirunner(
        [
            # No search arguments: return all datasets.
            'datasets'
        ], cli_method=datacube.scripts.search_tool.cli
    )

    assert str(pseudo_ls8_dataset.id) in result.output
    assert str(telemetry_metadata_type.name) in result.output

    assert result.exit_code == 0


def test_cli_info(index: Index,
                  clirunner: Any,
                  pseudo_ls8_dataset: Dataset,
                  pseudo_ls8_dataset2: Dataset) -> None:
    """
    Search datasets using the cli.
    """
    index.datasets.add_location(pseudo_ls8_dataset.id, 'file:///tmp/location1')
    index.datasets.add_location(pseudo_ls8_dataset.id, 'file:///tmp/location2')

    opts = [
        'dataset', 'info', str(pseudo_ls8_dataset.id)
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
    output_lines = [line for line in output_lines if not line.startswith('indexed:')]
    expected_lines = [
        "id: " + str(pseudo_ls8_dataset.id),
        'product: ls8_telemetry',
        'status: active',
        # Newest location first
        'locations:',
        '- file:///tmp/location2',
        '- file:///tmp/location1',
        'fields:',
        '    creation_time: 2015-04-22 06:32:04',
        '    format: PSEUDOMD',
        '    gsi: null',
        '    instrument: OLI_TIRS',
        '    label: LS8_OLITIRS_STD-MD_P00_LC81160740742015089ASA00_116_074_20150330T022553Z20150330T022657',
        '    lat: {begin: -31.37116, end: -29.23394}',
        '    lon: {begin: 149.78434, end: 152.21782}',
        '    orbit: null',
        '    platform: LANDSAT_8',
        '    product_type: pseudo_ls8_data',
        '    sat_path: {begin: 116, end: 116}',
        '    sat_row: {begin: 74, end: 84}',
        "    time: {begin: '2014-07-26T23:48:00.343853', end: '2014-07-26T23:52:00.343853'}",
    ]
    assert expected_lines == output_lines

    # Check indexed time separately, as we don't care what timezone it's displayed in.
    indexed_time = yaml_docs[0]['indexed']
    assert isinstance(indexed_time, datetime.datetime)
    assert assume_utc(indexed_time) == assume_utc(pseudo_ls8_dataset.indexed_time)

    # Request two, they should have separate yaml documents
    opts.append(str(pseudo_ls8_dataset2.id))

    result = clirunner(opts)
    yaml_docs = list(yaml.safe_load_all(result.output))
    assert len(yaml_docs) == 2, "Two datasets should produce two sets of info"
    assert yaml_docs[0]['id'] == str(pseudo_ls8_dataset.id)
    assert yaml_docs[1]['id'] == str(pseudo_ls8_dataset2.id)


def assume_utc(d):
    if d.tzinfo is None:
        return d.replace(tzinfo=tz.tzutc())
    else:
        return d.astimezone(tz.tzutc())


def test_cli_missing_info(clirunner, initialised_postgres_db):
    id_ = str(uuid.uuid4())
    result = clirunner(
        [
            'dataset', 'info', id_
        ],
        catch_exceptions=False,
        expect_success=False,
        verbose_flag=False
    )
    assert result.exit_code == 1, "Should return exit status when dataset is missing"
    # This should have been output to stderr, but the CliRunner doesnit distinguish
    assert result.output.endswith("{id} missing\n".format(id=id_))


# Current formulation of this test relies on non-EO3 test data
@pytest.mark.parametrize('datacube_env_name', ('datacube', ))
def test_find_duplicates(index, pseudo_ls8_type,
                         pseudo_ls8_dataset, pseudo_ls8_dataset2, pseudo_ls8_dataset3, pseudo_ls8_dataset4,
                         ls5_dataset_w_children):
    # type: (Index, DatasetType, Dataset, Dataset, Dataset, Dataset, Dataset) -> None

    # Our four ls8 datasets and three ls5.
    all_datasets = index.datasets.search_eager()
    assert len(all_datasets) == 7

    # First two ls8 datasets have the same path/row, last two have a different row.
    expected_ls8_path_row_duplicates = [
        (
            (
                NumericRange(Decimal('116'), Decimal('116'), '[]'),
                NumericRange(Decimal('74'), Decimal('84'), '[]')
            ),
            {pseudo_ls8_dataset.id, pseudo_ls8_dataset2.id}
        ),
        (
            (
                NumericRange(Decimal('116'), Decimal('116'), '[]'),
                NumericRange(Decimal('85'), Decimal('87'), '[]')
            ),
            {pseudo_ls8_dataset3.id, pseudo_ls8_dataset4.id}
        ),

    ]

    # Specifying groups as fields:
    f = pseudo_ls8_type.metadata_type.dataset_fields.get
    field_res = sorted(index.datasets.search_product_duplicates(
        pseudo_ls8_type,
        f('sat_path'), f('sat_row')
    ))
    assert field_res == expected_ls8_path_row_duplicates
    # Field names as strings
    product_res = sorted(index.datasets.search_product_duplicates(
        pseudo_ls8_type,
        'sat_path', 'sat_row'
    ))
    assert product_res == expected_ls8_path_row_duplicates

    # Get duplicates that start on the same day
    f = pseudo_ls8_type.metadata_type.dataset_fields.get
    field_res = sorted(index.datasets.search_product_duplicates(
        pseudo_ls8_type,
        f('time').lower.day  # type: ignore
    ))

    # Datasets 1 & 3 are on the 26th.
    # Datasets 2 & 4 are on the 27th.
    assert field_res == [
        (
            (
                datetime.datetime(2014, 7, 26, 0, 0),
            ),
            {pseudo_ls8_dataset.id, pseudo_ls8_dataset3.id}
        ),
        (
            (
                datetime.datetime(2014, 7, 27, 0, 0),
            ),
            {pseudo_ls8_dataset2.id, pseudo_ls8_dataset4.id}
        ),

    ]

    # No LS5 duplicates: there's only one of each
    sat_res = sorted(index.datasets.search_product_duplicates(
        ls5_dataset_w_children.type,
        'sat_path', 'sat_row'
    ))
    assert sat_res == []


def test_csv_search_via_cli(clirunner: Any,
                            pseudo_ls8_type: DatasetType,
                            pseudo_ls8_dataset: Dataset,
                            pseudo_ls8_dataset2: Dataset) -> None:
    """
    Search datasets via the cli with csv output
    """

    # Test dataset is:
    # platform: LANDSAT_8
    # from: 2014-7-26  23:48:00
    # to:   2014-7-26  23:52:00
    # coords:
    #     ll: (-31.33333, 149.78434)
    #     lr: (-31.37116, 152.20094)
    #     ul: (-29.23394, 149.85216)
    #     ur: (-29.26873, 152.21782)

    # Dataset 2 is the same but on day 2014-7-27

    def matches_both(*args):
        rows = _cli_csv_search(('datasets',) + args, clirunner)
        assert len(rows) == 2
        assert {rows[0]['id'], rows[1]['id']} == {str(pseudo_ls8_dataset.id), str(pseudo_ls8_dataset2.id)}

    def matches_1(*args):
        rows = _cli_csv_search(('datasets',) + args, clirunner)
        assert len(rows) == 1
        assert rows[0]['id'] == str(pseudo_ls8_dataset.id)

    def matches_none(*args):
        rows = _cli_csv_search(('datasets',) + args, clirunner)
        assert len(rows) == 0

    def no_such_product(*args):
        with pytest.raises(ValueError):
            _cli_csv_search(('datasets',) + args, clirunner)

    matches_both('lat in [-40, -10]')
    matches_both('product=' + pseudo_ls8_type.name)

    # Don't return on a mismatch
    matches_none('lat in [150, 160]')

    # Match only a single dataset using multiple fields
    matches_1('platform=LANDSAT_8', 'time in [2014-07-24, 2014-07-26]')

    # One matching field, one non-matching
    no_such_product('time in [2014-07-24, 2014-07-26]', 'platform=LANDSAT_5')

    # Test date shorthand
    matches_both('time in [2014-07, 2014-07]')
    matches_none('time in [2014-06, 2014-06]')

    matches_both('time in 2014-07')
    matches_none('time in 2014-08')
    matches_both('time in 2014')
    matches_none('time in 2015')

    matches_both('time in [2014, 2014]')
    matches_both('time in [2013, 2014]')
    matches_none('time in [2015, 2015]')
    matches_none('time in [2013, 2013]')

    matches_both('time in [2014-7, 2014-8]')
    matches_none('time in [2014-6, 2014-6]')
    matches_both('time in [2005, 2015]')


# Headers are currently in alphabetical order.
_EXPECTED_OUTPUT_HEADER_LEGACY = 'creation_time,dataset_type_id,format,gsi,id,indexed_by,indexed_time,' \
    'instrument,label,lat,lon,metadata_doc,metadata_type,metadata_type_id,' \
    'orbit,platform,product,product_type,sat_path,sat_row,time,uri'

_EXPECTED_OUTPUT_HEADER = 'creation_time,format,gsi,id,indexed_by,indexed_time,instrument,label,' \
    'lat,lon,metadata_doc,metadata_type,metadata_type_id,orbit,platform,' \
    'product,product_id,product_type,sat_path,sat_row,time,uri'


def test_csv_structure(clirunner, pseudo_ls8_type, ls5_telem_type,
                       pseudo_ls8_dataset, pseudo_ls8_dataset2):
    output = _csv_search_raw(['datasets', ' lat in [-40, -10]'], clirunner)
    lines = [line.strip() for line in output.split('\n') if line]
    # A header and two dataset rows
    assert len(lines) == 3
    header_line = lines[0]
    assert header_line in (_EXPECTED_OUTPUT_HEADER, _EXPECTED_OUTPUT_HEADER_LEGACY)


# Current formulation of this test relies on non-EO3 test data
@pytest.mark.parametrize('datacube_env_name', ('datacube', ))
def test_query_dataset_multi_product(index: Index, ls5_dataset_w_children: Dataset):
    # We have one ls5 level1 and its child nbar
    dc = Datacube(index)

    # Can we query a single product name?
    datasets = dc.find_datasets(product='ls5_nbar_scene')
    assert len(datasets) == 1

    # Can we query multiple products?
    datasets = dc.find_datasets(product=['ls5_nbar_scene', 'ls5_level1_scene'])
    assert len(datasets) == 2

    # Can we query multiple products in a tuple
    datasets = dc.find_datasets(product=('ls5_nbar_scene', 'ls5_level1_scene'))
    assert len(datasets) == 2


def _cli_csv_search(args, clirunner):
    # Do a CSV search from the cli, returning results as a list of dictionaries
    output = _csv_search_raw(args, clirunner)
    return list(csv.DictReader(io.StringIO(output)))


def _csv_search_raw(args, clirunner):
    # Do a CSV search from the cli, returning output as a string
    result = clirunner(['-f', 'csv'] + list(args), cli_method=datacube.scripts.search_tool.cli, verbose_flag=False)
    output = result.output
    output_lines = output.split("\n")
    return "\n".join(line for line in output_lines if "WARNING:" not in line)

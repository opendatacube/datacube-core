# coding=utf-8
"""
Module
"""
from __future__ import absolute_import

import datetime
from pathlib import Path

import pytest

_telemetry_uuid = '4ec8fe97-e8b9-11e4-87ff-1040f381a756'
_telemetry_dataset = {
    'product_type': 'satellite_telemetry_data',
    'checksum_path': 'package.sha1',
    'id': _telemetry_uuid,
    'ga_label': 'LS8_OLITIRS_STD-MD_P00_LC81160740742015089ASA00_'
                '116_074_20150330T022553Z20150330T022657',

    'ga_level': 'P00',
    'size_bytes': 637660782,
    'platform': {
        'code': 'LANDSAT_8'
    },
    # We're unlikely to have extent info for a raw dataset, we'll use it for search tests.
    'extent': {
        'center_dt': datetime.datetime(2014, 7, 26, 23, 49, 0, 343853),
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
        'name': 'MD'
    },
    'lineage': {
        'source_datasets': {}
    }
}


def test_search_dataset_equals(index, db, default_collection):
    """
    :type db: datacube.index.postgres._api.PostgresDb
    :type index: datacube.index._api.Index
    """
    was_inserted = db.insert_dataset(
        _telemetry_dataset,
        _telemetry_uuid,
        Path('/tmp/test/' + _telemetry_uuid)
    )
    assert was_inserted

    field = index.datasets.get_field

    datasets = index.datasets.search_eager(
        field('satellite') == 'LANDSAT_8',
    )
    assert len(datasets) == 1
    assert datasets[0].id == _telemetry_uuid

    datasets = index.datasets.search_eager(
        field('satellite') == 'LANDSAT_8',
        field('sensor') == 'OLI_TIRS',
    )
    assert len(datasets) == 1
    assert datasets[0].id == _telemetry_uuid

    # Wrong sensor name
    datasets = index.datasets.search_eager(
        field('satellite') == 'LANDSAT-8',
        field('sensor') == 'TM',
    )
    assert len(datasets) == 0


def test_search_dataset_ranges(index, db, default_collection):
    """
    :type db: datacube.index.postgres._api.PostgresDb
    :type index: datacube.index._api.Index
    """
    was_inserted = db.insert_dataset(
        _telemetry_dataset,
        _telemetry_uuid,
        Path('/tmp/test/' + _telemetry_uuid)
    )
    assert was_inserted

    field = index.datasets.get_field

    # In the lat bounds.
    datasets = index.datasets.search_eager(
        field('lat').between(-30.5, -29.5)
    )
    assert len(datasets) == 1
    assert datasets[0].id == _telemetry_uuid

    # Out of the lat bounds.
    datasets = index.datasets.search_eager(
        field('lat').between(28, 32)
    )
    assert len(datasets) == 0

    # A dataset that overlaps but is not fully contained by the search bounds.
    # TODO: Do we want overlap as the default behaviour?
    # Should we distinguish between 'contains' and 'overlaps'?
    datasets = index.datasets.search_eager(
        field('lat').between(-40, -30)
    )
    assert len(datasets) == 1
    assert datasets[0].id == _telemetry_uuid


def test_search_globally(index, db, telemetry_collection):
    """
    :type db: datacube.index.postgres._api.PostgresDb
    :type index: datacube.index._api.Index
    :type telemetry_collection: datacube.model.Collection
    """
    # Insert dataset. It should be matched to the telemetry collection.
    was_inserted = db.insert_dataset(
        _telemetry_dataset,
        _telemetry_uuid,
        # Path is optional.
    )
    assert was_inserted

    # No expressions means get all.
    results = list(index.datasets.search())
    assert len(results) == 1


def test_searches_only_collection(index, db, default_collection, telemetry_collection):
    """
    :type db: datacube.index.postgres._api.PostgresDb
    :type index: datacube.index._api.Index
    :type default_collection: datacube.model.Collection
    :type telemetry_collection: datacube.model.Collection
    """
    # Insert dataset. It should be matched to the telemetry collection.
    was_inserted = db.insert_dataset(
        _telemetry_dataset,
        _telemetry_uuid,
        Path('/tmp/test/' + _telemetry_uuid)
    )
    assert was_inserted

    # No results on the default collection.
    default_field = default_collection.dataset_fields.get
    datasets = index.datasets.search_eager(
        default_field('satellite') == 'LANDSAT_8',
        default_field('sensor') == 'OLI_TIRS'
    )
    assert len(datasets) == 0

    # One result in the telemetry collection.
    telemetry_field = telemetry_collection.dataset_fields.get
    datasets = index.datasets.search_eager(
        telemetry_field('satellite') == 'LANDSAT_8',
        telemetry_field('sensor') == 'OLI_TIRS',
    )
    assert len(datasets) == 1
    assert datasets[0].id == _telemetry_uuid

    # An error if you mix collections (although we may support this in the future):
    with pytest.raises(ValueError):
        index.datasets.search_eager(
            default_field('satellite') == 'LANDSAT_8',
            telemetry_field('sensor') == 'OLI_TIRS',
        )

    field = index.datasets.get_field
    # Specify explicit collection and a parameter.
    results = index.datasets.search_eager(
        field('collection') == 'landsat_telemetry',
        telemetry_field('satellite') == 'LANDSAT_8'
    )
    assert len(results) == 1


def test_fetch_all_of_collection(index, db, default_collection, telemetry_collection):
    """
    :type db: datacube.index.postgres._api.PostgresDb
    :type index: datacube.index._api.Index
    :type default_collection: datacube.model.Collection
    :type telemetry_collection: datacube.model.Collection
    """
    # Insert dataset. It should be matched to the telemetry collection.
    was_inserted = db.insert_dataset(
        _telemetry_dataset,
        _telemetry_uuid,
        Path('/tmp/test/' + _telemetry_uuid)
    )
    assert was_inserted

    field = index.datasets.get_field

    # Get every dataset in the collection
    results = index.datasets.search_eager(
        field('collection') == 'landsat_telemetry'
    )
    assert len(results) == 1

    results = index.datasets.search_eager(
        field('collection') == 'eo'
    )
    assert len(results) == 0


# coding=utf-8
"""
Test database methods.

Integration tests: these depend on a local Postgres instance.
"""
from __future__ import absolute_import

import datetime

import pytest
from pathlib import Path

from datacube.index.exceptions import DuplicateRecordError
from datacube.model import Dataset

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
        'center_dt': datetime.datetime(2014, 7, 26, 23, 49, 0, 343853).isoformat(),
        'coord': {
            'll': {'lat': -31.33333, 'lon': 149.78434},
            'lr': {'lat': -31.37116, 'lon': 152.20094},
            'ul': {'lat': -29.23394, 'lon': 149.85216},
            'ur': {'lat': -29.26873, 'lon': 152.21782}
        }
    },
    'creation_dt': datetime.datetime(2015, 4, 22, 6, 32, 4).isoformat(),
    'instrument': {'name': 'OLI_TIRS'},
    'format': {
        'name': 'MD'
    },
    'lineage': {
        'source_datasets': {},
        'blah': float('NaN')
    }
}

_pseudo_telemetry_dataset_type = {
    'name': 'ls8_telemetry',
    'description': 'LS8 test',
    'metadata': {
        'product_type': 'satellite_telemetry_data',
        'platform': {
            'code': 'LANDSAT_8'
        },
        'format': {
            'name': 'MD'
        }
    },
    'metadata_type': 'eo'
}
_EXAMPLE_LS7_NBAR_DATASET_FILE = Path(__file__).parent.joinpath('ls7-nbar-example.yaml')


def test_archive_datasets(index, db, local_config, default_metadata_type):
    with db.begin() as transaction:
        dataset_type = index.datasets.types.add_document(_pseudo_telemetry_dataset_type)
        was_inserted = db.insert_dataset(
            _telemetry_dataset,
            _telemetry_uuid,
            dataset_type.id
        )

    assert was_inserted
    assert db.contains_dataset(_telemetry_uuid)

    datsets = index.datasets.search_eager()
    assert len(datsets) == 1

    index.datasets.archive([_telemetry_uuid])
    datsets = index.datasets.search_eager()
    assert len(datsets) == 0

    index.datasets.restore([_telemetry_uuid])
    datsets = index.datasets.search_eager()
    assert len(datsets) == 1


def test_index_duplicate_dataset(index, db, local_config, default_metadata_type):
    """
    :type index: datacube.index._api.Index
    :type db: datacube.index.postgres._api.PostgresDb
    """

    assert not db.contains_dataset(_telemetry_uuid)

    dataset_type = index.datasets.types.add_document(_pseudo_telemetry_dataset_type)
    with db.begin() as transaction:
        was_inserted = transaction.insert_dataset(
            _telemetry_dataset,
            _telemetry_uuid,
            dataset_type.id
        )

    assert was_inserted
    assert db.contains_dataset(_telemetry_uuid)

    was_inserted = False
    with pytest.raises(DuplicateRecordError):
        # Insert again. It should be ignored.
        was_inserted = db.insert_dataset(
            _telemetry_dataset,
            _telemetry_uuid,
            dataset_type.id
        )
    assert not was_inserted
    assert db.contains_dataset(_telemetry_uuid)


def test_transactions(index, db, local_config, default_metadata_type):
    """
    :type index: datacube.index._api.Index
    :type db: datacube.index.postgres._api.PostgresDb
    """
    assert not db.contains_dataset(_telemetry_uuid)

    dataset_type = index.datasets.types.add_document(_pseudo_telemetry_dataset_type)
    with db.begin() as transaction:
        was_inserted = transaction.insert_dataset(
            _telemetry_dataset,
            _telemetry_uuid,
            dataset_type.id
        )
        assert was_inserted
        assert transaction.contains_dataset(_telemetry_uuid)
        # Normal DB uses a separate connection: No dataset visible yet.
        assert not db.contains_dataset(_telemetry_uuid)

        transaction.rollback()

    # Should have been rolled back.
    assert not db.contains_dataset(_telemetry_uuid)


def test_index_dataset_with_location(index, default_metadata_type):
    """
    :type index: datacube.index._api.Index
    :type default_collection: datacube.model.DatasetType
    """
    first_file = Path('/tmp/first/something.yaml').absolute()
    second_file = Path('/tmp/second/something.yaml').absolute()

    type_ = index.datasets.types.add_document(_pseudo_telemetry_dataset_type)
    dataset = Dataset(type_, _telemetry_dataset, first_file.as_uri())
    index.datasets.add(dataset)
    stored = index.datasets.get(dataset.id)

    assert stored.id == _telemetry_uuid
    # TODO: Dataset types?
    assert stored.type.id == type_.id
    assert stored.metadata_type.id == default_metadata_type.id
    assert stored.local_path == Path(first_file)

    # Ingesting again should have no effect.
    index.datasets.add(dataset)
    stored = index.datasets.get(dataset.id)
    locations = index.datasets.get_locations(dataset)
    assert len(locations) == 1

    # Ingesting with a new path should add the second one too.
    dataset.local_uri = second_file.as_uri()
    index.datasets.add(dataset)
    stored = index.datasets.get(dataset.id)
    locations = index.datasets.get_locations(dataset)
    assert len(locations) == 2
    # Newest to oldest.
    assert locations == [second_file.as_uri(), first_file.as_uri()]
    # And the second one is newer, so it should be returned as the default local path:
    assert stored.local_path == Path(second_file)

    # Ingestion again without location should have no effect.
    dataset.local_uri = None
    index.datasets.add(dataset)
    stored = index.datasets.get(dataset.id)
    locations = index.datasets.get_locations(dataset)
    assert len(locations) == 2
    # Newest to oldest.
    assert locations == [second_file.as_uri(), first_file.as_uri()]
    # And the second one is newer, so it should be returned as the default local path:
    assert stored.local_path == Path(second_file)

# coding=utf-8
"""
Test database methods.

Integration tests: these depend on a local Postgres instance.
"""
from __future__ import absolute_import

import datetime

from pathlib import Path

from datacube.index.postgres import PostgresDb
from datacube.index.postgres.tables import STORAGE_TYPE, STORAGE_UNIT
from datacube.index.postgres.tables._storage import DATASET_STORAGE
from datacube.model import StorageUnit, StorageType

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


def test_index_dataset_in_transactions(index, db, local_config, default_collection):
    """
    :type index: datacube.index._api.Index
    :type db: datacube.index.postgres._api.PostgresDb
    """

    assert not db.contains_dataset(_telemetry_uuid)

    with db.begin() as transaction:
        was_inserted = db.insert_dataset(
            _telemetry_dataset,
            _telemetry_uuid
        )

        assert was_inserted
        assert db.contains_dataset(_telemetry_uuid)

        # Insert again. It should be ignored.
        was_inserted = db.insert_dataset(
            _telemetry_dataset,
            _telemetry_uuid
        )
        assert not was_inserted
        assert db.contains_dataset(_telemetry_uuid)

        transaction.rollback()

    # Rollback should leave a blank database:
    assert not db.contains_dataset(_telemetry_uuid)

    # Check with a new connection too:
    db = PostgresDb.from_config(local_config)
    assert not db.contains_dataset(_telemetry_uuid)


def test_index_dataset_with_location(index, default_collection):
    """
    :type index: datacube.index._api.Index
    :type default_collection: datacube.model.Collection
    """
    dataset = index.datasets.add(
        _telemetry_dataset,
        metadata_path=Path('/tmp/something.yaml')
    )

    assert dataset.id == _telemetry_uuid
    assert dataset.collection.id_ == default_collection.id_
    assert dataset.collection.id_ == default_collection.id_
    assert dataset.metadata_type.id_ == default_collection.metadata_type.id_

    assert dataset.local_path == Path('/tmp/something.yaml')


def test_index_storage_unit(index, db, default_collection):
    """
    :type db: datacube.index.postgres._api.PostgresDb
    :type index: datacube.index._api.Index
    """

    # Setup foreign keys for our storage unit.
    was_inserted = db.insert_dataset(
        _telemetry_dataset,
        _telemetry_uuid
    )
    assert was_inserted
    db.ensure_storage_type(
        'test_storage_mapping',
        {},
        {},
    )
    storage_type = db._connection.execute(STORAGE_TYPE.select()).first()

    # Add storage unit
    index.storage.add(
        StorageUnit(
            [_telemetry_uuid],
            StorageType(
                name='test_storage_mapping',
                description=None,
                match=None,
                measurements={},
                location="file://g/data",
                filename_pattern="foo.nc",
                roi=None,
                definition={},
                id_=storage_type['id']
            ),
            {'test': 'descriptor'},
            '/test/offset'
        )
    )

    units = db._connection.execute(STORAGE_UNIT.select()).fetchall()
    assert len(units) == 1
    unit = units[0]
    assert unit['descriptor'] == {'test': 'descriptor'}
    assert unit['path'] == '/test/offset'
    assert unit['storage_type_ref'] == storage_type['id']

    # Dataset and storage should have been linked.
    d_ss = db._connection.execute(DATASET_STORAGE.select()).fetchall()
    assert len(d_ss) == 1
    d_s = d_ss[0]
    assert d_s['dataset_ref'] == _telemetry_uuid
    assert d_s['storage_unit_ref'] == unit['id']

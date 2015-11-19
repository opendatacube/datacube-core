# coding=utf-8
"""
Test database methods.

Integration tests: these depend on a local Postgres instance.
"""
from __future__ import absolute_import

import datetime

from datacube.index._data import DataIndex
from datacube.index.db.tables import STORAGE_MAPPING, STORAGE_UNIT
from datacube.index.db.tables._storage import DATASET_STORAGE
from datacube.model import StorageUnit, StorageMapping
from integration_tests.index._common import init_db, connect_db

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
    'creation_dt': datetime.datetime(2015, 4, 22, 6, 32, 4),
    'instrument': {'name': 'OLI_TIRS'},
    'format': {
        'name': 'MD'
    },
    'lineage': {
        'source_datasets': {}
    }
}


def test_index_dataset():
    db = init_db()

    assert not db.contains_dataset(_telemetry_uuid)

    with db.begin() as transaction:
        was_inserted = db.insert_dataset(
            _telemetry_dataset,
            _telemetry_uuid,
            '/tmp/test/' + _telemetry_uuid,
            'satellite_telemetry_data'
        )

        assert was_inserted
        assert db.contains_dataset(_telemetry_uuid)

        # Insert again. It should be ignored.
        was_inserted = db.insert_dataset(
            _telemetry_dataset,
            _telemetry_uuid,
            '/tmp/test/' + _telemetry_uuid,
            'satellite_telemetry_data'
        )
        assert not was_inserted
        assert db.contains_dataset(_telemetry_uuid)

        transaction.rollback()

    # Rollback should leave a blank database:
    assert not db.contains_dataset(_telemetry_uuid)

    # Check with a new connection too:
    db = connect_db()
    assert not db.contains_dataset(_telemetry_uuid)


def test_index_storage_unit():
    db = init_db()
    index = DataIndex(db)

    # Setup foreign keys for our storage unit.
    was_inserted = db.insert_dataset(
        _telemetry_dataset,
        _telemetry_uuid,
        '/tmp/test/' + _telemetry_uuid,
        'satellite_telemetry_data'
    )
    assert was_inserted
    db.ensure_storage_type(
        'NetCDF CF',
        'test_storage_type',
        {'storage_type': 'descriptor'}
    )
    db.ensure_storage_mapping(
        'NetCDF CF',
        'test_storage_type',
        'Test storage mapping',
        'location1', '/tmp/some/loc', {}, [], {}
    )
    mapping = db._connection.execute(STORAGE_MAPPING.select()).first()

    # Add storage unit
    index.add_storage_unit(
        StorageUnit(
            [_telemetry_uuid],
            StorageMapping(
                # Yikes:
                None, None, None, None, None, None,
                id_=mapping['id']
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
    assert unit['storage_mapping_ref'] == mapping['id']

    # Dataset and storage should have been linked.
    d_ss = db._connection.execute(DATASET_STORAGE.select()).fetchall()
    assert len(d_ss) == 1
    d_s = d_ss[0]
    assert d_s['dataset_ref'] == _telemetry_uuid
    assert d_s['storage_unit_ref'] == unit['id']


def test_search_dataset():
    db = init_db()

    # Setup foreign keys for our storage unit.
    was_inserted = db.insert_dataset(
        _telemetry_dataset,
        _telemetry_uuid,
        '/tmp/test/' + _telemetry_uuid,
        'satellite_telemetry_data'
    )
    assert was_inserted

    field = db.get_dataset_field

    datasets = db.search_datasets_eager(
        field('satellite') == 'LANDSAT_8',
    )
    assert len(datasets) == 1
    assert datasets[0]['id'] == _telemetry_uuid

    datasets = db.search_datasets_eager(
        field('satellite') == 'LANDSAT_8',
        field('sensor') == 'OLI_TIRS',
    )
    assert len(datasets) == 1
    assert datasets[0]['id'] == _telemetry_uuid

    # Wrong sensor name
    datasets = db.search_datasets_eager(
        field('satellite') == 'LANDSAT-8',
        field('sensor') == 'TM',
    )
    assert len(datasets) == 0

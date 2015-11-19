# coding=utf-8
"""
Test database methods.

Integration tests: these depend on a local Postgres instance.
"""
from __future__ import absolute_import

import datetime

from datacube.index._data import DataIndex
from datacube.index.postgres.tables import STORAGE_MAPPING, STORAGE_UNIT
from datacube.index.postgres.tables._storage import DATASET_STORAGE
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
                storage_type=None,
                name="test_mapping",
                match=None,
                measurements={},
                dataset_measurements_offset=None,
                location="file://g/data",
                filename_pattern="foo.nc",
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


def test_search_dataset_equals():
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

    field = index.get_dataset_field

    datasets = index.search_datasets_eager(
        field('satellite') == 'LANDSAT_8',
    )
    assert len(datasets) == 1
    assert datasets[0]['id'] == _telemetry_uuid

    datasets = index.search_datasets_eager(
        field('satellite') == 'LANDSAT_8',
        field('sensor') == 'OLI_TIRS',
    )
    assert len(datasets) == 1
    assert datasets[0]['id'] == _telemetry_uuid

    # Wrong sensor name
    datasets = index.search_datasets_eager(
        field('satellite') == 'LANDSAT-8',
        field('sensor') == 'TM',
    )
    assert len(datasets) == 0


def test_search_dataset_ranges():
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

    field = index.get_dataset_field

    # In the lat bounds.
    datasets = index.search_datasets_eager(
        field('lat').between(-30.5, -29.5)
    )
    assert len(datasets) == 1
    assert datasets[0]['id'] == _telemetry_uuid

    # Out of the lat bounds.
    datasets = index.search_datasets_eager(
        field('lat').between(28, 32)
    )
    assert len(datasets) == 0

    # A dataset that overlaps but is not fully contained by the search bounds.
    # TODO: Do we want overlap as the default behaviour?
    # Should we distinguish between 'contains' and 'overlaps'?
    datasets = index.search_datasets_eager(
        field('lat').between(-40, -30)
    )
    assert len(datasets) == 1
    assert datasets[0]['id'] == _telemetry_uuid

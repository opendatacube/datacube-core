# coding=utf-8
"""
Test database methods.

Integration tests: these depend on a local Postgres instance.
"""
from __future__ import absolute_import

import datetime

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
        'code': 'LANDSAT_8'},
    'creation_dt': datetime.datetime(2015, 4, 22, 6, 32, 4),
    'instrument': {'name': 'OLI_TIRS'},
    'format': {
        'name': 'MD'},
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


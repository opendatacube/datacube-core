# coding=utf-8
"""
Test database methods.

Integration tests: these depend on a local Postgres instance.
"""
from __future__ import absolute_import

import datetime
import uuid

from pathlib import Path

from datacube.index.postgres import PostgresDb
from datacube.index.postgres.tables import STORAGE_TYPE, DATASET, DATASET_SOURCE, DATASET_LOCATION
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

_pseudo_telemetry_dataset_type = {
    'name': 'ls8_telemetry',
    'match': {
        'metadata': {
            'product_type': 'satellite_telemetry_data',
            'platform': {
                'code': 'LANDSAT_8'
            },
            'format': {
                'name': 'MD'
            }
        }
    },
    'metadata_type': 'eo'
}
_EXAMPLE_LS7_NBAR_DATASET_FILE = Path(__file__).parent.joinpath('ls7-nbar-example.yaml')


def test_index_dataset_in_transactions(index, db, local_config, default_metadata_type):
    """
    :type index: datacube.index._api.Index
    :type db: datacube.index.postgres._api.PostgresDb
    """

    assert not db.contains_dataset(_telemetry_uuid)

    with db.begin() as transaction:
        index.datasets.types.add(_pseudo_telemetry_dataset_type)
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


def test_index_dataset_with_location(index, default_metadata_type):
    """
    :type index: datacube.index._api.Index
    :type default_collection: datacube.model.DatasetType
    """
    first_file = '/tmp/first/something.yaml'
    second_file = '/tmp/second/something.yaml'
    type_ = index.datasets.types.add(_pseudo_telemetry_dataset_type)
    dataset = index.datasets.add(
        _telemetry_dataset,
        metadata_path=Path(first_file)
    )

    assert dataset.id == _telemetry_uuid
    # TODO: Dataset types?
    assert dataset.type_.id == type_.id
    assert dataset.metadata_type.id == default_metadata_type.id

    assert dataset.local_path.absolute() == Path(first_file).absolute()

    # Ingesting again should have no effect.
    index.datasets.add(
        _telemetry_dataset,
        metadata_path=Path(first_file)
    )
    locations = index.datasets.get_locations(dataset)
    assert len(locations) == 1

    first_as_uri = Path(first_file).absolute().as_uri()
    second_as_uri = Path(second_file).absolute().as_uri()

    # Ingesting with a new path should add the second one too.
    dataset = index.datasets.add(
        _telemetry_dataset,
        uri=second_as_uri
    )
    locations = index.datasets.get_locations(dataset)
    assert len(locations) == 2
    # Newest to oldest.
    assert locations == [second_as_uri, first_as_uri]
    # And the second one is newer, so it should be returned as the default local path:
    assert dataset.local_path.absolute() == Path(second_file).absolute()


def test_index_storage_unit(index, db, default_metadata_type):
    """
    :type db: datacube.index.postgres._api.PostgresDb
    :type index: datacube.index._api.Index
    """

    # Setup foreign keys for our storage unit.
    type_ = index.datasets.types.add(_pseudo_telemetry_dataset_type)
    was_inserted = db.insert_dataset(
        _telemetry_dataset,
        _telemetry_uuid
    )
    assert was_inserted
    db.ensure_storage_type(
        'test_storage_mapping',
        {},
        {'storage': {'dimension_order': []}},
        target_dataset_id=type_.id
    )
    storage_type = db._connection.execute(STORAGE_TYPE.select()).first()
    storage_dataset_type_ = index.datasets.types.add({
        'name': 'ls8_telemetry_storage',
        'match': {
            'metadata': {
                'test': 'descriptor',
            }
        },
        'metadata_type': 'storage_unit'
    })

    # Add storage unit

    storage_unit = StorageUnit(
        id_=str(uuid.uuid4()),
        dataset_ids=[str(_telemetry_uuid)],
        storage_type=StorageType(
            document={
                'name': 'test_storage_mapping',
                'location': "file://g/data",
                'filename_pattern': "foo.nc",
            },
            target_dataset_type_id=type_.id,
            id_=storage_type['id']
        ),
        descriptor={
            'test': 'descriptor',
            'extent': {
                'center_dt': datetime.datetime(2014, 7, 26, 23, 49, 0, 343853).isoformat(),
            }
        },
        relative_path='/test/offset',
        size_bytes=1234
    )
    index.storage.add(
        storage_unit
    )

    units = db._connection.execute(DATASET.select().where(DATASET.c.storage_type_ref == storage_type.id)).fetchall()
    assert len(units) == 1
    unit = units[0]
    assert unit['metadata'] == {'test': 'descriptor', 'extent': {'center_dt': '2014-07-26T23:49:00.343853'}}

    assert unit['storage_type_ref'] == storage_type['id']
    # assert unit['size_bytes'] == 1234
    assert unit['dataset_type_ref'] == storage_dataset_type_.id

    locations = db._connection.execute(
        DATASET_LOCATION.select().where(DATASET_LOCATION.c.dataset_ref == unit['id'])).fetchall()
    assert locations[0]['uri_scheme'] == 'file'
    assert locations[0]['uri_body'] == '///test/offset'
    assert locations[0]['managed'] == True

    # Storage should have been linked to the source dataset.
    d_ss = db._connection.execute(DATASET_SOURCE.select()).fetchall()
    assert len(d_ss) == 1
    d_s = d_ss[0]
    assert d_s['dataset_ref'] == unit['id']
    assert d_s['source_dataset_ref'] == _telemetry_uuid
    assert d_s['classifier'] == '2014-07-26T23:49:00'

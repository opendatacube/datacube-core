# coding=utf-8
"""
Test database methods.

Integration tests: these depend on a local Postgres instance.
"""
import copy
import datetime
import sys
from pathlib import Path
from uuid import UUID

import pytest
from dateutil import tz

from datacube.drivers.postgres import PostgresDb
from datacube.index.exceptions import MissingRecordError
from datacube.index.index import Index
from datacube.model import Dataset, MetadataType

_telemetry_uuid = UUID('4ec8fe97-e8b9-11e4-87ff-1040f381a756')
_telemetry_dataset = {
    'product_type': 'satellite_telemetry_data',
    'checksum_path': 'package.sha1',
    'id': str(_telemetry_uuid),
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


def test_archive_datasets(index, initialised_postgres_db, local_config, default_metadata_type):
    dataset_type = index.products.add_document(_pseudo_telemetry_dataset_type)
    with initialised_postgres_db.begin() as transaction:
        was_inserted = transaction.insert_dataset(
            _telemetry_dataset,
            _telemetry_uuid,
            dataset_type.id
        )

    assert was_inserted
    assert index.datasets.has(_telemetry_uuid)

    datasets = index.datasets.search_eager()
    assert len(datasets) == 1
    assert datasets[0].is_active

    index.datasets.archive([_telemetry_uuid])
    datasets = index.datasets.search_eager()
    assert len(datasets) == 0

    # The model should show it as archived now.
    indexed_dataset = index.datasets.get(_telemetry_uuid)
    assert indexed_dataset.is_archived
    assert not indexed_dataset.is_active

    index.datasets.restore([_telemetry_uuid])
    datasets = index.datasets.search_eager()
    assert len(datasets) == 1

    # And now active
    indexed_dataset = index.datasets.get(_telemetry_uuid)
    assert indexed_dataset.is_active
    assert not indexed_dataset.is_archived


@pytest.fixture
def telemetry_dataset(index: Index, initialised_postgres_db: PostgresDb, default_metadata_type) -> Dataset:
    dataset_type = index.products.add_document(_pseudo_telemetry_dataset_type)
    assert not index.datasets.has(_telemetry_uuid)

    with initialised_postgres_db.begin() as transaction:
        was_inserted = transaction.insert_dataset(
            _telemetry_dataset,
            _telemetry_uuid,
            dataset_type.id
        )
    assert was_inserted

    return index.datasets.get(_telemetry_uuid)


def test_index_duplicate_dataset(index: Index, initialised_postgres_db: PostgresDb,
                                 local_config,
                                 default_metadata_type) -> None:
    dataset_type = index.products.add_document(_pseudo_telemetry_dataset_type)
    assert not index.datasets.has(_telemetry_uuid)

    with initialised_postgres_db.begin() as transaction:
        was_inserted = transaction.insert_dataset(
            _telemetry_dataset,
            _telemetry_uuid,
            dataset_type.id
        )

    assert was_inserted
    assert index.datasets.has(_telemetry_uuid)

    # Insert again.
    with initialised_postgres_db.connect() as connection:
        was_inserted = connection.insert_dataset(
            _telemetry_dataset,
            _telemetry_uuid,
            dataset_type.id
        )
        assert was_inserted is False

    assert index.datasets.has(_telemetry_uuid)


def test_has_dataset(index: Index, telemetry_dataset: Dataset) -> None:
    assert index.datasets.has(_telemetry_uuid)
    assert index.datasets.has(str(_telemetry_uuid))

    assert not index.datasets.has(UUID('f226a278-e422-11e6-b501-185e0f80a5c0'))
    assert not index.datasets.has('f226a278-e422-11e6-b501-185e0f80a5c0')

    assert index.datasets.bulk_has([_telemetry_uuid, UUID('f226a278-e422-11e6-b501-185e0f80a5c0')]) == [True, False]
    assert index.datasets.bulk_has([str(_telemetry_uuid), 'f226a278-e422-11e6-b501-185e0f80a5c0']) == [True, False]


def test_get_dataset(index: Index, telemetry_dataset: Dataset) -> None:
    assert index.datasets.has(_telemetry_uuid)
    assert index.datasets.has(str(_telemetry_uuid))

    assert index.datasets.bulk_has([_telemetry_uuid, 'f226a278-e422-11e6-b501-185e0f80a5c0']) == [True, False]

    for tr in (lambda x: x, lambda x: str(x)):
        ds = index.datasets.get(tr(_telemetry_uuid))
        assert ds.id == _telemetry_uuid

        ds, = index.datasets.bulk_get([tr(_telemetry_uuid)])
        assert ds.id == _telemetry_uuid

    assert index.datasets.bulk_get(['f226a278-e422-11e6-b501-185e0f80a5c0',
                                    'f226a278-e422-11e6-b501-185e0f80a5c1']) == []


def test_transactions(index: Index,
                      initialised_postgres_db: PostgresDb,
                      local_config,
                      default_metadata_type) -> None:
    assert not index.datasets.has(_telemetry_uuid)

    dataset_type = index.products.add_document(_pseudo_telemetry_dataset_type)
    with initialised_postgres_db.begin() as transaction:
        was_inserted = transaction.insert_dataset(
            _telemetry_dataset,
            _telemetry_uuid,
            dataset_type.id
        )
        assert was_inserted
        assert transaction.contains_dataset(_telemetry_uuid)
        # Normal DB uses a separate connection: No dataset visible yet.
        assert not index.datasets.has(_telemetry_uuid)

        transaction.rollback()

    # Should have been rolled back.
    assert not index.datasets.has(_telemetry_uuid)


def test_get_missing_things(index: Index) -> None:
    """
    The get(id) methods should return None if the object doesn't exist.
    """
    uuid_ = UUID('18474b58-c8a6-11e6-a4b3-185e0f80a5c0')
    missing_thing = index.datasets.get(uuid_, include_sources=False)
    assert missing_thing is None, "get() should return none when it doesn't exist"

    missing_thing = index.datasets.get(uuid_, include_sources=True)
    assert missing_thing is None, "get() should return none when it doesn't exist"

    id_ = sys.maxsize
    missing_thing = index.metadata_types.get(id_)
    assert missing_thing is None, "get() should return none when it doesn't exist"

    missing_thing = index.products.get(id_)
    assert missing_thing is None, "get() should return none when it doesn't exist"


def test_index_dataset_with_sources(index, default_metadata_type):
    type_ = index.products.add_document(_pseudo_telemetry_dataset_type)

    parent_doc = _telemetry_dataset.copy()
    parent = Dataset(type_, parent_doc, None, sources={})
    child_doc = _telemetry_dataset.copy()
    child_doc['lineage'] = {'source_datasets': {'source': _telemetry_dataset}}
    child_doc['id'] = '051a003f-5bba-43c7-b5f1-7f1da3ae9cfb'
    child = Dataset(type_, child_doc, sources={'source': parent})

    with pytest.raises(MissingRecordError):
        index.datasets.add(child, with_lineage=False)

    index.datasets.add(child)
    assert index.datasets.get(parent.id)
    assert index.datasets.get(child.id)

    assert len(index.datasets.bulk_get([parent.id, child.id])) == 2

    index.datasets.add(child, with_lineage=False)
    index.datasets.add(child, with_lineage=True)

    parent_doc['platform'] = {'code': 'LANDSAT_9'}
    index.datasets.add(child, with_lineage=True)
    index.datasets.add(child, with_lineage=False)

    # backwards compatibility code path checks, don't use this in normal code
    for p in ('skip', 'ensure', 'verify'):
        index.datasets.add(child, sources_policy=p)


@pytest.mark.parametrize('datacube_env_name', ('datacube', ), indirect=True)
def test_index_dataset_with_location(index: Index, default_metadata_type: MetadataType):
    first_file = Path('/tmp/first/something.yaml').absolute()
    second_file = Path('/tmp/second/something.yaml').absolute()

    type_ = index.products.add_document(_pseudo_telemetry_dataset_type)
    dataset = Dataset(type_, _telemetry_dataset, uris=[first_file.as_uri()], sources={})
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
    locations = index.datasets.get_locations(dataset.id)
    assert len(locations) == 1
    # Remove the location
    was_removed = index.datasets.remove_location(dataset.id, first_file.as_uri())
    assert was_removed
    was_removed = index.datasets.remove_location(dataset.id, first_file.as_uri())
    assert not was_removed
    locations = index.datasets.get_locations(dataset.id)
    assert len(locations) == 0
    # Re-add the location
    was_added = index.datasets.add_location(dataset.id, first_file.as_uri())
    assert was_added
    was_added = index.datasets.add_location(dataset.id, first_file.as_uri())
    assert not was_added
    locations = index.datasets.get_locations(dataset.id)
    assert len(locations) == 1

    # A rough date is ok: 1:01 beforehand just in case someone runs this during daylight savings time conversion :)
    # (any UTC conversion errors will be off by much more than this for PST/AEST)
    before_archival_dt = utc_now() - datetime.timedelta(hours=1, minutes=1)

    was_archived = index.datasets.archive_location(dataset.id, first_file.as_uri())
    assert was_archived
    locations = index.datasets.get_locations(dataset.id)
    assert locations == []
    locations = index.datasets.get_archived_locations(dataset.id)
    assert locations == [first_file.as_uri()]

    # It should return the time archived.
    location_times = index.datasets.get_archived_location_times(dataset.id)
    assert len(location_times) == 1
    location, archived_time = location_times[0]
    assert location == first_file.as_uri()
    assert utc_now() > archived_time > before_archival_dt

    was_restored = index.datasets.restore_location(dataset.id, first_file.as_uri())
    assert was_restored
    locations = index.datasets.get_locations(dataset.id)
    assert len(locations) == 1

    # Indexing with a new path should NOT add the second one.
    dataset.uris = [second_file.as_uri()]
    index.datasets.add(dataset)
    stored = index.datasets.get(dataset.id)
    locations = index.datasets.get_locations(dataset.id)
    assert len(locations) == 1

    # Add location manually instead
    index.datasets.add_location(dataset.id, second_file.as_uri())
    stored = index.datasets.get(dataset.id)
    assert len(stored.uris) == 2

    # Newest to oldest.
    assert stored.uris == [second_file.as_uri(), first_file.as_uri()]
    # And the second one is newer, so it should be returned as the default local path:
    assert stored.local_path == Path(second_file)

    # Can archive and restore the first file, and location order is preserved
    was_archived = index.datasets.archive_location(dataset.id, first_file.as_uri())
    assert was_archived
    locations = index.datasets.get_locations(dataset.id)
    assert locations == [second_file.as_uri()]
    was_restored = index.datasets.restore_location(dataset.id, first_file.as_uri())
    assert was_restored
    locations = index.datasets.get_locations(dataset.id)
    assert locations == [second_file.as_uri(), first_file.as_uri()]

    # Can archive and restore the second file, and location order is preserved
    was_archived = index.datasets.archive_location(dataset.id, second_file.as_uri())
    assert was_archived
    locations = index.datasets.get_locations(dataset.id)
    assert locations == [first_file.as_uri()]
    was_restored = index.datasets.restore_location(dataset.id, second_file.as_uri())
    assert was_restored
    locations = index.datasets.get_locations(dataset.id)
    assert locations == [second_file.as_uri(), first_file.as_uri()]

    # Indexing again without location should have no effect.
    dataset.uris = []
    index.datasets.add(dataset)
    stored = index.datasets.get(dataset.id)
    locations = index.datasets.get_locations(dataset.id)
    assert len(locations) == 2
    # Newest to oldest.
    assert locations == [second_file.as_uri(), first_file.as_uri()]
    # And the second one is newer, so it should be returned as the default local path:
    assert stored.local_path == Path(second_file)

    # Check order of uris is preserved when indexing with more than one
    second_ds_doc = copy.deepcopy(_telemetry_dataset)
    second_ds_doc['id'] = '366f32d8-e1f8-11e6-94b4-185e0f80a589'
    index.datasets.add(Dataset(type_, second_ds_doc, uris=['file:///a', 'file:///b'], sources={}))

    # test order using get_locations function
    assert index.datasets.get_locations(second_ds_doc['id']) == ['file:///a', 'file:///b']

    # test order using datasets.get(), it has custom query as it turns out
    assert index.datasets.get(second_ds_doc['id']).uris == ['file:///a', 'file:///b']

    # test update, this should prepend file:///c, file:///d to the existing list
    index.datasets.update(Dataset(type_, second_ds_doc, uris=['file:///a', 'file:///c', 'file:///d'], sources={}))
    assert index.datasets.get_locations(second_ds_doc['id']) == ['file:///c', 'file:///d', 'file:///a', 'file:///b']
    assert index.datasets.get(second_ds_doc['id']).uris == ['file:///c', 'file:///d', 'file:///a', 'file:///b']

    # Ability to get datasets for a location
    # Add a second dataset with a different location (to catch lack of joins, filtering etc)
    second_ds_doc = copy.deepcopy(_telemetry_dataset)
    second_ds_doc['id'] = '366f32d8-e1f8-11e6-94b4-185e0f80a5c0'
    index.datasets.add(Dataset(type_, second_ds_doc, uris=[second_file.as_uri()], sources={}))
    for mode in ('exact', 'prefix', None):
        dataset_ids = [d.id for d in index.datasets.get_datasets_for_location(first_file.as_uri(), mode=mode)]
        assert dataset_ids == [dataset.id]

    assert list(index.datasets.get_datasets_for_location(first_file.as_uri() + "#part=100")) == []

    with pytest.raises(ValueError):
        list(index.datasets.get_datasets_for_location(first_file.as_uri(), mode="nosuchmode"))


def utc_now():
    # utcnow() doesn't include a tzinfo.
    return datetime.datetime.utcnow().replace(tzinfo=tz.tzutc())

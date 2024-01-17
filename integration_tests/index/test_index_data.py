# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2024 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
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

from datacube.index.exceptions import MissingRecordError
from datacube.index import Index
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


def test_archive_datasets(index, ls8_eo3_dataset):
    datasets = index.datasets.search_eager()
    assert len(datasets) == 1
    assert not datasets[0].is_archived

    index.datasets.archive([ls8_eo3_dataset.id])
    datasets = index.datasets.search_eager()
    assert len(datasets) == 0

    # The model should show it as archived now.
    indexed_dataset = index.datasets.get(ls8_eo3_dataset.id)
    assert indexed_dataset.is_archived

    index.datasets.restore([ls8_eo3_dataset.id])
    datasets = index.datasets.search_eager()
    assert len(datasets) == 1

    # And now active
    indexed_dataset = index.datasets.get(ls8_eo3_dataset.id)
    assert not indexed_dataset.is_archived


def test_archive_less_mature(index, final_dataset, nrt_dataset, ds_no_region):
    # case 1: add nrt then final; nrt should get archived
    index.datasets.add(nrt_dataset, with_lineage=False, archive_less_mature=True)
    assert not index.datasets.get(nrt_dataset.id).is_archived
    index.datasets.add(final_dataset, with_lineage=False, archive_less_mature=True)
    assert index.datasets.get(nrt_dataset.id).is_archived
    assert not index.datasets.get(final_dataset.id).is_archived

    # case 2: purge nrt; re-add with final already there
    index.datasets.purge([nrt_dataset.id])
    assert index.datasets.get(nrt_dataset.id) is None
    with pytest.raises(ValueError):
        # should error as more mature version of dataset already exists
        index.datasets.add(nrt_dataset, with_lineage=False, archive_less_mature=True)


def test_cannot_search_for_less_mature(index, nrt_dataset, ds_no_region):
    # if a dataset is missing a property required for finding less mature datasets,
    # it should error
    index.datasets.add(nrt_dataset, with_lineage=False, archive_less_mature=0)
    assert not index.datasets.get(nrt_dataset.id).is_archived
    assert ds_no_region.metadata.region_code is None
    with pytest.raises(ValueError, match="region_code"):
        index.datasets.add(ds_no_region, with_lineage=False, archive_less_mature=0)


def test_archive_less_mature_approx_timestamp(index, ga_s2am_ard3_final, ga_s2am_ard3_interim):
    # test archive_less_mature where there's a slight difference in timestamps
    index.datasets.add(ga_s2am_ard3_interim, with_lineage=False)
    assert not index.datasets.get(ga_s2am_ard3_interim.id).is_archived
    index.datasets.add(ga_s2am_ard3_final, with_lineage=False, archive_less_mature=True)
    assert index.datasets.get(ga_s2am_ard3_interim.id).is_archived
    assert not index.datasets.get(ga_s2am_ard3_final.id).is_archived


def test_dont_archive_less_mature(index, final_dataset, nrt_dataset):
    # ensure datasets aren't archive if no archive_less_mature value is provided
    index.datasets.add(nrt_dataset, with_lineage=False)
    assert not index.datasets.get(nrt_dataset.id).is_archived
    index.datasets.add(final_dataset, with_lineage=False, archive_less_mature=None)
    assert not index.datasets.get(nrt_dataset.id).is_archived
    assert not index.datasets.get(final_dataset.id).is_archived


def test_archive_less_mature_bool(index, final_dataset, nrt_dataset):
    # if archive_less_mature value gets passed as a bool via an outdated script
    index.datasets.add(nrt_dataset, with_lineage=False)
    assert not index.datasets.get(nrt_dataset.id).is_archived
    index.datasets.add(final_dataset, with_lineage=False, archive_less_mature=False)
    assert not index.datasets.get(nrt_dataset.id).is_archived
    assert not index.datasets.get(final_dataset.id).is_archived


def test_purge_datasets(index, ls8_eo3_dataset):
    assert index.datasets.has(ls8_eo3_dataset.id)
    datasets = index.datasets.search_eager()
    assert len(datasets) == 1
    assert not datasets[0].is_archived

    # Archive dataset
    index.datasets.archive([ls8_eo3_dataset.id])
    datasets = index.datasets.search_eager()
    assert len(datasets) == 0

    # The model should show it as archived now.
    indexed_dataset = index.datasets.get(ls8_eo3_dataset.id)
    assert indexed_dataset.is_archived

    # Purge dataset
    index.datasets.purge([ls8_eo3_dataset.id])
    assert index.datasets.get(ls8_eo3_dataset.id) is None


def test_purge_datasets_cli(index, ls8_eo3_dataset, clirunner):
    dsid = ls8_eo3_dataset.id

    # Attempt to purge non-archived dataset should fail
    clirunner(['dataset', 'purge', str(dsid)], expect_success=False)

    # Archive dataset
    index.datasets.archive([dsid])
    indexed_dataset = index.datasets.get(dsid)
    assert indexed_dataset.is_archived

    # Test CLI dry run
    clirunner(['dataset', 'purge', '--dry-run', str(dsid)])
    indexed_dataset = index.datasets.get(dsid)
    assert indexed_dataset.is_archived

    # Test CLI purge
    clirunner(['dataset', 'purge', str(dsid)])
    assert index.datasets.get(dsid) is None

    # Attempt to purge non-existent dataset should fail
    clirunner(['dataset', 'purge', str(dsid)], expect_success=False)


def test_purge_all_datasets_cli(index, cfg_env, ls8_eo3_dataset, clirunner):
    product = ls8_eo3_dataset.product.id
    dsid = ls8_eo3_dataset.id

    # archive all datasets
    clirunner(['dataset', 'archive', '--all'])

    indexed_dataset = index.datasets.get(dsid)
    assert indexed_dataset.is_archived

    # Restore all datasets
    clirunner(['dataset', 'restore', '--all'])
    indexed_dataset = index.datasets.get(dsid)
    assert not indexed_dataset.is_archived

    # Archive again
    clirunner(['dataset', 'archive', '--all'])

    # and purge
    clirunner(['dataset', 'purge', '--all'])
    assert index.datasets.get(dsid) is None


def test_index_duplicate_dataset(index: Index,
                                 cfg_env,
                                 ls8_eo3_dataset) -> None:
    product = ls8_eo3_dataset.product
    dsid = ls8_eo3_dataset.id
    assert index.datasets.has(dsid)

    # Insert again.
    ds = Dataset(product, ls8_eo3_dataset.metadata_doc,
                 uris=ls8_eo3_dataset.uris)
    index.datasets.add(ds, with_lineage=False)

    assert index.datasets.has(dsid)


def test_has_dataset(index: Index, ls8_eo3_dataset: Dataset) -> None:
    assert index.datasets.has(ls8_eo3_dataset.id)
    assert index.datasets.has(str(ls8_eo3_dataset.id))

    assert not index.datasets.has(UUID('f226a278-e422-11e6-b501-185e0f80a5c0'))
    assert not index.datasets.has('f226a278-e422-11e6-b501-185e0f80a5c0')

    assert index.datasets.bulk_has([ls8_eo3_dataset.id, UUID('f226a278-e422-11e6-b501-185e0f80a5c0')]) == [True, False]
    assert index.datasets.bulk_has([str(ls8_eo3_dataset.id), 'f226a278-e422-11e6-b501-185e0f80a5c0']) == [True, False]


def test_get_dataset(index: Index, ls8_eo3_dataset: Dataset) -> None:
    assert index.datasets.has(ls8_eo3_dataset.id)
    assert index.datasets.has(str(ls8_eo3_dataset.id))

    assert index.datasets.bulk_has([ls8_eo3_dataset.id, 'f226a278-e422-11e6-b501-185e0f80a5c0']) == [True, False]

    for tr in (lambda x: x, lambda x: str(x)):
        ds = index.datasets.get(tr(ls8_eo3_dataset.id))
        assert ds.id == ls8_eo3_dataset.id

        ds, = index.datasets.bulk_get([tr(ls8_eo3_dataset.id)])
        assert ds.id == ls8_eo3_dataset.id

    assert index.datasets.bulk_get(['f226a278-e422-11e6-b501-185e0f80a5c0',
                                    'f226a278-e422-11e6-b501-185e0f80a5c1']) == []


def test_transactions_api_ctx_mgr(index,
                                  extended_eo3_metadata_type_doc,
                                  ls8_eo3_product,
                                  eo3_ls8_dataset_doc,
                                  eo3_ls8_dataset2_doc):
    from datacube.index.hl import Doc2Dataset
    resolver = Doc2Dataset(index, products=[ls8_eo3_product.name], verify_lineage=False)
    ds1, err = resolver(*eo3_ls8_dataset_doc)
    ds2, err = resolver(*eo3_ls8_dataset2_doc)
    with pytest.raises(Exception) as e:
        with index.transaction() as trans:
            assert index.datasets.get(ds1.id) is None
            index.datasets.add(ds1, with_lineage=False)
            assert index.datasets.get(ds1.id) is not None
            raise Exception("Rollback!")
    assert "Rollback!" in str(e.value)
    assert index.datasets.get(ds1.id) is None
    with index.transaction() as trans:
        assert index.datasets.get(ds1.id) is None
        index.datasets.add(ds1, with_lineage=False)
        assert index.datasets.get(ds1.id) is not None
    assert index.datasets.get(ds1.id) is not None
    with index.transaction() as trans:
        index.datasets.add(ds2, with_lineage=False)
        assert index.datasets.get(ds2.id) is not None
        raise trans.rollback_exception("Rollback")
    assert index.datasets.get(ds1.id) is not None
    assert index.datasets.get(ds2.id) is None


def test_transactions_api_ctx_mgr_nested(index,
                                         extended_eo3_metadata_type_doc,
                                         ls8_eo3_product,
                                         eo3_ls8_dataset_doc,
                                         eo3_ls8_dataset2_doc):
    from datacube.index.hl import Doc2Dataset
    resolver = Doc2Dataset(index, products=[ls8_eo3_product.name], verify_lineage=False)
    ds1, err = resolver(*eo3_ls8_dataset_doc)
    ds2, err = resolver(*eo3_ls8_dataset2_doc)
    with pytest.raises(Exception) as e:
        with index.transaction() as trans_outer:
            with index.transaction() as trans:
                assert index.datasets.get(ds1.id) is None
                index.datasets.add(ds1, False)
                assert index.datasets.get(ds1.id) is not None
                raise Exception("Rollback!")
    assert "Rollback!" in str(e.value)
    assert index.datasets.get(ds1.id) is None
    with index.transaction() as trans_outer:
        with index.transaction() as trans:
            assert index.datasets.get(ds1.id) is None
            index.datasets.add(ds1, False)
            assert index.datasets.get(ds1.id) is not None
    assert index.datasets.get(ds1.id) is not None
    with index.transaction() as trans_outer:
        with index.transaction() as trans:
            index.datasets.add(ds2, False)
            assert index.datasets.get(ds2.id) is not None
            raise trans.rollback_exception("Rollback")
    assert index.datasets.get(ds1.id) is not None
    assert index.datasets.get(ds2.id) is None


def test_transactions_api_manual(index,
                                 extended_eo3_metadata_type_doc,
                                 ls8_eo3_product,
                                 eo3_ls8_dataset_doc,
                                 eo3_ls8_dataset2_doc):
    from datacube.index.hl import Doc2Dataset
    resolver = Doc2Dataset(index, products=[ls8_eo3_product.name], verify_lineage=False)
    ds1, err = resolver(*eo3_ls8_dataset_doc)
    ds2, err = resolver(*eo3_ls8_dataset2_doc)
    trans = index.transaction()
    index.datasets.add(ds1, False)
    assert index.datasets.get(ds1.id) is not None
    trans.begin()
    index.datasets.add(ds2, False)
    assert index.datasets.get(ds1.id) is not None
    assert index.datasets.get(ds2.id) is not None
    trans.rollback()
    assert index.datasets.get(ds1.id) is not None
    assert index.datasets.get(ds2.id) is None
    trans.begin()
    index.datasets.add(ds2, False)
    trans.commit()
    assert index.datasets.get(ds1.id) is not None
    assert index.datasets.get(ds2.id) is not None


def test_transactions_api_hybrid(index,
                                 extended_eo3_metadata_type_doc,
                                 ls8_eo3_product,
                                 eo3_ls8_dataset_doc,
                                 eo3_ls8_dataset2_doc):
    from datacube.index.hl import Doc2Dataset
    resolver = Doc2Dataset(index, products=[ls8_eo3_product.name], verify_lineage=False)
    ds1, err = resolver(*eo3_ls8_dataset_doc)
    ds2, err = resolver(*eo3_ls8_dataset2_doc)
    with index.transaction() as trans:
        assert index.datasets.get(ds1.id) is None
        index.datasets.add(ds1, False)
        assert index.datasets.get(ds1.id) is not None
        trans.rollback()
        assert index.datasets.get(ds1.id) is None
        trans.begin()
        assert index.datasets.get(ds1.id) is None
        index.datasets.add(ds1, False)
        assert index.datasets.get(ds1.id) is not None
        trans.commit()
        assert index.datasets.get(ds1.id) is not None
        trans.begin()
        index.datasets.add(ds2, False)
        assert index.datasets.get(ds2.id) is not None
        trans.rollback()
    assert index.datasets.get(ds1.id) is not None
    assert index.datasets.get(ds2.id) is None


def test_get_missing_things(index: Index) -> None:
    """
    The get(id) methods should return None if the object doesn't exist.
    """
    uuid_ = UUID('18474b58-c8a6-11e6-a4b3-185e0f80a5c0')
    missing_thing = index.datasets.get(uuid_, include_sources=False)
    assert missing_thing is None, "get() should return none when it doesn't exist"

    if index.supports_lineage and not index.supports_external_lineage:
        missing_thing = index.datasets.get(uuid_, include_sources=True)
        assert missing_thing is None, "get() should return none when it doesn't exist"

    id_ = sys.maxsize
    missing_thing = index.metadata_types.get(id_)
    assert missing_thing is None, "get() should return none when it doesn't exist"

    missing_thing = index.products.get(id_)
    assert missing_thing is None, "get() should return none when it doesn't exist"


@pytest.mark.parametrize('datacube_env_name', ('datacube', ))
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


@pytest.mark.parametrize('datacube_env_name', ('datacube', ))
def test_index_dataset_with_location(index: Index, default_metadata_type: MetadataType):
    first_file = Path('/tmp/first/something.yaml').absolute()
    second_file = Path('/tmp/second/something.yaml').absolute()

    product = index.products.add_document(_pseudo_telemetry_dataset_type)
    dataset = Dataset(product, _telemetry_dataset, uris=[first_file.as_uri()], sources={})
    index.datasets.add(dataset)
    stored = index.datasets.get(dataset.id)

    assert stored.id == _telemetry_uuid
    # TODO: Dataset types?
    assert stored.product.id == product.id
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
    index.datasets.add(Dataset(product, second_ds_doc, uris=['file:///a', 'file:///b'], sources={}))

    # test order using get_locations function
    assert index.datasets.get_locations(second_ds_doc['id']) == ['file:///a', 'file:///b']

    # test order using datasets.get(), it has custom query as it turns out
    assert index.datasets.get(second_ds_doc['id']).uris == ['file:///a', 'file:///b']

    # test update, this should prepend file:///c, file:///d to the existing list
    index.datasets.update(Dataset(product, second_ds_doc, uris=['file:///a', 'file:///c', 'file:///d'], sources={}))
    assert index.datasets.get_locations(second_ds_doc['id']) == ['file:///c', 'file:///d', 'file:///a', 'file:///b']
    assert index.datasets.get(second_ds_doc['id']).uris == ['file:///c', 'file:///d', 'file:///a', 'file:///b']

    # Ability to get datasets for a location
    # Add a second dataset with a different location (to catch lack of joins, filtering etc)
    second_ds_doc = copy.deepcopy(_telemetry_dataset)
    second_ds_doc['id'] = '366f32d8-e1f8-11e6-94b4-185e0f80a5c0'
    index.datasets.add(Dataset(product, second_ds_doc, uris=[second_file.as_uri()], sources={}))
    for mode in ('exact', 'prefix', None):
        dataset_ids = [d.id for d in index.datasets.get_datasets_for_location(first_file.as_uri(), mode=mode)]
        assert dataset_ids == [dataset.id]

    assert list(index.datasets.get_datasets_for_location(first_file.as_uri() + "#part=100")) == []

    with pytest.raises(ValueError):
        list(index.datasets.get_datasets_for_location(first_file.as_uri(), mode="nosuchmode"))


def utc_now():
    # utcnow() doesn't include a tzinfo.
    return datetime.datetime.utcnow().replace(tzinfo=tz.tzutc())


def test_bulk_reads_transaction(index, extended_eo3_metadata_type_doc,
                                ls8_eo3_product,
                                eo3_ls8_dataset_doc,
                                eo3_ls8_dataset2_doc
                                ):
    with pytest.raises(ValueError) as e:
        with index.datasets._db_connection() as conn:
            conn.bulk_simple_dataset_search(batch_size=2)
    assert "within a transaction" in str(e.value)

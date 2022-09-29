# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2022 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
import pytest
from unittest.mock import MagicMock
from uuid import UUID

from datacube import Datacube


test_uuid = UUID('4ec8fe97-e8b9-11e4-87ff-1040f381a756')


def test_init_null(null_config):
    from datacube.drivers.indexes import index_cache
    idxs = index_cache()
    assert "default" in idxs._drivers
    assert "null" in idxs._drivers
    with Datacube(config=null_config, validate_connection=True) as dc:
        assert dc.index.url == "null"


def test_null_user_resource(null_config):
    with Datacube(config=null_config, validate_connection=True) as dc:
        assert dc.index.users.list_users() == []
        with pytest.raises(NotImplementedError) as e:
            dc.index.users.create_user("user1", "password2", "role1")
        with pytest.raises(NotImplementedError) as e:
            dc.index.users.delete_user("user1", "user2")
        with pytest.raises(NotImplementedError) as e:
            dc.index.users.grant_role("role1", "user1", "user2")


def test_null_metadata_types_resource(null_config):
    with Datacube(config=null_config, validate_connection=True) as dc:
        assert dc.index.metadata_types.get_all() == []
        with pytest.raises(NotImplementedError) as e:
            dc.index.metadata_types.from_doc({})
        with pytest.raises(NotImplementedError) as e:
            dc.index.metadata_types.add(MagicMock())
        with pytest.raises(NotImplementedError) as e:
            dc.index.metadata_types.can_update(MagicMock())
        with pytest.raises(NotImplementedError) as e:
            dc.index.metadata_types.update(MagicMock())
        with pytest.raises(NotImplementedError) as e:
            dc.index.metadata_types.update_document({})
        with pytest.raises(KeyError) as e:
            dc.index.metadata_types.get_unsafe(1)
        with pytest.raises(KeyError) as e:
            dc.index.metadata_types.get_by_name_unsafe("eo")
        with pytest.raises(NotImplementedError) as e:
            dc.index.metadata_types.check_field_indexes()


def test_null_product_resource(null_config):
    with Datacube(config=null_config, validate_connection=True) as dc:
        assert dc.index.products.get_all() == []
        assert dc.index.products.search_robust(foo="bar", baz=12) == []
        assert dc.index.products.get_with_fields(["foo", "bar"]) == []
        with pytest.raises(KeyError) as e:
            dc.index.products.get_unsafe(1)
        with pytest.raises(KeyError) as e:
            dc.index.products.get_by_name_unsafe("product1")
        with pytest.raises(NotImplementedError) as e:
            dc.index.products.add(MagicMock())
        with pytest.raises(NotImplementedError) as e:
            dc.index.products.can_update(MagicMock())
        with pytest.raises(NotImplementedError) as e:
            dc.index.products.update(MagicMock())


def test_null_dataset_resource(null_config):
    with Datacube(config=null_config, validate_connection=True) as dc:
        assert dc.index.datasets.get(test_uuid) is None
        assert dc.index.datasets.bulk_get([test_uuid, "foo"]) == []
        assert dc.index.datasets.get_derived(test_uuid) == []
        assert not dc.index.datasets.has(test_uuid)
        assert dc.index.datasets.bulk_has([test_uuid, "foo"]) == [False, False]
        with pytest.raises(NotImplementedError) as e:
            dc.index.datasets.add(MagicMock())
        with pytest.raises(NotImplementedError) as e:
            dc.index.datasets.can_update(MagicMock())
        with pytest.raises(NotImplementedError) as e:
            dc.index.datasets.update(MagicMock())
        with pytest.raises(NotImplementedError) as e:
            dc.index.datasets.archive([test_uuid, "foo"])
        with pytest.raises(NotImplementedError) as e:
            dc.index.datasets.restore([test_uuid, "foo"])
        with pytest.raises(NotImplementedError) as e:
            dc.index.datasets.purge([test_uuid, "foo"])

        assert dc.index.datasets.get_all_dataset_ids(True) == []
        assert dc.index.datasets.get_field_names() == []
        assert dc.index.datasets.get_locations(test_uuid) == []
        assert dc.index.datasets.get_archived_locations(test_uuid) == []
        assert dc.index.datasets.get_archived_location_times(test_uuid) == []
        assert dc.index.datasets.get_datasets_for_location("http://a.uri/test") == []

        with pytest.raises(NotImplementedError) as e:
            dc.index.datasets.add_location(test_uuid, "http://a.uri/test")
        with pytest.raises(NotImplementedError) as e:
            dc.index.datasets.remove_location(test_uuid, "http://a.uri/test")
        with pytest.raises(NotImplementedError) as e:
            dc.index.datasets.archive_location(test_uuid, "http://a.uri/test")
        with pytest.raises(NotImplementedError) as e:
            dc.index.datasets.restore_location(test_uuid, "http://a.uri/test")
        with pytest.raises(NotImplementedError) as e:
            dc.index.datasets.get_product_time_bounds("product1")

        assert dc.index.datasets.search_product_duplicates(MagicMock()) == []
        assert dc.index.datasets.search_by_metadata({}) == []
        assert dc.index.datasets.search(foo="bar", baz=12) == []
        assert dc.index.datasets.search_by_product(foo="bar", baz=12) == []
        assert dc.index.datasets.search_returning(["foo", "bar"], foo="bar", baz=12) == []
        assert dc.index.datasets.count(foo="bar", baz=12) == 0
        assert dc.index.datasets.count_by_product(foo="bar", baz=12) == []
        assert dc.index.datasets.count_by_product_through_time("1 month", foo="bar", baz=12) == []
        assert dc.index.datasets.count_product_through_time("1 month", foo="bar", baz=12) == []
        assert dc.index.datasets.search_summaries(foo="bar", baz=12) == []
        assert dc.index.datasets.search_eager(foo="bar", baz=12) == []
        assert dc.index.datasets.search_returning_datasets_light(("foo", "baz"), foo="bar", baz=12) == []


def test_null_transactions(null_config):
    with Datacube(config=null_config, validate_connection=True) as dc:
        trans = dc.index.transaction()
        assert not trans.active
        trans.begin()
        assert trans.active
        trans.commit()
        assert not trans.active
        trans.begin()
        assert dc.index.thread_transaction() == trans
        with pytest.raises(ValueError):
            trans.begin()
        trans.rollback()
        assert not trans.active
        assert dc.index.thread_transaction() is None

# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2022 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
import pytest
from datacube import Datacube


def test_init_memory(in_memory_config):
    from datacube.drivers.indexes import index_cache
    idxs = index_cache()
    assert "default" in idxs._drivers
    assert "memory" in idxs._drivers
    with Datacube(config=in_memory_config, validate_connection=True) as dc:
        assert(dc.index.url) == "memory"


def test_mem_user_resource(in_memory_config):
    with Datacube(config=in_memory_config, validate_connection=True) as dc:
        # Test default user
        assert dc.index.users.list_users() == [("local_user", "localuser", "Default user")]
        # Test create_user success
        dc.index.users.create_user("test_user_1", "password123", "odc_user", "Test1")
        dc.index.users.create_user("test_user_2", "password123", "agdc_user", "Test2")
        users = dc.index.users.list_users()
        assert ("local_user", "localuser", "Default user") in users
        assert ("odc_user", "test_user_1", "Test1") in users
        assert ("agdc_user", "test_user_2", "Test2") in users
        # Test create_user errors
        with pytest.raises(ValueError) as e:
            dc.index.users.create_user("test_user_2", "password123", "agdc_user", "Test2")
        assert "User test_user_2 already exists" in str(e.value)
        with pytest.raises(ValueError) as e:
            dc.index.users.create_user("test_user_3", "password123", "omg_user", "Test3")
        assert "omg_user is not a known role" in str(e.value)
        # Test grant_role success
        dc.index.users.grant_role("agdc_admin", "test_user_1", "test_user_2")
        # Roles can be granted multiple times.
        dc.index.users.grant_role("agdc_admin", "test_user_1", "test_user_2")
        # Test grant_role errors
        with pytest.raises(ValueError) as e:
            dc.index.users.grant_role("omg_admin", "test_user_1", "test_user_2")
        assert "omg_admin is not a known role" in str(e.value)
        with pytest.raises(ValueError) as e:
            dc.index.users.grant_role("odc_admin", "test_user_1", "test_user_3")
        assert "test_user_3 is not a known username" in str(e.value)
        # Test delete_user errors
        with pytest.raises(ValueError) as e:
            dc.index.users.delete_user("test_user_1", "test_user_2", "test_user_3")
        assert "test_user_3 is not a known username" in str(e.value)
        # Confirm one error means no users deleted
        users = dc.index.users.list_users()
        assert ("odc_user", "test_user_1", "Test1") in users
        # Confirm delete error success
        dc.index.users.delete_user("test_user_1", "test_user_2")
        assert dc.index.users.list_users() == [("local_user", "localuser", "Default user")]


def test_mem_metadatatype_resource(in_memory_config):
    with Datacube(config=in_memory_config, validate_connection=True) as dc:
        assert len(dc.index.metadata_types.by_id) == len(dc.index.metadata_types.by_name)
        assert len(list(dc.index.metadata_types.get_all())) == 3
        mdt = dc.index.metadata_types.get(1)
        assert mdt is not None and mdt.id == 1
        eo3 = dc.index.metadata_types.get_by_name("eo3")
        assert eo3 is not None and eo3.name == "eo3"
        # Verify we cannot mess with the cache
        eo3.definition["description"] = "foo"
        eo3.definition["dataset"]["measurements"] = ["over_here", "measurements"]
        eo3_fresh = dc.index.metadata_types.get_by_name("eo3")
        assert eo3.description != eo3_fresh.description
        assert eo3.definition["dataset"]["measurements"] != eo3_fresh.definition["dataset"]["measurements"]
        # Changing measurements definition is not safe
        with pytest.raises(ValueError) as e:
            dc.index.metadata_types.update(eo3)
        # Changing descriptions is safe.
        eo3_fresh.definition["description"] = "New description"
        dc.index.metadata_types.update(eo3_fresh)
        eo3_fresher = dc.index.metadata_types.get_by_name("eo3")
        assert eo3_fresher.description == eo3_fresh.description

# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2024 ODC Contributors
# SPDX-License-Identifier: Apache-2.0


def test_spindex_table_name():
    from datacube.drivers.postgis._spatial import is_spindex_table_name
    assert is_spindex_table_name("spatial_4326")
    assert not is_spindex_table_name("spatial")
    assert not is_spindex_table_name("spatial_")
    assert not is_spindex_table_name("spatial_0")
    assert not is_spindex_table_name("spam_spam")
    assert not is_spindex_table_name("spatial_spam")
    assert not is_spindex_table_name("spatial_-4326")
    assert not is_spindex_table_name("spatial_4326_spam")
    assert not is_spindex_table_name("spatial_spam_4326")

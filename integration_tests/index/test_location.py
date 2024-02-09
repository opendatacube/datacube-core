# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2024 ODC Contributors
# SPDX-License-Identifier: Apache-2.0

import pytest

from datacube.model import Dataset


def test_legacy_location_behaviour(index,ls8_eo3_dataset):
    locations = index.datasets.get_locations(ls8_eo3_dataset.id)  # Test of deprecated method
    assert locations == [ls8_eo3_dataset.uri]

    update = Dataset(  # Test of deprecated behaviour
        ls8_eo3_dataset.product,
        ls8_eo3_dataset.metadata_doc,
        uris=locations + ["file:/tmp/foo"])
    index.datasets.update(update)
    locations = index.datasets.get_locations(ls8_eo3_dataset.id)  # Test of deprecated method
    assert locations == ["file:/tmp/foo", ls8_eo3_dataset.uri]

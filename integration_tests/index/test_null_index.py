# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2022 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
import pytest

from datacube import Datacube
from datacube.config import LocalConfig


def test_init_null(null_config):
    from datacube.drivers.indexes import index_cache
    idxs = index_cache()
    assert "default" in idxs._drivers
    assert "null" in idxs._drivers
    with Datacube(config=null_config, validate_connection=True) as dc:
        assert(dc.index.url) == "null"

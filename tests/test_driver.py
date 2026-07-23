# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2026 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
from types import SimpleNamespace

import pytest

from datacube.drivers import (
    index_drivers,
    new_datasource,
    reader_drivers,
    writer_drivers,
)
from datacube.drivers.indexes import IndexDriverCache
from datacube.storage import BandInfo
from datacube.storage._rio import RasterDatasetDataSource
from datacube.testutils import mk_sample_dataset


def test_new_datasource_fallback() -> None:
    bands = [{"name": "green", "path": ""}]
    dataset = mk_sample_dataset(bands, "file:///foo", format="GeoTiff")

    assert dataset.uri_scheme == "file"

    rdr = new_datasource(BandInfo(dataset, "green"))
    assert rdr is not None
    assert isinstance(rdr, RasterDatasetDataSource)

    # check that None format works
    band = BandInfo(mk_sample_dataset(bands, "file:///file", format=None), "green")
    rdr = new_datasource(band)
    assert rdr is not None
    assert isinstance(rdr, RasterDatasetDataSource)


def test_reader_drivers() -> None:
    available_drivers = reader_drivers()
    assert isinstance(available_drivers, list)


def test_writer_drivers() -> None:
    available_drivers = writer_drivers()
    assert "netcdf" in available_drivers
    assert "NetCDF CF" in available_drivers


def test_index_drivers() -> None:
    available_drivers = index_drivers()
    assert "default" in available_drivers
    assert "null" in available_drivers
    assert "memory" in available_drivers


def test_default_injection() -> None:
    cache = IndexDriverCache("datacube.plugins.index-no-such-prefix")
    assert set(cache.drivers()) == {
        "default",
        "postgres",
        "legacy",
        "postgis",
        "memory",
    }


def test_writer_driver_mk_uri() -> None:
    from datacube.drivers.netcdf.driver import NetcdfWriterDriver

    writer_driver = NetcdfWriterDriver()

    assert writer_driver.uri_scheme == "file"

    file_path = "/path/to/my_file.nc"
    file_uri = writer_driver.mk_uri(file_path=file_path)
    assert file_uri == f"file://{file_path}"


def test_reader_cache_throws_on_missing_fallback() -> None:
    from datacube.drivers.readers import rdr_cache

    rdrs = rdr_cache()
    assert rdrs is not None

    with pytest.raises(KeyError):
        rdrs("file", "no-such-format")


def test_driver_singleton() -> None:
    from unittest.mock import MagicMock

    from datacube.drivers._tools import singleton_setup

    result = object()
    factory = MagicMock(return_value=result)
    obj = SimpleNamespace()

    assert singleton_setup(obj, "xx", factory) is result
    assert singleton_setup(obj, "xx", factory) is result
    assert singleton_setup(obj, "xx", factory) is result
    assert obj.xx is result

    factory.assert_called_once_with()

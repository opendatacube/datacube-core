# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2025 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
from collections.abc import Callable

from datacube.storage._base import BandInfo

from ._tools import singleton_setup
from .datasource import DataSource
from .driver_cache import load_drivers

DatasourceFactory = Callable[[BandInfo], DataSource]  # pylint: disable=invalid-name


class ReaderDriverCache:
    def __init__(self, group: str) -> None:
        self._drivers = load_drivers(group)

        lookup = {}
        for driver in self._drivers.values():
            for uri_scheme in driver.protocols:
                for fmt in driver.formats:
                    if driver.supports(uri_scheme, fmt):
                        key = (uri_scheme.lower(), fmt.lower())
                        lookup[key] = driver

        self._lookup = lookup

    def _find_driver(self, uri_scheme: str, fmt: str):
        key = (uri_scheme.lower(), fmt.lower())
        return self._lookup.get(key)

    def __call__(
        self, uri_scheme: str, fmt: str, fallback: DatasourceFactory | None = None
    ) -> DatasourceFactory:
        """
        Lookup `new_datasource` constructor method from the driver. Returns
        `fallback` method if no driver is found.

        :param uri_scheme: Protocol part of the Dataset uri
        :param fmt: Dataset format
        :return: Returns function `(DataSet, band_name:str) => DataSource`
        """
        driver = self._find_driver(uri_scheme, fmt)
        if driver is not None:
            return driver.new_datasource
        if fallback is not None:
            return fallback
        else:
            raise KeyError("No driver found and no fallback provided")

    def drivers(self) -> list[str]:
        """
        Returns list of driver names
        """
        return list(self._drivers.keys())


def rdr_cache() -> ReaderDriverCache:
    """
    Singleton for ReaderDriverCache
    """
    return singleton_setup(
        rdr_cache, "_instance", ReaderDriverCache, "datacube.plugins.io.read"
    )


def reader_drivers() -> list[str]:
    """
    Returns list driver names
    """
    return rdr_cache().drivers()


def choose_datasource(band: BandInfo) -> DatasourceFactory:
    """
    Returns appropriate `DataSource` class (or a constructor method) for loading
    given `dataset`.

    An appropriate `DataSource` implementation is chosen based on:

    - Dataset URI (protocol part)
    - Dataset format
    - Current system settings
    - Available IO plugins

    NOTE: we assume that all bands can be loaded with the same implementation.
    """
    from datacube.storage._rio import RasterDatasetDataSource

    return rdr_cache()(band.uri_scheme, band.format, fallback=RasterDatasetDataSource)


def new_datasource(band: BandInfo) -> DataSource | None:
    """
    Returns a newly constructed data source to read dataset band data.

    An appropriate `DataSource` implementation is chosen based on:

    - Dataset URI (protocol part)
    - Dataset format
    - Current system settings
    - Available IO plugins

    This function will return the default :class:`RasterDatasetDataSource` if no more specific
    ``DataSource`` can be found.

    :param band: The band to choose data source from.
    """
    source_type = choose_datasource(band)

    if source_type is None:
        return None

    return source_type(band)

# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2025 ODC Contributors
# SPDX-License-Identifier: Apache-2.0

from ._tools import singleton_setup
from .driver_cache import load_drivers


class WriterDriverCache:
    def __init__(self, group: str) -> None:
        self._drivers = load_drivers(group)

        for driver in list(self._drivers.values()):
            if hasattr(driver, "aliases"):
                for alias in driver.aliases:
                    self._drivers[alias] = driver

    def __call__(self, name: str) -> dict | None:
        """
        :returns: None if driver with a given name is not found

        :param name: Driver name
        :return: Returns WriterDriver
        """
        return self._drivers.get(name, None)

    def drivers(self) -> list[str]:
        """
        Returns list of driver names
        """
        return list(self._drivers.keys())


def writer_cache() -> WriterDriverCache:
    """
    Singleton for WriterDriverCache
    """
    return singleton_setup(
        writer_cache, "_instance", WriterDriverCache, "datacube.plugins.io.write"
    )


def writer_drivers() -> list[str]:
    """
    Returns list driver names
    """
    return writer_cache().drivers()


def storage_writer_by_name(name: str):
    """
    Lookup writer driver by name

    :return: Initialised writer driver instance or None if driver with this name doesn't exist
    """
    return writer_cache()(name)

# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2025 ODC Contributors
# SPDX-License-Identifier: Apache-2.0

from ..index.abstract import AbstractIndexDriver
from ._tools import singleton_setup
from .driver_cache import load_drivers


class IndexDriverCache:
    def __init__(self, group: str) -> None:
        self._drivers = load_drivers(group)

        if len(self._drivers) == 0:
            # Driver load has failed.  Something is very wrong, but try to manually load the main drivers anyway.
            from datacube.index.memory.index import (
                index_driver_init as mem_index_driver_init,
            )
            from datacube.index.postgis.index import (
                index_driver_init as pgis_index_driver_init,
            )
            from datacube.index.postgres.index import (
                index_driver_init as pg_index_driver_init,
            )

            self._drivers = {
                "postgres": pg_index_driver_init(),
                "postgis": pgis_index_driver_init(),
                "memory": mem_index_driver_init(),
            }

        for driver in list(self._drivers.values()):
            if hasattr(driver, "aliases"):
                for alias in driver.aliases:
                    self._drivers[alias] = driver

    def __call__(self, name: str) -> AbstractIndexDriver:
        """
        :returns: None if driver with a given name is not found

        :param name: Driver name
        :return: Returns IndexDriver
        """
        return self._drivers.get(name, None)

    def drivers(self) -> list[str]:
        """
        Returns list of driver names
        """
        return list(self._drivers.keys())


def index_cache() -> IndexDriverCache:
    """
    Singleton for IndexDriverCache
    """
    return singleton_setup(
        index_cache, "_instance", IndexDriverCache, "datacube.plugins.index"
    )


def index_drivers() -> set[str]:
    """
    Returns a set of driver names
    """
    return set(index_cache().drivers())


def index_driver_by_name(name: str) -> AbstractIndexDriver | None:
    """
    Lookup index driver by name

    :returns: Initialised index driver instance
    :returns: None if driver with this name doesn't exist
    """
    return index_cache()(name)

from typing import List

from ._tools import singleton_setup
from .driver_cache import load_drivers


class WriterDriverCache(object):
    def __init__(self, group: str):
        self._drivers = load_drivers(group)

        for driver in list(self._drivers.values()):
            if hasattr(driver, 'aliases'):
                for alias in driver.aliases:
                    self._drivers[alias] = driver

    def __call__(self, name: str):
        """
        :returns: None if driver with a given name is not found

        :param name: Driver name
        :return: Returns WriterDriver
        """
        return self._drivers.get(name, None)

    def drivers(self) -> List[str]:
        """ Returns list of driver names
        """
        return list(self._drivers.keys())


def writer_cache() -> WriterDriverCache:
    """ Singleton for WriterDriverCache
    """
    return singleton_setup(writer_cache, '_instance',
                           WriterDriverCache,
                           'datacube.plugins.io.write')


def writer_drivers() -> List[str]:
    """ Returns list driver names
    """
    return writer_cache().drivers()


def storage_writer_by_name(name):
    """ Lookup writer driver by name

    :returns: Initialised writer driver instance
    :returns: None if driver with this name doesn't exist
    """
    return writer_cache()(name)

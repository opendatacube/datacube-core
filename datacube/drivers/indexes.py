from __future__ import absolute_import

from .driver_cache import load_drivers


class IndexDriverCache(object):
    def __init__(self, group):
        self._drivers = load_drivers(group)

        if len(self._drivers) == 0:
            from datacube.index.index import index_driver_init
            self._drivers = dict(default=index_driver_init())

        for driver in list(self._drivers.values()):
            if hasattr(driver, 'aliases'):
                for alias in driver.aliases:
                    self._drivers[alias] = driver

    def __call__(self, name):
        """
        :returns: None if driver with a given name is not found

        :param str name: Driver name
        :return: Returns IndexDriver
        """
        return self._drivers.get(name, None)

    def drivers(self):
        """ Returns list of driver names
        """
        return list(self._drivers.keys())


def index_cache():
    """ Singleton for IndexDriverCache
    """
    # pylint: disable=protected-access
    if not hasattr(index_cache, '_instance'):
        index_cache._instance = IndexDriverCache('datacube.plugins.index')
    return index_cache._instance


def index_drivers():
    """ Returns list driver names
    """
    return index_cache().drivers()


def index_driver_by_name(name):
    """ Lookup writer driver by name

    :returns: Initialised writer driver instance
    :returns: None if driver with this name doesn't exist
    """
    return index_cache()(name)

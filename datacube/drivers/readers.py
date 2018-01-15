from __future__ import absolute_import

from .driver_cache import load_drivers


class ReaderDriverCache(object):
    def __init__(self, group):
        self._drivers = load_drivers(group)

        lookup = {}
        for driver in self._drivers.values():
            for uri_scheme in driver.protocols:
                for fmt in driver.formats:
                    if driver.supports(uri_scheme, fmt):
                        key = (uri_scheme.lower(), fmt.lower())
                        lookup[key] = driver

        self._lookup = lookup

    def _find_driver(self, uri_scheme, fmt):
        key = (uri_scheme.lower(), fmt.lower())
        return self._lookup.get(key)

    def __call__(self, uri_scheme, fmt, fallback=None):
        """Lookup `new_datasource` constructor method from the driver. Returns
        `fallback` method if no driver is found.

        :param str uri_scheme: Protocol part of the Dataset uri
        :param str fmt: Dataset format
        :return: Returns function `(DataSet, band_name:str) => DataSource`
        """
        driver = self._find_driver(uri_scheme, fmt)
        return fallback if driver is None else driver.new_datasource

    def drivers(self):
        """ Returns list of driver names
        """
        return list(self._drivers.keys())


def rdr_cache():
    """ Singleton for ReaderDriverCache
    """
    # pylint: disable=protected-access
    if not hasattr(rdr_cache, '_instance'):
        rdr_cache._instance = ReaderDriverCache('datacube.plugins.io.read')
    return rdr_cache._instance


def reader_drivers():
    """ Returns list driver names
    """
    return rdr_cache().drivers()


def choose_datasource(dataset):
    """Returns appropriate `DataSource` class (or a constructor method) for loading
    given `dataset`.

    An appropriate `DataSource` implementation is chosen based on:

    - Dataset URI (protocol part)
    - Dataset format
    - Current system settings
    - Available IO plugins

    NOTE: we assume that all bands can be loaded with the same implementation.

    """
    from ..storage.storage import RasterDatasetDataSource
    return rdr_cache()(dataset.uri_scheme, dataset.format, fallback=RasterDatasetDataSource)


def new_datasource(dataset, band_name=None):
    """Returns a newly constructed data source to read dataset band data.

    An appropriate `DataSource` implementation is chosen based on:

    - Dataset URI (protocol part)
    - Dataset format
    - Current system settings
    - Available IO plugins

    This function will return the default :class:`RasterDatasetDataSource` if no more specific
    ``DataSource`` can be found.

    :param dataset: The dataset to read.
    :param str band_name: the name of the band to read.

    """

    source_type = choose_datasource(dataset)

    if source_type is None:
        return None

    return source_type(dataset, band_name)

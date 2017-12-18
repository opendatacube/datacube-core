"""This module implements a simple plugin manager for storage drivers.

Drivers are automatically loaded provided they:
  - Store all code in a direct subdirectory, e.g. `s3/`
  - Include a `DRIVER_SPEC` attribute in the `__init__.py`. This
    attribute must be a tuple indicating `(<name>, <class_name>,
    <filepath>)` where the `<name>` is the driver's name as specified
    by the end user, e.g. `NetCDF CF` or `s3`; `<class_name>` is the
    python class name for that driver, e.g. `S3Driver`; and
    `<filepath>` is the filepath to the python module containing that
    class, e.g. `./driver.py`. The reason for specifying this
    information is to optimise the search for the driver, without
    loading all modules in the subdirectory.
  - Extend the `driver.Driver` abstract class,
    e.g. `S3Driver(Driver)`.

`drivers.loader.drivers` returns a dictionary of drivers instances,
indexed by their `name` as defined in `DRIVER_SPEC`. These are
instantiated on the first call to that method and cached until the
loader object is deleted.

TODO: update docs post DriverManager
"""
from __future__ import absolute_import
from pkg_resources import iter_entry_points


class ReaderDriverCache(object):
    def __init__(self, group):
        drivers = [(ep.name, ep.resolve()) for ep in iter_entry_points(group=group, name=None)]

        self._drivers = dict((name, init()) for name, init in drivers)

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
        '''Lookup `new_datasource` constructor method from the driver. Returns
        `fallback` method if no driver is found.

        :param str uri_scheme: Protocol part of the Dataset uri
        :param str fmt: Dataset format
        :return: Returns function `(DataSet, band_name:str) => DataSource`
        '''
        driver = self._find_driver(uri_scheme, fmt)
        return fallback if driver is None else driver.new_datasource


def rdr_cache():
    """ Singleton for ReaderDriverCache
    """
    # pylint: disable=protected-access
    if not hasattr(rdr_cache, '_instance'):
        rdr_cache._instance = ReaderDriverCache('datacube.plugins.io.read')
    return rdr_cache._instance


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
    from ..storage.storage import RasterDatasetSource
    return rdr_cache()(dataset.uri_scheme, dataset.format, fallback=RasterDatasetSource)


def new_datasource(dataset, band_name=None):
    """Returns a newly constructed data source to read dataset band data.

    An appropriate `DataSource` implementation is chosen based on:

    - Dataset URI (protocol part)
    - Dataset format
    - Current system settings
    - Available IO plugins

    This function will return None if no `DataSource` can be found that
    supports that type of `dataset`.


    :param dataset: The dataset to read.
    :param str band_name: the name of the band to read.

    """

    source_type = choose_datasource(dataset)

    if source_type is None:
        return None

    return source_type(dataset, band_name)

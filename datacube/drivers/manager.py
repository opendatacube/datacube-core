'''Module used to dynamically load storage drivers.
'''
from __future__ import absolute_import

import logging
import sys
from pathlib import Path
from collections import Iterable
import atexit
import random

from .driver import Driver
from .index import Index

# Dynamic loading from filename varies across python versions
# Based on http://stackoverflow.com/a/67692
if sys.version_info >= (3, 5): # python 3.5+
    # pylint: disable=import-error
    from importlib.util import spec_from_file_location, module_from_spec
    def load_mod(name, filepath):
        spec = spec_from_file_location(name, filepath)
        mod = module_from_spec(spec)
        spec.loader.exec_module(mod)
        return mod
    # pylint: disable=invalid-name, redefined-variable-type
    load_module = load_mod
elif sys.version_info[0] == 3: # python 3.3, 3.4: untested
    # pylint: disable=import-error
    from importlib.machinery import SourceFileLoader
    # pylint: disable=invalid-name, redefined-variable-type
    load_module = SourceFileLoader
else: # python 2
    # pylint: disable=import-error
    import imp
    # pylint: disable=invalid-name, redefined-variable-type
    load_module = imp.load_source


class DriverManager(object):
    '''The manager loads and manages storage drivers added as plugins. Its
    `driver` attribute returns a dictionary of drivers instances,
    indexed by their `name` as defined in `__init__.py`'s
    `DRIVER_SPEC`. The manager is a singleton and only looks up
    drivers on its first initialisation, but instantiates them all
    before caching them.

    The manager also forwards calls to the abstract methods of the
    `drivers` class to each specific driver.
    '''

    DEFAULT_DRIVER = 'NetCDF CF'
    '''Default driver, assuming its code is present. Otherwise any other
    driver gets selected.
    '''

    DRIVER_SPEC = 'DRIVER_SPEC'
    '''Attribue name where driver information is stored in `__init__.py`.
    '''

    __index = None
    '''Generic index.'''

    __driver = None
    '''Current driver.'''

    __drivers = None
    '''Singleton list of all available drivers, indexed by name.'''


    def __init__(self, index=None, *index_args, **index_kargs):
        '''Initialise the singleton and/or reload drivers.

        The singleton gets re-instantiated each time a new default
        driver name is specified or an index is passed. Otherwise, the
        current instance gets returned, and contains a default driver,
        list of available drivers.

        Each driver get initialised during instantiation, including
        the initialisation of their index, using the `index_args` and
        `index_kargs` optional arguments. If index is specified, it is
        passed to the driver to be used internally in place of the
        original index, as a test feature.

        The reason for re-instantiating the instance when a default
        driver or index is specified, is because caching these leaves
        the system in an inconsistent state, e.g. when testing various
        drivers.

        TODO: Ideally, the driver manager should not be a singleton
        but an object being instantiated by the entry point to the
        system, e.g. :mod:`datacube.ui.click` or
        :mod:`datacube.api.core`. This implementation is intended to
        limit the changes to the existing codebase until plugin
        drivers are fully developped.

        :param str default_driver_name: The name of the default driver
          to be used by the manager if no driver is specified in the
          dataset.
        :param index: An index object behaving like
          :class:`datacube.index._api.Index` and used for testing
          purposes only. In the current implementation, only the
          `index._db` variable is used, and is passed to the index
          initialisation method, that should basically replace the
          existing DB connection with that variable.
        :param args: Optional positional arguments to be passed to the
          index on initialisation. Caution: In the current
          implementation all parameters get passed to all potential
          indexes.
        :param args: Optional keyword arguments to be passed to the
          index on initialisation. Caution: In the current
          implementation all parameters get passed to all potential
          indexes.
        '''
        self.logger = logging.getLogger(self.__class__.__name__)
        # Initialise the generic index
        # pylint: disable=protected-access
        self.set_index(index, *index_args, **index_kargs)
        self.reload_drivers(index, *index_args, **index_kargs)
        self.set_default_driver(DriverManager.DEFAULT_DRIVER)
        self.logger.info('Ready. %s', self.index)


    def close(self):
        if self.__drivers:
            for driver in self.__drivers.values():
                # pylint: disable=protected-access
                if driver.index._db != self.__index._db:
                    driver.index.close()
        if self.__index:
            self.__index.close()
        self.logger.info('Closed index connections')


    def set_index(self, index=None, *index_args, **index_kargs):
        if self.__index:
            self.__index.close()
        self.__index = Index(self, index, *index_args, **index_kargs)
        self.logger.debug('Generic index set to %s', self.__index)


    def reload_drivers(self, index=None, *index_args, **index_kargs):
        '''Load and initialise all available drivers.

        See :meth:`__init__` for a description of the parameters.
        '''
        if self.__drivers:
            for driver in self.__drivers.values():
                # pylint: disable=protected-access
                if driver.index._db != self.__index._db:
                    driver.index.close()
        self.__drivers = {}
        for init_path in Path(__file__).parent.glob('*/__init__.py'):
            init = load_module(str(init_path.parent.stem), str(init_path))
            if self.DRIVER_SPEC in init.__dict__:
                spec = init.__dict__[self.DRIVER_SPEC]
                # pylint: disable=old-division
                filepath = init_path.parent / spec[2]
                # pylint: disable=old-division
                module = load_module(spec[1], str(init_path.parent / spec[2]))
                driver_cls = getattr(module, spec[1])
                if issubclass(driver_cls, Driver):
                    driver = driver_cls(self, spec[0], index, *index_args, **index_kargs)
                    self.__drivers[driver.name] = driver
                else:
                    self.logger.warning('Driver plugin "%s" is not a subclass of the abstract Driver class.',
                                        driver_cls)
        if len(self.__drivers) < 1:
            raise RuntimeError('No plugin driver found, Datacube cannot operate.')
        self.logger.debug('Reloaded %d drivers.', len(self.__drivers))


    def set_default_driver(self, driver_name):
        '''Set the default driver.

        If driver_name is None, then the driver currently in use
        remains active, or the default driver for the class is used as
        last resort..

        :param str driver_name: The name of the driver to set as
          current driver.
        '''
        if not driver_name in self.__drivers:
            raise ValueError('Default driver "%s" is not available in %s' % (
                driver_name, ', '.join(self.__drivers.keys())))
        self.__driver = self.__drivers[driver_name]
        self.logger.debug('Using default driver: %s', driver_name)


    @property
    def driver(self):
        '''Current default driver.
        '''
        return self.__driver


    @property
    def index(self):
        '''Generic index.
        '''
        return self.__index


    @property
    def drivers(self):
        '''Dictionary of drivers available, indexed by their name.'''
        return self.__drivers


    def __str__(self):
        '''Human-readable list of available drivers as a string.
        '''
        return '%s(default driver: %s; available drivers: %s)' % (
            self.__class__.__name__, self.driver.name,
            ', '.join(self.__drivers.keys()))


    def write_dataset_to_storage(self, dataset, *args, **kargs):
        '''Store a dataset using the the current driver.

        See :meth:`datacube.drivers.driver.write_dataset_to_storage`
        '''
        return self.driver.write_dataset_to_storage(dataset, *args, **kargs)


    def index_datasets(self, datasets, sources_policy):
        '''Index several datasets using the current driver.

        :param datasets: The datasets to be indexed.
        :param str sources_policy: The sources policy.
        :return: The number of datasets indexed.
        '''
        return self.driver.index.add_datasets(datasets, sources_policy)


    def get_driver_by_scheme(self, uris):
        '''Returns the driver required to read a dataset.

        Assuming all the dataset `uris` use the same scheme, this
        method returns a driver able to handle that scheme. Caution:
        several drivers may be able to process a given scheme in the
        current design, hence the return value of this method may not
        be deterministic.

        :todo: Discuss unique scheme per driver for future
        implementations.

        :param list uris: List of dataset locations, which may or may
          not contain a scheme. If not, it will default to `file`.
        :return: A driver able to handle the first `uri` is used to
          determine the dataset scheme. If not available, the scheme
          defaults to `file`.
        '''
        scheme = 'file'
        # Use only the first uri (if there is one)
        if isinstance(uris, Iterable) and len(uris) > 0:
            parts = uris[0].split(':', 1)
            # If there is a scheme and body there must be 2 parts
            if len(parts) == 2:
                scheme = parts[0]
        for driver in self.__drivers.values():
            if scheme == driver.uri_scheme:
                return driver
        raise ValueError('No driver found for scheme "%s"' % scheme)


    def get_datasource(self, dataset, band_name=None):
        return self.get_driver_by_scheme(dataset.uris).get_datasource(dataset, band_name)


    def add_specifics(self, dataset):
        return self.get_driver_by_scheme(dataset.uris).index.add_specifics(dataset)

'''Module used to dynamically load storage drivers.
'''
from __future__ import absolute_import

import logging
import sys
from pathlib import Path

from .driver import Driver

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

    __instance = None
    '''Singleton instance of this manager.'''

    __driver = None
    '''Current driver.'''

    __drivers = None
    '''Singleton list of all available drivers, indexed by name.'''


    def __new__(cls, default_driver_name=None, index=None, *index_args, **index_kargs):
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
          :class:`datacube.index._api.Index`.
        :param args: Optional positional arguments to be passed to the
          index on initialisation. Caution: In the current
          implementation all parameters get passed to all potential
          indexes.
        :param args: Optional keyword arguments to be passed to the
          index on initialisation. Caution: In the current
          implementation all parameters get passed to all potential
          indexes.
        '''
        if cls.__instance is None or default_driver_name or index:
            instance = super(DriverManager, cls).__new__(cls)
            instance.logger = logging.getLogger(cls.__name__)
            # pylint: disable=protected-access
            instance._load_drivers(index, *index_args, **index_kargs)
            # pylint: disable=protected-access
            instance._set_default_driver(default_driver_name)
            cls.__instance = instance
        return cls.__instance


    def _load_drivers(self, index, *index_args, **index_kargs):
        '''Load and initialise all available drivers.

        See :meth:`__init__` for a description of the parameters.
        '''
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
                    driver = driver_cls(spec[0], index, *index_args, **index_kargs)
                    self.__drivers[driver.name] = driver
                else:
                    self.logger.warning('Driver plugin "%s" is not a subclass of the abstract Driver class.',
                                        driver_cls)
        if len(self.__drivers) < 1:
            raise RuntimeError('No plugin driver found, Datacube cannot operate.')


    def _set_default_driver(self, driver_name):
        '''Set the default driver.

        The default driver is used by the manager if no driver is
        specified in the dataset.

        :param str default_driver_name: The name of the default
          driver.
        '''
        if driver_name:
            if not driver_name in self.__drivers:
                raise ValueError('Default driver "%s" is not available in %s' % (
                    driver_name, ', '.join(self.__drivers.keys())))
        else:
            driver_name = self.DEFAULT_DRIVER
            if driver_name not in self.__drivers:
                driver_name = list(self.__drivers.values())[0].name
        self.__driver = self.__drivers[driver_name]
        self.logger.info('Reloaded drivers. Using default driver: %s', driver_name)


    @property
    def driver(self):
        '''Current default driver.
        '''
        return self.__driver


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


    def get_driver_by_scheme(self, dataset):
        '''Returns the driver required to read a dataset.

        Since a request into the index is required to pull the
        locations for the dataset, this method uses the default
        driver's index to identify the location and then finds the
        first available driver able to deal withe uri scheme.

        TODO: Several drivers may be able to process a given scheme in
        the current design. This should be discussed for future
        implementations.

        :param datacube.model.Dataset dataset: A dataset with at least
          one location expected to be registered in the index. The
          first location found is used to extract the uri scheme. If
          no location is found, the scheme defaults to `file`.
        '''
        locations = self.driver.index.datasets.get_locations(dataset.id)
        scheme = locations[0].split(':', 1)[0] if locations and locations[0] else 'file'
        for driver in self.__drivers.values():
            if scheme == driver.uri_scheme:
                return driver
        raise ValueError('No driver found for scheme "%s"' % scheme)


    def get_index_specifics(self, dataset, band_name=None):
        '''TODO(csiro) implement this method in each driver as required.

        :param dataset: The dataset to read.
        :param str band_name: The band name.

        '''
        specifics = {}
        driver = self.get_driver_by_scheme(dataset)
        # TODO(csiro) Extract driver specifics from self.get_driver_by_scheme(dataset)
        return specifics


    def get_datasource(self, dataset, band_name=None):
        return self.get_driver_by_scheme(dataset).get_datasource(dataset, band_name)

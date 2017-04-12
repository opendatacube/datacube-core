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

    DRIVER_SPEC = 'DRIVER_SPEC'
    '''Attribue name where driver information is stored in `__init__.py`.
    '''

    __instance = None
    '''Singleton instance of this manager.'''

    __driver = None
    '''Current driver.'''

    __drivers = None
    '''Singleton list of all available drivers, indexed by name.'''


    def __new__(cls, driver_name=None, local_config=None,
                application_name=None, validate_connection=True):
        '''(Re-)Instantiate the singleton by loading the driver and initialising
        its index.

        The singleton gets re-instantiated each time a new
        DriverManager is created and `driver_name` is specified.
        '''
        if driver_name or cls.__instance is None:
            cls.__instance = super(DriverManager, cls).__new__(cls)
            cls.__instance.logger = logging.getLogger(cls.__name__)
            # pylint: disable=protected-access
            cls.__instance.__load_driver(driver_name, local_config, application_name, validate_connection)
            cls.__instance.logger.info('Ready with driver: %s', cls.__instance.driver)
        return cls.__instance


    def __load_driver(self, driver_name, local_config, application_name, validate_connection):
        '''Lookup and instantiate driver.
        '''
        if not driver_name:
            raise ValueError('A storage driver must be specified to initialise the manager.')
        for init_path in Path(__file__).parent.glob('*/__init__.py'):
            init = load_module(str(init_path.parent.stem), str(init_path))
            if self.DRIVER_SPEC in init.__dict__ and \
               init.__dict__[self.DRIVER_SPEC][0] == driver_name:
                spec = init.__dict__[self.DRIVER_SPEC]
                # pylint: disable=old-division
                filepath = init_path.parent / spec[2]
                # pylint: disable=old-division
                module = load_module(spec[1], str(init_path.parent / spec[2]))
                driver_cls = getattr(module, spec[1])
                if not issubclass(driver_cls, Driver):
                    raise ValueError('Driver plugin `%s` is not a subclass of the abstract Driver class.',
                                     driver_cls)
                self.__driver = driver_cls(spec[0], local_config, application_name, validate_connection)


    @property
    def driver(self):
        '''Current driver.
        '''
        return self.__driver


    @property
    def drivers(self):
        '''Dictionary of drivers available, indexed by their name.

        This singleton list is populated on first call of this property.
        '''
        if self.__drivers is None:
            self.__drivers = []
            for init_path in Path(__file__).parent.glob('*/__init__.py'):
                init = load_module(str(init_path.parent.stem), str(init_path))
                if self.DRIVER_SPEC in init.__dict__:
                    spec = init.__dict__[self.DRIVER_SPEC]
                    # pylint: disable=old-division
                    filepath = init_path.parent / spec[2]
                    # pylint: disable=old-division
                    module = load_module(spec[1], str(init_path.parent / spec[2]))
                    driver_cls = getattr(module, spec[1])
                    if not issubclass(driver_cls, Driver):
                        self.logger.warning('Driver plugin `%s` is not a subclass of the abstract Driver class. ',
                                            driver_cls)
                        continue
                    self.__drivers.append(spec[0])
        return self.__drivers


    def __str__(self):
        '''Human-readable list of available drivers as a string.
        '''
        return '%s(driver: %s)' % (self.__class__.__name__,
                                   self.driver.name)


    def write_dataset_to_storage(self, dataset, *args, **kargs):
        '''Forwards a call to a specific driver.

        An additional first parameter must be specified:
        :param: str driver: The name of the driver to handle this
          call. If unknown, a `ValueError` exception will be raised.

        See :meth:`datacube.drivers.driver.write_dataset_to_storage`
        for the other parameters and return value.
        '''
        return self.driver.write_dataset_to_storage(dataset, *args, **kargs)


    def index_datasets(self, datasets, sources_policy):
        return self.driver.index.add_datasets(datasets, sources_policy)

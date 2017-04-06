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

    __drivers = {}
    '''Avaliable drivers, indexed by name.'''


    def __new__(cls):
        '''Instantiate the singleton and load all available drivers.
        '''
        if cls.__instance is None:
            cls.__instance = super(DriverManager, cls).__new__(cls)
            cls.__instance.logger = logging.getLogger(cls.__name__)
            # pylint: disable=protected-access
            cls.__instance.__load_drivers()
            cls.__instance.logger.debug('Loaded %d storage drivers: %s',
                                        len(cls.__instance.drivers),
                                        ', '.join(cls.__instance.drivers.keys()))
        return cls.__instance


    def __load_drivers(self):
        '''Lookup and instantiate all available drivers.
        '''
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
                    self.logger.warning('Driver plugin `%s` is not a subclass of the abstract Driver class. ' \
                                        +'It will be ignored!', driver_cls)
                    continue
                self.__drivers[spec[0]] = driver_cls()


    @property
    def drivers(self):
        '''Dictionary of drivers available, indexed by their name.
        '''
        return self.__drivers


    def __str__(self):
        '''Human-readable list of available drivers as a string.
        '''
        return '%s(%s)' % (self.__class__.__name__,
                           ', '.join(self.drivers.keys()))


    def write_dataset_to_storage(self, driver, dataset, *args, **kargs):
        '''Forwards a call to a specific driver.

        An additional first parameter must be specified:
        :param: str driver: The name of the driver to handle this
          call. If unknown, a `ValueError` exception will be raised.

        See :meth:`datacube.drivers.driver.write_dataset_to_storage`
        for the other parameters and return value.
        '''
        if not driver:
            raise ValueError('A driver must be specified to call its methods.')
        if driver not in self.drivers:
            raise ValueError('Unknown storage driver: %s' % driver)
        return self.drivers[driver].write_dataset_to_storage(dataset, *args, **kargs)

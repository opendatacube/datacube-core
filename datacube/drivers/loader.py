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




class DriverLoader(object):
    '''The loader returns a dictionary of drivers instances, indexed by
    their `name` as defined in `__init__.py`'s `DRIVER_SPEC`. These
    are instantiated on the first call to that method and cached until
    the loader object is deleted.
    '''

    DRIVER_SPEC = 'DRIVER_SPEC'
    '''Attribue name where driver information is stored in `__init__.py`.
    '''

    def __init__(self):
        '''Prepares to load all drivers in direct subdirectories.
        '''
        self.logger = logging.getLogger(self.__class__.__name__)
        self._path = Path(__file__)
        self._drivers = {}


    @property
    def drivers(self):
        '''Dictionary of drivers available, indexed by their name.
        '''
        if not self._drivers:
            for init_path in self._path.parent.glob('*/__init__.py'):
                init = load_module(str(init_path.parent.stem), str(init_path))
                if self.DRIVER_SPEC in init.__dict__:
                    spec = init.__dict__[self.DRIVER_SPEC]
                    # pylint: disable=old-division
                    filepath = init_path.parent / spec[2]
                    # pylint: disable=old-division
                    module = load_module(spec[1], str(init_path.parent / spec[2]))
                    driver = getattr(module, spec[1])
                    self._drivers[spec[0]] = driver()
            self.logger.info('Loaded %d storage drivers: %s',
                             len(self._drivers),
                             ', '.join(list(self.drivers.keys())))
        return self._drivers

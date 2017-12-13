"""Module used to dynamically load storage drivers.
"""
from __future__ import absolute_import, division

import logging
import weakref
from pathlib import Path
from collections import Iterable
from cloudpickle import loads, dumps

from ..compat import load_module
from .driver import Driver
from .index import Index


class DriverManager(object):
    """Storage drivers are added as plugins providing a storage driver and
    indexing mechanisms. The manager loads and initialise all
    available plugins when it starts and makes them available by
    name. It also includes a generic index allowing to load a dataset
    metadata from the index using only basic information about the
    dataset, in particular, without knowing what driver is required to
    retrieve the actual data.

    A 'current' driver can be set in the manager and which can be used
    to handle datasets which don't specify a storage driver in their
    metadata.
    """

    #: Default 'current' driver, assuming its code is present.
    _DEFAULT_DRIVER = 'NetCDF CF'

    #: Attribute name where driver information is stored in `__init__.py`.
    _DRIVER_SPEC = 'DRIVER_SPEC'

    def __init__(self, index=None, default_driver_name=None, *index_args, **index_kargs):
        """Initialise the manager.

        Each driver get initialised during instantiation, including
        the initialisation of their index, using the `index_args` and
        `index_kargs` optional arguments. If `index` is specified, it
        is passed to the driver for its DB connection to be used
        internally, as a test feature.

        :param index: An index object behaving like
          :class:`datacube.index._api.Index` and used for testing
          purposes only. In the current implementation, only the
          `index._db` variable is used, and is passed to the index
          initialisation method, that should basically replace the
          existing DB connection with that variable.
        :param args: Optional positional arguments to be passed to the
          index on initialisation. Caution: In the current
          implementation all parameters get passed to all available
          indexes.
        :param args: Optional keyword arguments to be passed to the
          index on initialisation. Caution: In the current
          implementation all parameters get passed to all available
          indexes.
        """

        self._orig = {'index': dumps(index), 'index_args': index_args, 'index_kargs': index_kargs}

        self.__index = None
        '''Generic index.'''

        self.__driver = None
        '''Current driver.'''

        self.__drivers = None
        '''List of all available drivers, indexed by name.'''

        self.logger = logging.getLogger(self.__class__.__name__)
        self.is_clone = False
        # Initialise the generic index
        # pylint: disable=protected-access
        self.set_index(index, *index_args, **index_kargs)
        self.reload_drivers(*index_args, **index_kargs)
        self.set_current_driver(default_driver_name or self._DEFAULT_DRIVER)
        self.logger.debug('Ready. %s', self)

    def __getstate__(self):
        self._orig['current_driver'] = self.driver.name
        return self._orig

    def __setstate__(self, state):
        self.__init__(index=loads(state['index']), *state['index_args'], **state['index_kargs'])
        self.set_current_driver(state['current_driver'])
        self.is_clone = True

    def __del__(self):
        try:
            self.close()
        # pylint: disable=bare-except
        except:
            if hasattr(self, 'logger'):
                self.logger.debug('Connections already closed')

    def close(self):
        """Close all drivers' index connections."""
        if self.__drivers:
            for driver in self.__drivers.values():
                # pylint: disable=protected-access
                if driver.index._db != self.__index._db:
                    driver.index.close()
        if self.__index:
            self.__index.close()

        if hasattr(self, 'logger'):
            self.logger.debug('Closed index connections')

    def set_index(self, index=None, *index_args, **index_kargs):
        """Initialise the generic index.

        :param index: An index object behaving like
          :class:`datacube.index._api.Index` and used for testing
          purposes only. In the current implementation, only the
          `index._db` variable is used, and is passed to the index
          initialisation method, that should basically replace the
          existing DB connection with that variable.
        :param args: Optional positional arguments to be passed to the
          index on initialisation. Caution: In the current
          implementation all parameters get passed to all available
          indexes.
        :param args: Optional keyword arguments to be passed to the
          index on initialisation. Caution: In the current
          implementation all parameters get passed to all available
          indexes.
        """
        if self.__index:
            self.__index.close()
        self.__index = Index(index, *index_args, **index_kargs)
        self.logger.debug('Generic index set to %s', self.__index)

    def reload_drivers(self, *index_args, **index_kargs):
        """Load and initialise all available drivers and their indexes.


        :param index: An index object behaving like
          :class:`datacube.index._api.Index` and used for testing
          purposes only. In the current implementation, only the
          `index._db` variable is used, and is passed to the index
          initialisation method, that should basically replace the
          existing DB connection with that variable.
        :param args: Optional positional arguments to be passed to the
          index on initialisation. Caution: In the current
          implementation all parameters get passed to all available
          indexes.
        :param args: Optional keyword arguments to be passed to the
          index on initialisation. Caution: In the current
          implementation all parameters get passed to all available
          indexes.
        """
        if self.__drivers:
            for driver in self.__drivers.values():
                # pylint: disable=protected-access
                if driver.index._db != self.__index._db:
                    driver.index.close()
        self.__drivers = {}
        for init_path in Path(__file__).parent.glob('*/__init__.py'):
            init = load_module(str(init_path.parent.stem), str(init_path))
            if self._DRIVER_SPEC in init.__dict__:
                spec = init.__dict__[self._DRIVER_SPEC]
                filepath = init_path.parent / spec[2]
                try:
                    driver_module = load_module(spec[1], str(filepath))
                except ImportError:
                    self.logger.info('Import Failed for Driver plugin "%s", skipping.', spec[1])
                    continue
                driver_cls = getattr(driver_module, spec[1])
                if issubclass(driver_cls, Driver):
                    driver = driver_cls(weakref.ref(self)(), spec[0], self.__index, *index_args, **index_kargs)

                    validate_connection = index_kargs['validate_connection'] \
                        if 'validate_connection' in index_kargs else True
                    if validate_connection and not driver.requirements_satisfied():
                        self.logger.info('Driver plugin "%s" failed requirements check, skipping.', spec[1])
                        continue

                    self.__drivers[driver.name] = driver
                else:
                    self.logger.info('Driver plugin "%s" is not a subclass of the abstract Driver class.',
                                     driver_cls)
        if len(self.__drivers) < 1:
            raise RuntimeError('No plugin driver found, Datacube cannot operate.')
        self.logger.debug('Reloaded %d drivers.', len(self.__drivers))

    def set_current_driver(self, driver_name):
        """Set the current driver.

        If driver_name is None, then the driver currently in use
        remains active, or a default driver is used as last resort.

        :param str driver_name: The name of the driver to set as
          current driver.
        """
        if driver_name not in self.__drivers:
            raise ValueError('Default driver "%s" is not available in %s' % (
                driver_name, ', '.join(self.__drivers.keys())))
        self.__driver = self.__drivers[driver_name]
        self.logger.debug('Using default driver: %s', driver_name)

    @property
    def driver(self):
        """Current driver.
        """
        return self.__driver

    @property
    def index(self):
        """Generic index.

        :rtype: Index
        """
        return self.__index

    @property
    def drivers(self):
        """Dictionary of drivers available, indexed by their name."""
        return self.__drivers

    def __str__(self):
        """Information about the available drivers.
        """
        return 'DriverManager(current driver: %s; available drivers: %s)' % (
            self.driver.name, ', '.join(self.__drivers.keys()))

    def write_dataset_to_storage(self, dataset, *args, **kargs):
        """Store a dataset using the the current driver.

        See :meth:`datacube.drivers.driver.write_dataset_to_storage`
        """
        return self.driver.write_dataset_to_storage(dataset, *args, **kargs)

    def index_datasets(self, datasets, sources_policy):
        """Index several datasets using the current driver.

        :param datasets: The datasets to be indexed.
        :param str sources_policy: The sources policy.
        :return: The number of datasets indexed.
        """
        return self.driver.index.add_datasets(datasets, sources_policy)

    def get_driver_by_scheme(self, uris):
        """Returns the driver required to read a dataset.

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
        """
        scheme = 'file'
        # Use only the first uri (if there is one)
        if isinstance(uris, Iterable) and uris:
            parts = uris[0].split(':', 1)
            # If there is a scheme and body there must be 2 parts
            if len(parts) == 2:
                scheme = parts[0]
        for driver in self.__drivers.values():
            if scheme == driver.uri_scheme:
                return driver
        raise ValueError('No driver found for scheme "%s"' % scheme)

    def get_datasource(self, dataset, band_name=None):
        """Returns a data source to read a dataset band data.

        The appropriate driver is determined from the dataset uris,
        then the datasource created using that driver.
        :param dataset: The dataset to read.
        :param band_name: the name of the band to read.

        """
        return self.get_driver_by_scheme(dataset.uris).get_datasource(dataset, band_name)

    def add_specifics(self, dataset):
        """Pulls driver-specific index data from the DB.

        This method should only be called by the generic index to pull
        driver-specific metadata from the index. The appropriate
        driver is determined from the dataset uris, then the
        specific data retrieved using that driver.

        :param dataset: The dataset for which to extract the specific
          data.
        """
        return self.get_driver_by_scheme(dataset.uris).index.add_specifics(dataset)

    def __enter__(self):
        return self

    def __exit__(self, type_, value, traceback):
        self.close()

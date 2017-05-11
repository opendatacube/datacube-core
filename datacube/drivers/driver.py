'''Module containing the abstract `Driver` class to be implemented by
all storage drivers. There is a 1:1 relationship between a driver and
storage mechanism.
'''
from __future__ import absolute_import

from abc import ABCMeta, abstractmethod
from six import add_metaclass

@add_metaclass(ABCMeta)
class Driver(object):
    '''Abstract base class for storage drivers.

    TODO(csiro): Add more methods to cater for indexing and data
    access.
    '''

    __name = None
    '''Driver's name.'''

    __index = None
    '''Driver's index.'''


    def __init__(self, name, index=None, *index_args, **index_kargs):
        '''Initialise the driver's name and index.

        This should be called by subclasses, or the name and index set manually.

        :param str name: The name this driver should be referred to as.
        :param index: An index object behaving like
          :class:`datacube.index._api.Index`. In the current
          implementation, only the `index._db` variable is used, and
          is passed to the index initialisation method, that should
          basically replace the existing DB connection with that
          variable.
        :param args: Optional positional arguments to be passed to the
          index on initialisation. Caution: In the current
          implementation all parameters get passed to all potential
          indexes.
        :param args: Optional keyword arguments to be passed to the
          index on initialisation. Caution: In the current
          implementation all parameters get passed to all potential
          indexes.
        '''
        self.__name = name
        # pylint: disable=protected-access
        self.__index = self._init_index(index._db if index else None, *index_args, **index_kargs)


    @property
    def name(self):
        '''A human-readable name for this driver.'''
        if not self.__name:
            raise ValueError('Driver was not initialised properly. Make sure you call the base class _init__')
        return self.__name


    @property
    def format(self):
        '''Output format for this driver for use in metadata.

        Defaults to driver name, but may need to be overriden by some drivers.'''
        return self.__name


    @property
    def uri_scheme(self):
        '''URI scheme used by this driver.'''
        return 'file'


    @property
    def index(self):
        '''This driver's index.'''
        if not self.__index:
            raise ValueError('Driver was not initialised properly. Make sure you call the base class _init__')
        return self.__index


    def as_uri(self, path):
        '''Set or replace the uri scheme for a path according to the driver's.
        '''
        path = str(path)
        body = path.split(':', 1)[1] if ':' in path else path
        return '%s:%s' % (self.uri_scheme, body)


    @abstractmethod
    def write_dataset_to_storage(self, dataset, *args, **kargs):
        '''Write a Data Cube style xarray Dataset to the storage.

        Requires a spatial Dataset, with attached coordinates and
        global crs attribute. This does not include the indexing step.

        :param `xarray.Dataset` dataset: The dataset
        :param list args: Storage-specific positional arguments
        :param list kargs: Storage-specific keyword arguments
        :return: Storage-specific write operation output, e.g. data
          relevant to the indexing
        '''
        return None


    @abstractmethod
    def _init_index(self, db=None, *index_args, **index_kargs):
        '''Initialise this driver's index.

        :param db: A DB connection that should be used by the
          index. This is provided for test support only, and not all
          drivers may support it in the future.
        :param args: Optional positional arguments to be passed to the
          index on initialisation. Caution: In the current
          implementation all parameters get passed to all potential
          indexes.
        :param args: Optional keyword arguments to be passed to the
          index on initialisation. Caution: In the current
          implementation all parameters get passed to all potential
          indexes.
        '''
        return None


    def get_index_specifics(self, dataset):
        return None

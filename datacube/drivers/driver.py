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


    def __init__(self, name, local_config=None, application_name=None, validate_connection=True):
        '''Initialise the driver's name and index.

        This should be called by subclasses, or the name and index set manually.
        '''
        self.__name = name
        self.__index = self._init_index(local_config, application_name, validate_connection)


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
    def index(self):
        '''This driver's index.'''
        if not self.__index:
            raise ValueError('Driver was not initialised properly. Make sure you call the base class _init__')
        return self.__index


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
    def _init_index(self, local_config=None, application_name=None, validate_connection=True):
        '''Initialise this driver's index.

        :param application_name: A short, alphanumeric name to identify this application.
        :param local_config: Config object to use.
        :type local_config: :py:class:`datacube.config.LocalConfig`, optional
        :param validate_connection: Validate database connection and schema immediately
        :raises datacube.index.postgres._api.EnvironmentError:
        '''
        return None

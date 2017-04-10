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

    @property
    @abstractmethod
    def name(self):
        '''A human-readable name for this driver.'''
        return 'Base driver'


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
    def index_connect(self, local_config=None, application_name=None, validate_connection=True):
        '''Connect to the index for this driver.

        :param application_name: A short, alphanumeric name to identify this application.
        :param local_config: Config object to use.
        :type local_config: :py:class:`datacube.config.LocalConfig`, optional
        :param validate_connection: Validate database connection and schema immediately
        :raises datacube.index.postgres._api.EnvironmentError:
        :rtype: Index
        '''
        return None

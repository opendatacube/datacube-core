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

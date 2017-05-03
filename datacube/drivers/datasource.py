'''Module containing the abstract `DatasetSource` class to be implemented by
all storage drivers.
'''
from __future__ import absolute_import

from abc import ABCMeta, abstractmethod
from six import add_metaclass

@add_metaclass(ABCMeta)
class DataSource(object):
    '''Abstract base class for dataset source.
    '''

    @abstractmethod
    def get_bandnumber(self, src):
        '''Return the band number for a dataset source.

        :param src: TODO(csiro) Do we need it for non NetCDF sources?
        :return: The band index number.
        '''
        return None


    @abstractmethod
    def open(self):
        return None


    @abstractmethod
    def get_transform(self, shape):
        return None


    @abstractmethod
    def get_crs(self):
        return None

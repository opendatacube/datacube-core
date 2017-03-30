'''Module containing the abstract `Driver` class to be implemented by
all storage drivers.
'''
from __future__ import absolute_import

from abc import ABCMeta, abstractmethod
from six import add_metaclass

@add_metaclass(ABCMeta)
class Driver(object):
    '''Abstract base class for storage drivers.
    '''

    @property
    @abstractmethod
    def name(self):
        '''A human-readable name for this driver.'''
        return 'Base driver'

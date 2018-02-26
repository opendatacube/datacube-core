"""Module containing the abstract `DatasetSource` class to be implemented by
all storage drivers.
"""
from __future__ import absolute_import

from abc import ABCMeta, abstractmethod
from six import add_metaclass
from contextlib import contextmanager


@add_metaclass(ABCMeta)
class DataSource(object):
    """Abstract base class for dataset source.
    """

    @abstractmethod
    @contextmanager
    def open(self):
        return None

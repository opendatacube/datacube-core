"""Module containing the abstract `DatasetSource` class to be implemented by
all storage drivers.
"""
from abc import ABCMeta, abstractmethod
from contextlib import contextmanager


class DataSource(object, metaclass=ABCMeta):
    """Abstract base class for dataset source.
    """

    @abstractmethod
    @contextmanager
    def open(self):
        return None

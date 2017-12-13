"""Module containing the abstract `Driver` class to be implemented by
all storage drivers. There is a 1:1 relationship between a driver and
storage mechanism.
"""
from __future__ import absolute_import

import re
from abc import ABCMeta, abstractmethod
from pathlib import Path

from six import add_metaclass


@add_metaclass(ABCMeta)
class Driver(object):
    """Abstract base class for storage drivers.

    TODO(csiro): Add more methods to cater for indexing and data
    access.
    """

    #: Driver's Name
    __name = None

    #: Driver's Index
    __index = None

    def __init__(self, driver_manager, name, index=None, *index_args, **index_kargs):
        """Initialise the driver's name and index.

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
        """
        self.__name = name
        self._driver_manager = driver_manager
        # pylint: disable=protected-access
        self.__index = self._init_index(driver_manager, index, *index_args, **index_kargs)

    @property
    def name(self):
        """A human-readable name for this driver."""
        if not self.__name:
            raise ValueError('Driver was not initialised properly. Make sure you call the base class _init__')
        return self.__name

    @property
    def format(self):
        """Output format for this driver for use in metadata.

        Defaults to driver name, but may need to be overriden by some drivers."""
        return self.__name

    @property
    def uri_scheme(self):
        """URI scheme used by this driver."""
        return 'file'

    @property
    def index(self):
        """This driver's index."""
        if not self.__index:
            raise ValueError('Driver was not initialised properly. Make sure you call the base class _init__')
        return self.__index

    def requirements_satisfied(self):
        """Check requirements are satisfied.

        :return: True if requirements is satisfied, otherwise returns False
        """
        return True

    def as_uri(self, path):
        """Set or replace the uri scheme for a path according to the driver's.
        """
        path = Path(path).as_uri()
        return re.sub("^file", self.uri_scheme, path)

    @abstractmethod
    def write_dataset_to_storage(self, dataset, *args, **kargs):
        """Write a Data Cube style xarray Dataset to the storage.

        Requires a spatial Dataset, with attached coordinates and
        global crs attribute. This does not include the indexing step.

        :param `xarray.Dataset` dataset: The dataset
        :param list args: Storage-specific positional arguments
        :param list kargs: Storage-specific keyword arguments
        :return: Storage-specific write operation output, e.g. data
          relevant to the indexing
        """
        return None

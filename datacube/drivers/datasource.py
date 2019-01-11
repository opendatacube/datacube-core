""" Defines abstract types for IO reader drivers.
"""
from abc import ABCMeta, abstractmethod
from contextlib import contextmanager
import numpy as np
from affine import Affine
from typing import Tuple, Iterator, Optional, Union


RasterShape = Tuple[int, int]                 # pylint: disable=invalid-name
RasterWindow = Union[                         # pylint: disable=invalid-name
    Tuple[Tuple[int, int], Tuple[int, int]],
    Tuple[slice, slice]]

# pylint: disable=pointless-statement


class GeoRasterReader(object, metaclass=ABCMeta):
    """ Abstract base class for dataset reader.
    """

    @property
    @abstractmethod
    def crs(self):
        ...

    @property
    @abstractmethod
    def transform(self) -> Optional[Affine]:
        ...

    @property
    @abstractmethod
    def dtype(self) -> Union[str, np.dtype]:
        ...

    @property
    @abstractmethod
    def shape(self) -> RasterShape:
        ...

    @property
    @abstractmethod
    def nodata(self) -> Optional[Union[int, float]]:
        ...

    @abstractmethod
    def read(self,
             window: Optional[RasterWindow] = None,
             out_shape: Optional[RasterShape] = None) -> Optional[np.ndarray]:
        ...


class DataSource(object, metaclass=ABCMeta):
    """ Abstract base class for dataset source.
    """

    @abstractmethod
    @contextmanager
    def open(self) -> Iterator[GeoRasterReader]:
        ...

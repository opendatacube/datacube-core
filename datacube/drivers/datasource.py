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
        ...  # pragma: no cover

    @property
    @abstractmethod
    def transform(self) -> Optional[Affine]:
        ...  # pragma: no cover

    @property
    @abstractmethod
    def dtype(self) -> Union[str, np.dtype]:
        ...  # pragma: no cover

    @property
    @abstractmethod
    def shape(self) -> RasterShape:
        ...  # pragma: no cover

    @property
    @abstractmethod
    def nodata(self) -> Optional[Union[int, float]]:
        ...  # pragma: no cover

    @abstractmethod
    def read(self,
             window: Optional[RasterWindow] = None,
             out_shape: Optional[RasterShape] = None) -> Optional[np.ndarray]:
        ...  # pragma: no cover


class DataSource(object, metaclass=ABCMeta):
    """ Abstract base class for dataset source.
    """

    @abstractmethod
    @contextmanager
    def open(self) -> Iterator[GeoRasterReader]:
        ...  # pragma: no cover

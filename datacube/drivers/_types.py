""" Defines abstract types for IO drivers.
"""
from typing import (
    List, Tuple, Optional, Union, Any, Iterable,
    TYPE_CHECKING
)

from abc import ABCMeta, abstractmethod
import numpy as np
from affine import Affine
from concurrent.futures import Future
from datacube.storage import BandInfo
from datacube.utils.geometry import CRS

# pylint: disable=invalid-name,unsubscriptable-object,pointless-statement

if TYPE_CHECKING:
    FutureGeoRasterReader = Future['GeoRasterReader']  # pragma: no cover
    FutureNdarray = Future[np.ndarray]                 # pragma: no cover
else:
    FutureGeoRasterReader = Future
    FutureNdarray = Future


RasterShape = Tuple[int, int]
RasterWindow = Tuple[slice, slice]


class GeoRasterReader(object, metaclass=ABCMeta):
    """ Abstract base class for dataset reader.
    """

    @property
    @abstractmethod
    def crs(self) -> Optional[CRS]:
        ...  # pragma: no cover

    @property
    @abstractmethod
    def transform(self) -> Optional[Affine]:
        ...  # pragma: no cover

    @property
    @abstractmethod
    def dtype(self) -> np.dtype:
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
             out_shape: Optional[RasterShape] = None) -> FutureNdarray:
        ...  # pragma: no cover


class ReaderDriver(object, metaclass=ABCMeta):
    """ Interface for Reader Driver
    """

    @abstractmethod
    def new_load_context(self,
                         bands: Iterable[BandInfo],
                         old_ctx: Optional[Any]) -> Any:
        """Recycle old context if available/possible and create new context.
           ``old_ctx`` won't be used after this call.

           Same context object is passed to all calls to ``open`` function that
           happen within the same ``dc.load``.

           If your driver doesn't need it just return ``None``
        """
        ...  # pragma: no cover

    @abstractmethod
    def open(self, band: BandInfo, ctx: Any) -> FutureGeoRasterReader:
        ...  # pragma: no cover


class ReaderDriverEntry(object, metaclass=ABCMeta):
    @property
    @abstractmethod
    def protocols(self) -> List[str]:
        ...  # pragma: no cover

    @property
    @abstractmethod
    def formats(self) -> List[str]:
        ...  # pragma: no cover

    @abstractmethod
    def supports(self, protocol: str, fmt: str) -> bool:
        ...  # pragma: no cover

    def new_instance(self, cfg: dict) -> ReaderDriver:
        ...  # pragma: no cover

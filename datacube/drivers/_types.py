# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2025 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
"""Defines abstract types for IO drivers."""

from abc import ABCMeta, abstractmethod
from collections.abc import Iterable
from concurrent.futures import Future
from typing import TYPE_CHECKING, Any

import numpy as np
from affine import Affine
from odc.geo.crs import CRS

from datacube.storage import BandInfo

# pylint: disable=invalid-name,unsubscriptable-object,pointless-statement

if TYPE_CHECKING:
    FutureGeoRasterReader = Future["GeoRasterReader"]  # pragma: no cover
    FutureNdarray = Future[np.ndarray]  # pragma: no cover
else:
    FutureGeoRasterReader = Future
    FutureNdarray = Future


RasterShape = tuple[int, int]
RasterWindow = tuple[slice, slice]


class GeoRasterReader(metaclass=ABCMeta):
    """Abstract base class for dataset reader."""

    @property
    @abstractmethod
    def crs(self) -> CRS | None: ...  # pragma: no cover

    @property
    @abstractmethod
    def transform(self) -> Affine | None: ...  # pragma: no cover

    @property
    @abstractmethod
    def dtype(self) -> np.dtype: ...  # pragma: no cover

    @property
    @abstractmethod
    def shape(self) -> RasterShape: ...  # pragma: no cover

    @property
    @abstractmethod
    def nodata(self) -> int | float | None: ...  # pragma: no cover

    @abstractmethod
    def read(
        self, window: RasterWindow | None = None, out_shape: RasterShape | None = None
    ) -> FutureNdarray: ...  # pragma: no cover


class ReaderDriver(metaclass=ABCMeta):
    """Interface for Reader Driver"""

    @abstractmethod
    def new_load_context(self, bands: Iterable[BandInfo], old_ctx: Any | None) -> Any:
        """Recycle old context if available/possible and create new context.
        ``old_ctx`` won't be used after this call.

        Same context object is passed to all calls to ``open`` function that
        happen within the same ``dc.load``.

        If your driver doesn't need it just return ``None``
        """
        ...  # pragma: no cover

    @abstractmethod
    def open(
        self, band: BandInfo, ctx: Any
    ) -> FutureGeoRasterReader: ...  # pragma: no cover


class ReaderDriverEntry(metaclass=ABCMeta):
    @property
    @abstractmethod
    def protocols(self) -> list[str]: ...  # pragma: no cover

    @property
    @abstractmethod
    def formats(self) -> list[str]: ...  # pragma: no cover

    @abstractmethod
    def supports(self, protocol: str, fmt: str) -> bool: ...  # pragma: no cover

    @abstractmethod
    def new_instance(self, cfg: dict) -> ReaderDriver: ...  # pragma: no cover

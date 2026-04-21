# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2026 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
"""Defines abstract types for IO reader drivers."""

from __future__ import annotations

from abc import ABCMeta, abstractmethod
from contextlib import contextmanager
from typing import TypeAlias

TYPE_CHECKING = False
if TYPE_CHECKING:
    from collections.abc import Iterator

    import numpy as np
    from affine import Affine
    from odc.geo import CRS

RasterShape: TypeAlias = tuple[int, int]  # pylint: disable=invalid-name
RasterWindow: TypeAlias = tuple[slice, slice] | tuple[tuple[int, int], tuple[int, int]]  # pylint: disable=invalid-name

# pylint: disable=pointless-statement


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
    def dtype(self) -> str | np.dtype: ...  # pragma: no cover

    @property
    @abstractmethod
    def shape(self) -> RasterShape: ...  # pragma: no cover

    @property
    @abstractmethod
    def nodata(self) -> int | float | None: ...  # pragma: no cover

    @abstractmethod
    def read(
        self, window: RasterWindow | None = None, out_shape: RasterShape | None = None
    ) -> np.ndarray | None: ...  # pragma: no cover


class DataSource(metaclass=ABCMeta):
    """Abstract base class for dataset source."""

    @abstractmethod
    @contextmanager
    def open(self) -> Iterator[GeoRasterReader]: ...  # pragma: no cover

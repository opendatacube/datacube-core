# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2025 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
"""
Driver implementation for Rasterio based reader.
"""

from __future__ import annotations

import contextlib
import logging
from collections.abc import Iterator
from contextlib import contextmanager
from threading import RLock
from urllib.parse import urlparse

import numpy as np
import rasterio
from affine import Affine
from odc.geo import CRS
from typing_extensions import override

from datacube.utils import get_part_from_uri, is_vsipath, uri_to_local_path
from datacube.utils.math import num2numpy
from datacube.utils.rio import activate_from_config

from ..drivers.datasource import DataSource, GeoRasterReader, RasterShape, RasterWindow
from ._base import BandInfo
from ._hdf5 import HDF5_LOCK

_LOG: logging.Logger = logging.getLogger(__name__)


def _rasterio_crs(src):
    if src.crs is None:
        raise ValueError("no CRS")

    return CRS(src.crs)


def maybe_lock(lock):
    if lock is None:
        return contextlib.suppress()
    return lock


class BandDataSource(GeoRasterReader):
    """
    Wrapper for a :class:`rasterio.Band` object
    """

    def __init__(
        self, source: rasterio.Band, nodata=None, lock: RLock | None = None
    ) -> None:
        self.source = source
        if nodata is None:
            nodata = self.source.ds.nodatavals[self.source.bidx - 1]

        self._nodata = num2numpy(nodata, source.dtype)
        self._lock = lock

    @property
    def nodata(self):
        return self._nodata

    @override
    @property
    def crs(self) -> CRS:
        return _rasterio_crs(self.source.ds)

    @override
    @property
    def transform(self) -> Affine:
        return self.source.ds.transform

    @override
    @property
    def dtype(self) -> np.dtype:
        return np.dtype(self.source.dtype)

    @override
    @property
    def shape(self) -> RasterShape:
        return self.source.shape

    @override
    def read(
        self, window: RasterWindow | None = None, out_shape: RasterShape | None = None
    ) -> np.ndarray | None:
        """Read data in the native format, returning a numpy array"""
        with maybe_lock(self._lock):
            return self.source.ds.read(
                indexes=self.source.bidx, window=window, out_shape=out_shape
            )


class RasterioDataSource(DataSource):
    """
    Abstract class used by fuse_sources and :func:`read_from_source`
    """

    def __init__(self, filename, nodata, lock=None) -> None:
        self.filename = filename
        self.nodata = nodata
        self._lock = lock

    @override
    def get_bandnumber(self, src):
        raise NotImplementedError()

    @override
    def get_transform(self, shape):
        raise NotImplementedError()

    @override
    def get_crs(self):
        raise NotImplementedError()

    @override
    @contextmanager
    def open(self) -> Iterator[GeoRasterReader]:
        """Context manager which returns a :class:`BandDataSource`"""

        activate_from_config()  # check if settings changed and apply new

        lock = self._lock
        locked = False if lock is None else lock.acquire(blocking=True)

        try:
            _LOG.debug("opening %s", self.filename)
            with rasterio.open(str(self.filename), sharing=False) as src:
                broken = src.transform.is_identity

                try:
                    _ = _rasterio_crs(src)
                except ValueError:
                    broken = True

                bandnumber = self.get_bandnumber(src)
                band = rasterio.band(src, bandnumber)
                nodata = (
                    src.nodatavals[band.bidx - 1]
                    if src.nodatavals[band.bidx - 1] is not None
                    else self.nodata
                )
                nodata = num2numpy(nodata, band.dtype)

                if locked:
                    locked = False
                    lock.release()

                if broken:
                    raise RuntimeError(
                        f'Broken/missing geospatial data was found in file "{self.filename}"'
                    )
                yield BandDataSource(band, nodata=nodata, lock=lock)

        except Exception as e:
            _LOG.error("Error opening source dataset: %s", self.filename)
            raise e
        finally:
            if locked:
                lock.release()


class RasterDatasetDataSource(RasterioDataSource):
    """Data source for reading from a Data Cube Dataset"""

    def __init__(self, band: BandInfo) -> None:
        """
        Initialise for reading from a Data Cube Dataset.

        :param band: band to read.
        """
        self._band_info = band
        self._hdf = _is_hdf(band.format)
        self._part = get_part_from_uri(band.uri)
        filename = _url2rasterio(band.uri, band.format, band.layer)
        lock = HDF5_LOCK if self._hdf else None
        super().__init__(filename, nodata=band.nodata, lock=lock)

    @override
    def get_bandnumber(self, src=None) -> int | None:
        # If `band` property is set to an integer it overrides any other logic
        bi = self._band_info
        if bi.band is not None:
            return bi.band

        if not self._hdf:
            return 1

        # Netcdf/hdf only below
        if self._part is not None:
            return self._part + 1  # Convert to rasterio 1-based indexing

        if src is None:
            # File wasnt' open, could be unstacked file in a new format, or
            # stacked/unstacked in old. We assume caller knows what to do
            # (maybe based on some side-channel information), so just report
            # undefined.
            return None

        if src.count == 1:  # Single-slice netcdf file
            return 1

        raise DeprecationWarning(
            "Stacked netcdf without explicit time index is not supported anymore"
        )

    @override
    def get_transform(self, shape: RasterShape) -> Affine:
        return self._band_info.transform * Affine.scale(  # type: ignore[operator, type-var, return-value]
            1 / shape[1], 1 / shape[0]
        )

    @override
    def get_crs(self):
        return self._band_info.crs


def _is_hdf(fmt: str) -> bool:
    """Check if format is of HDF type (this includes netcdf variants)"""
    fmt = fmt.lower()
    return any(f in fmt for f in ("netcdf", "hdf"))


def _build_hdf_uri(url_str: str, fmt: str, layer: str) -> str:
    if is_vsipath(url_str):
        base = url_str
    else:
        url = urlparse(url_str)
        if url.scheme in (None, ""):
            raise ValueError("Expect either URL or /vsi path")

        if url.scheme != "file":
            raise RuntimeError(f"Can't access {fmt} over {url.scheme}")
        base = str(uri_to_local_path(url_str))

    return f'{fmt}:"{base}":{layer}'


def _url2rasterio(url_str: str, fmt: str, layer: str | None) -> str:
    """
    turn URL into a string that could be passed to raterio.open
    """
    if _is_hdf(fmt):
        if layer is None:
            raise ValueError("Missing layer for hdf/netcdf format dataset")

        return _build_hdf_uri(url_str, fmt, layer)

    if is_vsipath(url_str):
        return url_str

    url = urlparse(url_str)
    if url.scheme in (None, ""):
        raise ValueError("Expect either URL or /vsi path")

    if url.scheme == "file":
        # if local path strip scheme and other gunk
        return str(uri_to_local_path(url_str))

    return url_str

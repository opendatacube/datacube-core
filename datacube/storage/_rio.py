# coding=utf-8
"""
Driver implementation for Rasterio based reader.
"""
import logging
import warnings
import contextlib
from contextlib import contextmanager
from threading import RLock
import numpy as np
from affine import Affine
import rasterio
from urllib.parse import urlparse
from typing import Optional, Iterator

from datacube.utils import geometry
from datacube.utils.math import num2numpy
from datacube.utils import uri_to_local_path, get_part_from_uri, is_vsipath
from datacube.utils.rio import activate_from_config
from . import DataSource, GeoRasterReader, RasterShape, RasterWindow, BandInfo
from ._hdf5 import HDF5_LOCK

_LOG = logging.getLogger(__name__)


def _rasterio_crs(src):
    if src.crs is None:
        raise ValueError('no CRS')

    return geometry.CRS(src.crs)


def maybe_lock(lock):
    if lock is None:
        return contextlib.suppress()
    return lock


class BandDataSource(GeoRasterReader):
    """
    Wrapper for a :class:`rasterio.Band` object

    :type source: rasterio.Band
    """

    def __init__(self, source, nodata=None,
                 lock: Optional[RLock] = None):
        self.source = source
        if nodata is None:
            nodata = self.source.ds.nodatavals[self.source.bidx-1]

        self._nodata = num2numpy(nodata, source.dtype)
        self._lock = lock

    @property
    def nodata(self):
        return self._nodata

    @property
    def crs(self) -> geometry.CRS:
        return _rasterio_crs(self.source.ds)

    @property
    def transform(self) -> Affine:
        return self.source.ds.transform

    @property
    def dtype(self) -> np.dtype:
        return np.dtype(self.source.dtype)

    @property
    def shape(self) -> RasterShape:
        return self.source.shape

    def read(self, window: Optional[RasterWindow] = None,
             out_shape: Optional[RasterShape] = None) -> Optional[np.ndarray]:
        """Read data in the native format, returning a numpy array
        """
        with maybe_lock(self._lock):
            return self.source.ds.read(indexes=self.source.bidx, window=window, out_shape=out_shape)


class OverrideBandDataSource(GeoRasterReader):
    """Wrapper for a rasterio.Band object that overrides nodata, CRS and transform

    This is useful for files with malformed or missing properties.


    :type source: rasterio.Band
    """

    def __init__(self,
                 source: rasterio.Band,
                 nodata,
                 crs: geometry.CRS,
                 transform: Affine,
                 lock: Optional[RLock] = None):
        self.source = source
        self._nodata = num2numpy(nodata, source.dtype)
        self._crs = crs
        self._transform = transform
        self._lock = lock

    @property
    def crs(self) -> geometry.CRS:
        return self._crs

    @property
    def transform(self) -> Affine:
        return self._transform

    @property
    def nodata(self):
        return self._nodata

    @property
    def dtype(self) -> np.dtype:
        return np.dtype(self.source.dtype)

    @property
    def shape(self) -> RasterShape:
        return self.source.shape

    def read(self, window: Optional[RasterWindow] = None,
             out_shape: Optional[RasterShape] = None) -> Optional[np.ndarray]:
        """Read data in the native format, returning a native array
        """
        with maybe_lock(self._lock):
            return self.source.ds.read(indexes=self.source.bidx, window=window, out_shape=out_shape)


class RasterioDataSource(DataSource):
    """
    Abstract class used by fuse_sources and :func:`read_from_source`

    """

    def __init__(self, filename, nodata, lock=None):
        self.filename = filename
        self.nodata = nodata
        self._lock = lock

    def get_bandnumber(self, src):
        raise NotImplementedError()

    def get_transform(self, shape):
        raise NotImplementedError()

    def get_crs(self):
        raise NotImplementedError()

    @contextmanager
    def open(self) -> Iterator[GeoRasterReader]:
        """Context manager which returns a :class:`BandDataSource`"""

        activate_from_config()  # check if settings changed and apply new

        lock = self._lock
        locked = False if lock is None else lock.acquire(blocking=True)

        try:
            _LOG.debug("opening %s", self.filename)
            with rasterio.open(self.filename, sharing=False) as src:
                override = False

                transform = src.transform
                if transform.is_identity:
                    override = True
                    transform = self.get_transform(src.shape)

                try:
                    crs = _rasterio_crs(src)
                except ValueError:
                    override = True
                    crs = self.get_crs()

                bandnumber = self.get_bandnumber(src)
                band = rasterio.band(src, bandnumber)
                nodata = src.nodatavals[band.bidx-1] if src.nodatavals[band.bidx-1] is not None else self.nodata
                nodata = num2numpy(nodata, band.dtype)

                if locked:
                    locked = False
                    lock.release()

                if override:
                    warnings.warn(f"""Broken/missing geospatial data was found in file:
"{self.filename}"
Will use approximate metadata for backwards compatibility reasons (#673).
This behaviour is deprecated. Future versions will raise an error.""",
                                  category=DeprecationWarning)
                    yield OverrideBandDataSource(band, nodata=nodata, crs=crs, transform=transform, lock=lock)
                else:
                    yield BandDataSource(band, nodata=nodata, lock=lock)

        except Exception as e:
            _LOG.error("Error opening source dataset: %s", self.filename)
            raise e
        finally:
            if locked:
                lock.release()


class RasterDatasetDataSource(RasterioDataSource):
    """Data source for reading from a Data Cube Dataset"""

    def __init__(self, band: BandInfo):
        """
        Initialise for reading from a Data Cube Dataset.

        :param dataset: dataset to read from
        :param measurement_id: measurement to read. a single 'band' or 'slice'
        """
        self._band_info = band
        self._hdf = _is_hdf(band.format)
        self._part = get_part_from_uri(band.uri)
        filename = _url2rasterio(band.uri, band.format, band.layer)
        lock = HDF5_LOCK if self._hdf else None
        super(RasterDatasetDataSource, self).__init__(filename, nodata=band.nodata, lock=lock)

    def get_bandnumber(self, src=None) -> Optional[int]:

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

        raise DeprecationWarning("Stacked netcdf without explicit time index is not supported anymore")

    def get_transform(self, shape: RasterShape) -> Affine:
        return self._band_info.transform * Affine.scale(1 / shape[1], 1 / shape[0])

    def get_crs(self):
        return self._band_info.crs


def _is_hdf(fmt: str) -> bool:
    """ Check if format is of HDF type (this includes netcdf variants)
    """
    fmt = fmt.lower()
    return any(f in fmt for f in ('netcdf', 'hdf'))


def _build_hdf_uri(url_str: str, fmt: str, layer: str) -> str:
    if is_vsipath(url_str):
        base = url_str
    else:
        url = urlparse(url_str)
        if url.scheme in (None, ''):
            raise ValueError("Expect either URL or /vsi path")

        if url.scheme != 'file':
            raise RuntimeError("Can't access %s over %s" % (fmt, url.scheme))
        base = str(uri_to_local_path(url_str))

    return '{}:"{}":{}'.format(fmt, base, layer)


def _url2rasterio(url_str: str, fmt: str, layer: Optional[str]) -> str:
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
    if url.scheme in (None, ''):
        raise ValueError("Expect either URL or /vsi path")

    if url.scheme == 'file':
        # if local path strip scheme and other gunk
        return str(uri_to_local_path(url_str))

    return url_str

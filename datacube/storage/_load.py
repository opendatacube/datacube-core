"""
Important functions are:

* :func:`reproject_and_fuse`

"""
import logging
from collections import OrderedDict
import numpy as np
from xarray.core.dataarray import DataArray as XrDataArray
from xarray.core.dataset import Dataset as XrDataset
from typing import (
    Union, Optional, Callable,
    List, Any, Iterator, Iterable, Mapping, Tuple
)

from datacube.utils import ignore_exceptions_if
from datacube.utils.geometry import GeoBox, Coordinate, roi_is_empty
from datacube.model import Measurement
from datacube.drivers._types import ReaderDriver
from . import DataSource, BandInfo

_LOG = logging.getLogger(__name__)

FuserFunction = Callable[[np.ndarray, np.ndarray], Any]  # pylint: disable=invalid-name


def reproject_and_fuse(datasources: List[DataSource],
                       destination: np.ndarray,
                       dst_gbox: GeoBox,
                       dst_nodata: Optional[Union[int, float]],
                       resampling: str = 'nearest',
                       fuse_func: Optional[FuserFunction] = None,
                       skip_broken_datasets: bool = False):
    """
    Reproject and fuse `sources` into a 2D numpy array `destination`.

    :param datasources: Data sources to open and read from
    :param destination: ndarray of appropriate size to read data into
    :param dst_gbox: GeoBox defining destination region
    :param skip_broken_datasets: Carry on in the face of adversity and failing reads.
    """
    # pylint: disable=too-many-locals
    from ._read import read_time_slice
    assert len(destination.shape) == 2

    def copyto_fuser(dest: np.ndarray, src: np.ndarray) -> None:
        where_nodata = (dest == dst_nodata) if not np.isnan(dst_nodata) else np.isnan(dest)
        np.copyto(dest, src, where=where_nodata)

    fuse_func = fuse_func or copyto_fuser

    destination.fill(dst_nodata)
    if len(datasources) == 0:
        return destination
    elif len(datasources) == 1:
        with ignore_exceptions_if(skip_broken_datasets):
            with datasources[0].open() as rdr:
                read_time_slice(rdr, destination, dst_gbox, resampling, dst_nodata)

        return destination
    else:
        # Multiple sources, we need to fuse them together into a single array
        buffer_ = np.full(destination.shape, dst_nodata, dtype=destination.dtype)
        for source in datasources:
            with ignore_exceptions_if(skip_broken_datasets):
                with source.open() as rdr:
                    roi = read_time_slice(rdr, buffer_, dst_gbox, resampling, dst_nodata)

                if not roi_is_empty(roi):
                    fuse_func(destination[roi], buffer_[roi])
                    buffer_[roi] = dst_nodata  # clean up for next read

        return destination


def _coord_to_xr(name: str, c: Coordinate) -> XrDataArray:
    """ Construct xr.DataArray from named Coordinate object, this can then be used
        to define coordinates for xr.Dataset|xr.DataArray
    """
    return XrDataArray(c.values,
                       coords={name: c.values},
                       dims=(name,),
                       attrs={'units': c.units})


def _mk_empty_ds(coords: Mapping[str, XrDataArray], geobox: GeoBox) -> XrDataset:
    cc = OrderedDict(coords.items())
    cc.update((n, _coord_to_xr(n, c)) for n, c in geobox.coordinates.items())
    return XrDataset(coords=cc, attrs={'crs': geobox.crs})


def _allocate_storage(coords: Mapping[str, XrDataArray],
                      geobox: GeoBox,
                      measurements: Iterable[Measurement]) -> XrDataset:
    xx = _mk_empty_ds(coords, geobox)
    dims = list(xx.coords.keys())
    shape = tuple(xx.sizes[k] for k in dims)

    for m in measurements:
        name, dtype, attrs = m.name, m.dtype, m.dataarray_attrs()
        attrs['crs'] = geobox.crs
        data = np.empty(shape, dtype=dtype)
        xx[name] = XrDataArray(data, coords=xx.coords, dims=dims, name=name, attrs=attrs)

    return xx


def xr_load(sources: XrDataArray,
            geobox: GeoBox,
            measurements: List[Measurement],
            driver: ReaderDriver,
            driver_ctx_prev: Optional[Any] = None,
            skip_broken_datasets: bool = False) -> Tuple[XrDataset, Any]:
    # pylint: disable=too-many-locals

    out = _allocate_storage(sources.coords, geobox, measurements)

    def all_groups() -> Iterator[Tuple[Any, int, List[BandInfo]]]:
        for idx, dss in np.ndenumerate(sources.values):
            for m in measurements:
                bbi = [BandInfo(ds, m.name) for ds in dss]
                yield (m, idx, bbi)

    def just_bands(groups) -> Iterator[BandInfo]:
        for _, _, bbi in groups:
            yield from bbi

    groups = list(all_groups())
    ctx = driver.new_load_context(just_bands(groups), driver_ctx_prev)

    # TODO: run upto N concurrently
    for m, idx, bbi in groups:
        dst = out[m.name][idx]
        dst[:] = m.nodata

        for band in bbi:
            rdr = driver.open(band, ctx).result()
            print(rdr.crs)
            print(rdr.nodata)

    return out, ctx

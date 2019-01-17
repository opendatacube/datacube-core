"""
This modules provides interim solution for supporting S3AIO read driver.

This module will be removed once S3AIO driver migrates to new style.
"""
import logging
import numpy as np
from typing import Union, Optional, Callable, List, Any

from datacube.utils import ignore_exceptions_if
from datacube.utils.geometry import GeoBox, roi_is_empty
from . import DataSource

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

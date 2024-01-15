# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2024 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
import warnings
import toolz                                  # type: ignore[import]
import rasterio                               # type: ignore[import]
from rasterio.shutil import copy as rio_copy  # type: ignore[import]
import numpy as np
import xarray as xr
import dask
from dask.delayed import Delayed
from pathlib import Path
from typing import Union, Optional, List, Any, Dict

from datacube.migration import ODC2DeprecationWarning

from .io import check_write_path
from .geometry import GeoBox
from .geometry.tools import align_up

from deprecat import deprecat

__all__ = ("write_cog", "to_cog")


def _adjust_blocksize(block, dim):
    if block > dim:
        return align_up(dim, 16)
    return align_up(block, 16)


def _write_cog(
    pix: np.ndarray,
    geobox: GeoBox,
    fname: Union[Path, str],
    nodata: Optional[float] = None,
    overwrite: bool = False,
    blocksize: Optional[int] = None,
    overview_resampling: Optional[str] = None,
    overview_levels: Optional[List[int]] = None,
    ovr_blocksize: Optional[int] = None,
    use_windowed_writes: bool = False,
    intermediate_compression: Union[bool, str, Dict[str, Any]] = False,
    **extra_rio_opts
) -> Union[Path, bytes]:
    """Write geo-registered ndarray to a GeoTiff file or RAM.

    :param pix: ``xarray.DataArray`` with crs or (ndarray, geobox, nodata) triple
    :param fname:  Output file or ":mem:"
    :param nodata: Set ``nodata`` flag to this value if supplied
    :param overwrite: True -- replace existing file, False -- abort with IOError exception
    :param blocksize: Size of internal tiff tiles (512x512 pixels)
    :param ovr_blocksize: Size of internal tiles in overview images (defaults to blocksize)
    :param overview_resampling: Use this resampling when computing overviews
    :param overview_levels: List of shrink factors to compute overviews for: [2,4,8,16,32]
                            to disable overviews supply empty list ``[]``
    :param use_windowed_writes: Write image block by block (might need this for large images)
    :param intermediate_compression: Configure compression settings for first pass write, default is no compression
    :param extra_rio_opts: Any other option is passed to ``rasterio.open``

    When fname=":mem:" write COG to memory rather than to a file and return it
    as a memoryview object.

    NOTE: about memory requirements

    This function generates a temporary in memory tiff file without compression
    to speed things up. It then adds overviews to this file and only then
    copies it to the final destination with requested compression settings.
    This is necessary to produce a compliant COG, since the COG standard demands
    overviews to be placed before native resolution data and a double pass is the
    only way to achieve this currently.

    This means that this function will use about 1.5 to 2 times memory taken by `pix`.
    """
    # pylint: disable=too-many-locals
    if blocksize is None:
        blocksize = 512
    if ovr_blocksize is None:
        ovr_blocksize = blocksize
    if overview_resampling is None:
        overview_resampling = "nearest"

    # normalise intermediate_compression argument to a dict()
    if isinstance(intermediate_compression, bool):
        intermediate_compression = (
            {"compress": "deflate", "zlevel": 2} if intermediate_compression else {}
        )
    elif isinstance(intermediate_compression, str):
        intermediate_compression = {"compress": intermediate_compression}

    if pix.ndim == 2:
        h, w = pix.shape
        nbands = 1
        band = 1  # type: Any
    elif pix.ndim == 3:
        if pix.shape[:2] == geobox.shape:
            pix = pix.transpose([2, 0, 1])
        elif pix.shape[-2:] != geobox.shape:
            raise ValueError("GeoBox shape does not match image shape")

        nbands, h, w = pix.shape
        band = tuple(i for i in range(1, nbands + 1))
    else:
        raise ValueError("Need 2d or 3d ndarray on input")

    assert geobox.shape == (h, w)

    if overview_levels is None:
        if min(w, h) < 512:
            overview_levels = []
        else:
            overview_levels = [2 ** i for i in range(1, 6)]

    if fname != ":mem:":
        path = check_write_path(
            fname, overwrite
        )  # aborts if overwrite=False and file exists already

    resampling = rasterio.enums.Resampling[overview_resampling]

    if (blocksize % 16) != 0:
        warnings.warn("Block size must be a multiple of 16, will be adjusted")

    rio_opts = dict(
        width=w,
        height=h,
        count=nbands,
        dtype=pix.dtype.name,
        crs=str(geobox.crs),
        transform=geobox.transform,
        tiled=True,
        blockxsize=_adjust_blocksize(blocksize, w),
        blockysize=_adjust_blocksize(blocksize, h),
        zlevel=6,
        predictor=3 if pix.dtype.kind == "f" else 2,
        compress="DEFLATE",
    )

    if nodata is not None:
        rio_opts.update(nodata=nodata)

    rio_opts.update(extra_rio_opts)

    def _write(pix, band, dst):
        if not use_windowed_writes:
            dst.write(pix, band)
            return

        for _, win in dst.block_windows():
            if pix.ndim == 2:
                block = pix[win.toslices()]
            else:
                block = pix[(slice(None),) + win.toslices()]

            dst.write(block, indexes=band, window=win)

    # Deal efficiently with "no overviews needed case"
    if len(overview_levels) == 0:
        if fname == ":mem:":
            with rasterio.MemoryFile() as mem:
                with mem.open(driver="GTiff", **rio_opts) as dst:
                    _write(pix, band, dst)
                return bytes(mem.getbuffer())
        else:
            with rasterio.open(path, mode="w", driver="GTiff", **rio_opts) as dst:
                _write(pix, band, dst)
            return path

    # copy re-compresses anyway so skip compression for temp image
    tmp_opts = toolz.dicttoolz.dissoc(rio_opts, "compress", "predictor", "zlevel")
    tmp_opts.update(intermediate_compression)

    with rasterio.Env(GDAL_TIFF_OVR_BLOCKSIZE=ovr_blocksize):
        with rasterio.MemoryFile() as mem:
            with mem.open(driver="GTiff", **tmp_opts) as tmp:
                _write(pix, band, tmp)
                tmp.build_overviews(overview_levels, resampling)

                if fname == ":mem:":
                    with rasterio.MemoryFile() as mem2:
                        rio_copy(
                            tmp,
                            mem2.name,
                            driver="GTiff",
                            copy_src_overviews=True,
                            **toolz.dicttoolz.dissoc(
                                rio_opts,
                                "width",
                                "height",
                                "count",
                                "dtype",
                                "crs",
                                "transform",
                                "nodata",
                            )
                        )
                        return bytes(mem2.getbuffer())

                rio_copy(tmp, path, driver="GTiff", copy_src_overviews=True, **rio_opts)

    return path


_delayed_write_cog_to_mem = dask.delayed(  # pylint: disable=invalid-name
    _write_cog, name="compress-cog", pure=True, nout=1
)

_delayed_write_cog_to_file = dask.delayed(  # pylint: disable=invalid-name
    _write_cog, name="save-cog", pure=False, nout=1
)


@deprecat(reason='This method has been moved to odc-geo.', version='1.9.0', category=ODC2DeprecationWarning)
def write_cog(
    geo_im: xr.DataArray,
    fname: Union[str, Path],
    overwrite: bool = False,
    blocksize: Optional[int] = None,
    ovr_blocksize: Optional[int] = None,
    overview_resampling: Optional[str] = None,
    overview_levels: Optional[List[int]] = None,
    use_windowed_writes: bool = False,
    intermediate_compression: Union[bool, str, Dict[str, Any]] = False,
    **extra_rio_opts
) -> Union[Path, bytes, Delayed]:
    """
    Save ``xarray.DataArray`` to a file in Cloud Optimized GeoTiff format.

    This function is "Dask aware". If ``geo_im`` is a Dask array, then the
    output of this function is also a Dask Delayed object. This allows us to
    save multiple images concurrently across a Dask cluster. If you are not
    familiar with Dask this can be confusing, as no operation is performed until the
    ``.compute()`` method is called, so if you call this function with Dask
    array it will return immediately without writing anything to disk.

    If you are using Dask to speed up data loading, follow the example below:

    .. code-block:: python

       # Example: save red band from first time slice to file "red.tif"
       xx = dc.load(.., dask_chunks=dict(x=1024, y=1024))
       write_cog(xx.isel(time=0).red, "red.tif").compute()
       # or compute input first instead
       write_cog(xx.isel(time=0).red.compute(), "red.tif")

    :param geo_im: ``xarray.DataArray`` with crs
    :param fname: Output path or ``":mem:"`` in which case compress to RAM and return bytes
    :param overwrite: True -- replace existing file, False -- abort with IOError exception
    :param blocksize: Size of internal tiff tiles (512x512 pixels)
    :param ovr_blocksize: Size of internal tiles in overview images (defaults to blocksize)
    :param overview_resampling: Use this resampling when computing overviews
    :param overview_levels: List of shrink factors to compute overiews for: [2,4,8,16,32],
                            to disable overviews supply empty list ``[]``
    :param nodata: Set ``nodata`` flag to this value if supplied, by default ``nodata`` is
                   read from the attributes of the input array (``geo_im.attrs['nodata']``).
    :param use_windowed_writes: Write image block by block (might need this for large images)
    :param intermediate_compression: Configure compression settings for first pass write, default is no compression
    :param extra_rio_opts: Any other option is passed to ``rasterio.open``

    :returns: Path to which output was written
    :returns: Bytes if ``fname=":mem:"``
    :returns: ``dask.Delayed`` object if input is a Dask array

    .. note ::

       **memory requirements**

       This function generates a temporary in memory tiff file without
       compression to speed things up. It then adds overviews to this file and
       only then copies it to the final destination with requested compression
       settings. This is necessary to produce a compliant COG, since the COG standard
       demands overviews to be placed before native resolution data and double
       pass is the only way to achieve this currently.

       This means that this function will use about 1.5 to 2 times memory taken by ``geo_im``.
    """
    pix = geo_im.data
    geobox = getattr(geo_im, "geobox", None)
    nodata = extra_rio_opts.pop("nodata", None)
    if nodata is None:
        nodata = geo_im.attrs.get("nodata", None)

    if geobox is None:
        raise ValueError("Need geo-registered array on input")

    if dask.is_dask_collection(pix):
        real_op = (
            _delayed_write_cog_to_mem
            if fname == ":mem:"
            else _delayed_write_cog_to_file
        )
    else:
        real_op = _write_cog

    return real_op(
        pix,
        geobox,
        fname,
        nodata=nodata,
        overwrite=overwrite,
        blocksize=blocksize,
        ovr_blocksize=ovr_blocksize,
        overview_resampling=overview_resampling,
        overview_levels=overview_levels,
        use_windowed_writes=use_windowed_writes,
        intermediate_compression=intermediate_compression,
        **extra_rio_opts
    )


@deprecat(reason='This method has been moved to odc-geo.', version='1.9.0', category=ODC2DeprecationWarning)
def to_cog(
    geo_im: xr.DataArray,
    blocksize: Optional[int] = None,
    ovr_blocksize: Optional[int] = None,
    overview_resampling: Optional[str] = None,
    overview_levels: Optional[List[int]] = None,
    use_windowed_writes: bool = False,
    intermediate_compression: Union[bool, str, Dict[str, Any]] = False,
    **extra_rio_opts
) -> Union[bytes, Delayed]:
    """
    Compress ``xarray.DataArray`` into Cloud Optimized GeoTiff bytes in memory.

    This function doesn't write to disk, it compresses in RAM, which is useful
    for saving data to S3 or other cloud object stores.

    This function is "Dask aware". If ``geo_im`` is a Dask array, then the
    output of this function is also a Dask Delayed object. This allows us to
    compress multiple images concurrently across a Dask cluster. If you are not
    familiar with Dask this can be confusing, as no operation is performed until the
    ``.compute()`` method is called, so if you call this function with Dask
    array it will return immediately without compressing any data.

    :param geo_im: ``xarray.DataArray`` with crs
    :param blocksize: Size of internal tiff tiles (512x512 pixels)
    :param ovr_blocksize: Size of internal tiles in overview images (defaults to blocksize)
    :param overview_resampling: Use this resampling when computing overviews
    :param overview_levels: List of shrink factors to compute overiews for: [2,4,8,16,32]
    :param nodata: Set ``nodata`` flag to this value if supplied, by default ``nodata`` is
                   read from the attributes of the input array (``geo_im.attrs['nodata']``).
    :param use_windowed_writes: Write image block by block (might need this for large images)
    :param intermediate_compression: Configure compression settings for first pass write, default is no compression
    :param extra_rio_opts: Any other option is passed to ``rasterio.open``

    :returns: In-memory GeoTiff file as bytes
    :returns: ``dask.Delayed`` object if input is a Dask array

    Also see :py:meth:`~datacube.utils.cog.write_cog`

    """
    bb = write_cog(
        geo_im,
        ":mem:",
        blocksize=blocksize,
        ovr_blocksize=ovr_blocksize,
        overview_resampling=overview_resampling,
        overview_levels=overview_levels,
        use_windowed_writes=use_windowed_writes,
        intermediate_compression=intermediate_compression,
        **extra_rio_opts
    )

    assert isinstance(
        bb, (bytes, Delayed)
    )  # for mypy sake for :mem: output it bytes or delayed bytes
    return bb

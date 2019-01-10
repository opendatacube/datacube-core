from typing import Union, Optional
import rasterio.warp
import rasterio.crs
import numpy as np
from affine import Affine
from . import GeoBox

Resampling = Union[str, int, rasterio.warp.Resampling]  # pylint: disable=invalid-name
Nodata = Optional[Union[int, float]]  # pylint: disable=invalid-name
_WRP_CRS = rasterio.crs.CRS.from_epsg(3857)


def resampling_s2rio(name: str) -> rasterio.warp.Resampling:
    """
    Convert from string to rasterio.warp.Resampling enum, raises ValueError on bad input.
    """
    try:
        return getattr(rasterio.warp.Resampling, name.lower())
    except AttributeError:
        raise ValueError('Bad resampling parameter: {}'.format(name))


def is_resampling_nn(resampling: Resampling) -> bool:
    """
    :returns: True if resampling mode is nearest neighbour
    :returns: False otherwise
    """
    if isinstance(resampling, str):
        return resampling.lower() == 'nearest'
    return resampling == rasterio.warp.Resampling.nearest


def warp_affine_rio(src: np.ndarray,
                    dst: np.ndarray,
                    A: Affine,
                    resampling: Resampling,
                    src_nodata: Nodata = None,
                    dst_nodata: Nodata = None,
                    **kwargs) -> np.ndarray:
    """
    Perform Affine warp using rasterio as backend library.

    :param        src: image as ndarray
    :param        dst: image as ndarray
    :param          A: Affine transformm, maps from dst_coords to src_coords
    :param resampling: str|rasterio.warp.Resampling resampling strategy
    :param src_nodata: Value representing "no data" in the source image
    :param dst_nodata: Value to represent "no data" in the destination image

    **kwargs -- any other args to pass to ``rasterio.warp.reproject``

    :returns: dst
    """
    crs = _WRP_CRS
    src_transform = Affine.identity()
    dst_transform = A

    if isinstance(resampling, str):
        resampling = resampling_s2rio(resampling)

    dst0 = dst

    # TODO: GDAL doesn't support signed 8-bit values, so we coerce to uint8,
    # *BUT* this is only valid for nearest|mode resampling strategies, proper
    # way is to perform warp in int16 space and then convert back to int8 with
    # clamping.
    if dst.dtype.name == 'int8':
        dst = dst.view('uint8')
        if dst_nodata is not None:
            dst_nodata = int(np.uint8(dst_nodata))
    if src.dtype.name == 'int8':
        src = src.view('uint8')
        if src_nodata is not None:
            src_nodata = int(np.uint8(src_nodata))

    rasterio.warp.reproject(src,
                            dst,
                            src_transform=src_transform,
                            dst_transform=dst_transform,
                            src_crs=crs,
                            dst_crs=crs,
                            resampling=resampling,
                            src_nodata=src_nodata,
                            dst_nodata=dst_nodata,
                            **kwargs)

    return dst0


def warp_affine(src: np.ndarray,
                dst: np.ndarray,
                A: Affine,
                resampling: Resampling,
                src_nodata: Nodata = None,
                dst_nodata: Nodata = None,
                **kwargs) -> np.ndarray:
    """
    Perform Affine warp using best available backend (GDAL via rasterio is the only one so far).

    :param        src: image as ndarray
    :param        dst: image as ndarray
    :param          A: Affine transformm, maps from dst_coords to src_coords
    :param resampling: str resampling strategy
    :param src_nodata: Value representing "no data" in the source image
    :param dst_nodata: Value to represent "no data" in the destination image

    **kwargs -- any other args to pass to implementation

    :returns: dst
    """
    return warp_affine_rio(src, dst, A, resampling,
                           src_nodata=src_nodata,
                           dst_nodata=dst_nodata,
                           **kwargs)


def rio_reproject(src: np.ndarray,
                  dst: np.ndarray,
                  s_gbox: GeoBox,
                  d_gbox: GeoBox,
                  resampling: Resampling,
                  src_nodata: Nodata = None,
                  dst_nodata: Nodata = None,
                  **kwargs) -> np.ndarray:
    """
    Perform reproject from ndarray->ndarray using rasterio as backend library.

    :param        src: image as ndarray
    :param        dst: image as ndarray
    :param     s_gbox: GeoBox of source image
    :param     d_gbox: GeoBox of destination image
    :param resampling: str|rasterio.warp.Resampling resampling strategy
    :param src_nodata: Value representing "no data" in the source image
    :param dst_nodata: Value to represent "no data" in the destination image

    **kwargs -- any other args to pass to ``rasterio.warp.reproject``

    :returns: dst
    """
    if isinstance(resampling, str):
        resampling = resampling_s2rio(resampling)

    dst0 = dst

    # TODO: GDAL doesn't support signed 8-bit values, so we coerce to uint8,
    # *BUT* this is only valid for nearest|mode resampling strategies, proper
    # way is to perform warp in int16 space and then convert back to int8 with
    # clamping.
    if dst.dtype.name == 'int8':
        dst = dst.view('uint8')
        if dst_nodata is not None:
            dst_nodata = int(np.uint8(dst_nodata))
    if src.dtype.name == 'int8':
        src = src.view('uint8')
        if src_nodata is not None:
            src_nodata = int(np.uint8(src_nodata))

    rasterio.warp.reproject(src,
                            dst,
                            src_transform=s_gbox.transform,
                            dst_transform=d_gbox.transform,
                            src_crs=str(s_gbox.crs),
                            dst_crs=str(d_gbox.crs),
                            resampling=resampling,
                            src_nodata=src_nodata,
                            dst_nodata=dst_nodata,
                            **kwargs)

    return dst0

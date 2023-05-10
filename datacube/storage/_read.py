# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2020 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
""" Dataset -> Raster
"""
import numpy as np
from typing import Optional, Tuple

from ..utils.math import valid_mask

from odc.geo import wh_
from odc.geo.roi import (
    roi_shape,
    roi_is_empty,
    roi_is_full,
    roi_pad,
    w_,
)
from odc.geo.geobox import GeoBox, zoom_out
from odc.geo.warp import (
    warp_affine,
    rio_reproject,
    is_resampling_nn,
    Resampling,
    Nodata,
)
from odc.geo.overlap import compute_reproject_roi, is_affine_st
from odc.geo.math import is_almost_int


def rdr_geobox(rdr) -> GeoBox:
    """ Construct GeoBox from opened dataset reader.
    """
    h, w = rdr.shape
    return GeoBox(wh_(w, h), rdr.transform, rdr.crs)


def pick_read_scale(scale: float, rdr=None, tol=1e-3):
    assert scale > 0
    # First find nearest integer scale
    #    Scale down to nearest integer, unless we can scale up by less than tol
    #
    # 2.999999 -> 3
    # 2.8 -> 2
    # 0.3 -> 1

    if scale < 1:
        return 1

    if is_almost_int(scale, tol):
        scale = np.round(scale)

    scale = int(scale)

    if rdr is not None:
        # TODO: check available overviews in rdr
        pass

    return scale


def read_time_slice(rdr,
                    dst: np.ndarray,
                    dst_gbox: GeoBox,
                    resampling: Resampling,
                    dst_nodata: Nodata,
                    extra_dim_index: Optional[int] = None) -> Tuple[slice, slice]:
    """ From opened reader object read into `dst`

    :returns: affected destination region
    """
    assert dst.shape == dst_gbox.shape
    src_gbox = rdr_geobox(rdr)

    is_nn = is_resampling_nn(resampling)

    rr = compute_reproject_roi(src_gbox, dst_gbox, ttol=0.9 if is_nn else 0.01)

    if roi_is_empty(rr.roi_dst):
        return rr.roi_dst

    scale = pick_read_scale(rr.scale, rdr)

    def norm_read_args(roi, shape, extra_dim_index):
        if roi_is_full(roi, rdr.shape):
            roi = None

        if roi is None and shape == rdr.shape:
            shape = None

        w = w_[roi]

        # Build 3D read window
        # Note: Might be a good idea to natively support nD read windows.
        if extra_dim_index is not None:
            if w is None:
                w = ()
            return (extra_dim_index,) + w, shape
        else:
            # 2D read window
            return w, shape

    if rr.paste_ok:
        A = rr.transform.linear
        sx, sy = A.a, A.e

        dst = dst[rr.roi_dst]
        pix = rdr.read(*norm_read_args(rr.roi_src, dst.shape, extra_dim_index))

        if sx < 0:
            pix = pix[:, ::-1]
        if sy < 0:
            pix = pix[::-1, :]

        if rdr.nodata is None:
            np.copyto(dst, pix)
        else:
            np.copyto(dst, pix, where=valid_mask(pix, rdr.nodata))
    else:
        is_st = False if rr.transform.linear is None else is_affine_st(rr.transform.linear)
        if is_st:
            # add padding on src/dst ROIs, it was set to tight bounds
            # TODO: this should probably happen inside compute_reproject_roi
            rr.roi_dst = roi_pad(rr.roi_dst, 1, dst_gbox.shape)
            rr.roi_src = roi_pad(rr.roi_src, 1, src_gbox.shape)

        dst = dst[rr.roi_dst]
        dst_gbox = dst_gbox[rr.roi_dst]
        src_gbox = src_gbox[rr.roi_src]
        if scale > 1:
            src_gbox = zoom_out(src_gbox, scale)

        pix = rdr.read(*norm_read_args(rr.roi_src, src_gbox.shape, extra_dim_index))

        if rr.transform.linear is not None:
            A = (~src_gbox.transform)*dst_gbox.transform
            warp_affine(pix, dst, A, resampling,
                        src_nodata=rdr.nodata, dst_nodata=dst_nodata)
        else:
            rio_reproject(pix, dst, src_gbox, dst_gbox, resampling,
                          src_nodata=rdr.nodata, dst_nodata=dst_nodata)

    return rr.roi_dst


def read_time_slice_v2(rdr,
                       dst_gbox: GeoBox,
                       resampling: Resampling,
                       dst_nodata: Nodata) -> Tuple[Optional[np.ndarray],
                                                    Tuple[slice, slice]]:
    """ From opened reader object read into `dst`

    :returns: pixels read and ROI of dst_gbox that was affected
    """
    # pylint: disable=too-many-locals
    src_gbox = rdr_geobox(rdr)

    is_nn = is_resampling_nn(resampling)
    rr = compute_reproject_roi(src_gbox, dst_gbox, ttol=0.9 if is_nn else 0.01)

    if roi_is_empty(rr.roi_dst):
        return None, rr.roi_dst

    scale = pick_read_scale(rr.scale, rdr)

    def norm_read_args(roi, shape):
        if roi_is_full(roi, rdr.shape):
            roi = None

        if roi is None and shape == rdr.shape:
            shape = None

        return roi, shape

    if rr.paste_ok:
        read_shape = roi_shape(rr.roi_dst)
        A = rr.transform.linear
        sx, sy = A.a, A.e

        pix = rdr.read(*norm_read_args(rr.roi_src, read_shape)).result()

        if sx < 0:
            pix = pix[:, ::-1]
        if sy < 0:
            pix = pix[::-1, :]

        # normalise nodata to be equal to `dst_nodata`
        if rdr.nodata is not None and rdr.nodata != dst_nodata:
            pix[pix == rdr.nodata] = dst_nodata

        dst = pix
    else:
        is_st = False if rr.transform.linear is None else is_affine_st(rr.transform.linear)
        if is_st:
            # add padding on src/dst ROIs, it was set to tight bounds
            # TODO: this should probably happen inside compute_reproject_roi
            rr.roi_dst = roi_pad(rr.roi_dst, 1, dst_gbox.shape)
            rr.roi_src = roi_pad(rr.roi_src, 1, src_gbox.shape)

        dst_gbox = dst_gbox[rr.roi_dst]
        src_gbox = src_gbox[rr.roi_src]
        if scale > 1:
            src_gbox = zoom_out(src_gbox, scale)

        dst = np.full(dst_gbox.shape, dst_nodata, dtype=rdr.dtype)
        pix = rdr.read(*norm_read_args(rr.roi_src, src_gbox.shape)).result()

        if rr.transform.linear is not None:
            A = (~src_gbox.transform)*dst_gbox.transform
            warp_affine(pix, dst, A, resampling,
                        src_nodata=rdr.nodata, dst_nodata=dst_nodata)
        else:
            rio_reproject(pix, dst, src_gbox, dst_gbox, resampling,
                          src_nodata=rdr.nodata, dst_nodata=dst_nodata)

    return dst, rr.roi_dst

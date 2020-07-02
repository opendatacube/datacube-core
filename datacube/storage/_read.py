""" Dataset -> Raster
"""
from affine import Affine
import numpy as np
from typing import Tuple

from ..utils.math import is_almost_int, valid_mask

from ..utils.geometry import (
    roi_shape,
    roi_is_empty,
    roi_is_full,
    roi_pad,
    GeoBox,
    w_,
    warp_affine,
    rio_reproject,
    compute_reproject_roi)

from ..utils.geometry._warp import is_resampling_nn, Resampling, Nodata
from ..utils.geometry import gbox as gbx


def rdr_geobox(rdr) -> GeoBox:
    """ Construct GeoBox from opened dataset reader.
    """
    h, w = rdr.shape
    return GeoBox(w, h, rdr.transform, rdr.crs)


def can_paste(rr, stol=1e-3, ttol=1e-2):
    """
    Take result of compute_reproject_roi and check if can read(possibly with scale) and paste,
    or do we need to read then reproject.

    :returns: (True, None) if one can just read and paste
    :returns: (False, Reason) if pasting is not possible, so need to reproject after reading
    """
    if not rr.is_st:  # not linear or not Scale + Translation
        return False, "not ST"

    scale = rr.scale
    if not is_almost_int(scale, stol):  # non-integer scaling
        return False, "non-integer scale"

    scale = np.round(scale)
    A = rr.transform.linear           # src -> dst
    A = A*Affine.scale(scale, scale)  # src.overview[scale] -> dst

    (sx, _, tx,  # tx, ty are in dst pixel space
     _, sy, ty,
     *_) = A

    if any(abs(abs(s) - 1) > stol
           for s in (sx, sy)):  # not equal scaling across axis?
        return False, "sx!=sy, probably"

    ny, nx = (n/scale
              for n in roi_shape(rr.roi_src))

    # src_roi doesn't divide by scale properly:
    #  example 3x7 scaled down by factor of 2
    if not all(is_almost_int(n, stol) for n in (nx, ny)):
        return False, "src_roi doesn't align for scale"

    # TODO: probably need to deal with sub-pixel translation here, if we want
    # to ignore sub-pixel translation and dst roi is 1 pixel bigger than src it
    # should still be ok to paste after cropping dst roi by one pixel on the
    # appropriate side. As it stands sub-pixel translation will be ignored only
    # in some cases.

    # scaled down shape doesn't match dst shape
    s_shape = (int(ny), int(nx))
    if s_shape != roi_shape(rr.roi_dst):
        return False, "src_roi/scale != dst_roi"

    # final check: sub-pixel translation
    if not all(is_almost_int(t, ttol) for t in (tx, ty)):
        return False, "sub-pixel translation"

    return True, None


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
                    dst_nodata: Nodata) -> Tuple[slice, slice]:
    """ From opened reader object read into `dst`

    :returns: affected destination region
    """
    assert dst.shape == dst_gbox.shape
    src_gbox = rdr_geobox(rdr)

    rr = compute_reproject_roi(src_gbox, dst_gbox)

    if roi_is_empty(rr.roi_dst):
        return rr.roi_dst

    is_nn = is_resampling_nn(resampling)
    scale = pick_read_scale(rr.scale, rdr)

    paste_ok, _ = can_paste(rr, ttol=0.9 if is_nn else 0.01)

    def norm_read_args(roi, shape):
        if roi_is_full(roi, rdr.shape):
            roi = None

        if roi is None and shape == rdr.shape:
            shape = None

        return w_[roi], shape

    if paste_ok:
        A = rr.transform.linear
        sx, sy = A.a, A.e

        dst = dst[rr.roi_dst]
        pix = rdr.read(*norm_read_args(rr.roi_src, dst.shape))

        if sx < 0:
            pix = pix[:, ::-1]
        if sy < 0:
            pix = pix[::-1, :]

        if rdr.nodata is None:
            np.copyto(dst, pix)
        else:
            np.copyto(dst, pix, where=valid_mask(pix, rdr.nodata))
    else:
        if rr.is_st:
            # add padding on src/dst ROIs, it was set to tight bounds
            # TODO: this should probably happen inside compute_reproject_roi
            rr.roi_dst = roi_pad(rr.roi_dst, 1, dst_gbox.shape)
            rr.roi_src = roi_pad(rr.roi_src, 1, src_gbox.shape)

        dst = dst[rr.roi_dst]
        dst_gbox = dst_gbox[rr.roi_dst]
        src_gbox = src_gbox[rr.roi_src]
        if scale > 1:
            src_gbox = gbx.zoom_out(src_gbox, scale)

        pix = rdr.read(*norm_read_args(rr.roi_src, src_gbox.shape))

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
                       dst_nodata: Nodata) -> Tuple[np.ndarray,
                                                    Tuple[slice, slice]]:
    """ From opened reader object read into `dst`

    :returns: pixels read and ROI of dst_gbox that was affected
    """
    # pylint: disable=too-many-locals
    src_gbox = rdr_geobox(rdr)

    rr = compute_reproject_roi(src_gbox, dst_gbox)

    if roi_is_empty(rr.roi_dst):
        return None, rr.roi_dst

    is_nn = is_resampling_nn(resampling)
    scale = pick_read_scale(rr.scale, rdr)

    paste_ok, _ = can_paste(rr, ttol=0.9 if is_nn else 0.01)

    def norm_read_args(roi, shape):
        if roi_is_full(roi, rdr.shape):
            roi = None

        if roi is None and shape == rdr.shape:
            shape = None

        return roi, shape

    if paste_ok:
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
        if rr.is_st:
            # add padding on src/dst ROIs, it was set to tight bounds
            # TODO: this should probably happen inside compute_reproject_roi
            rr.roi_dst = roi_pad(rr.roi_dst, 1, dst_gbox.shape)
            rr.roi_src = roi_pad(rr.roi_src, 1, src_gbox.shape)

        dst_gbox = dst_gbox[rr.roi_dst]
        src_gbox = src_gbox[rr.roi_src]
        if scale > 1:
            src_gbox = gbx.zoom_out(src_gbox, scale)

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

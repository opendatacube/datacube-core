""" Dataset -> Raster
"""
from affine import Affine
import numpy as np

from datacube.utils.geometry import (
    roi_shape,
    GeoBox,
    compute_reproject_roi)


def rdr_geobox(rdr):
    """ Construct GeoBox from opened dataset reader.
    """
    h, w = rdr.shape
    return GeoBox(w, h, rdr.transform, rdr.crs)


def is_almost_int(x, tol):
    from math import fmod

    x = abs(fmod(x, 1))
    if x > 0.5:
        x = 1 - x
    return x < tol


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

    # scaled down shape doesn't match dst shape
    s_shape = (int(ny), int(nx))
    if s_shape != roi_shape(rr.roi_dst):
        return False, "src_roi/scale != dst_roi"

    # final check: sub-pixel translation
    if not all(is_almost_int(t, ttol) for t in (tx, ty)):
        return False, "sub-pixel translation"

    return True, None

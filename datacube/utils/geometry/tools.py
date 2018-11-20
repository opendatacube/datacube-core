import numpy as np
from . import GeoBox
from affine import Affine


def polygon_path(x, y=None):
    """A little bit like numpy.meshgrid, except returns only boundary values and
    limited to 2d case only.

    Examples:
      [0,1], [3,4] =>
      array([[0, 1, 1, 0, 0],
             [3, 3, 4, 4, 3]])

      [0,1] =>
      array([[0, 1, 1, 0, 0],
             [0, 0, 1, 1, 0]])
    """

    if y is None:
        y = x

    return np.vstack([
        np.vstack([x, np.full_like(x, y[0])]).T,
        np.vstack([np.full_like(y, x[-1]), y]).T[1:],
        np.vstack([x, np.full_like(x, y[-1])]).T[::-1][1:],
        np.vstack([np.full_like(y, x[0]), y]).T[::-1][1:]]).T


def gbox_boundary(gbox, pts_per_side=16):
    """Return points in pixel space along the perimeter of a GeoBox, or a 2d array.

    """
    H, W = gbox.shape[:2]
    xx = np.linspace(0, W, pts_per_side, dtype='float32')
    yy = np.linspace(0, H, pts_per_side, dtype='float32')

    return polygon_path(xx, yy).T[:-1]


def scaled_down_geobox(src_geobox, scaler: int):
    """Given a source geobox and integer scaler compute geobox of a scaled down image.

        Output geobox will be padded when shape is not a multiple of scaler.
        Example: 5x4, scaler=2 -> 3x2

        NOTE: here we assume that pixel coordinates are 0,0 at the top-left
              corner of a top-left pixel.

    """
    assert scaler > 1

    H, W = [X//scaler + (1 if X % scaler else 0)
            for X in src_geobox.shape]

    # Since 0,0 is at the corner of a pixel, not center, there is no
    # translation between pixel plane coords due to scaling
    A = src_geobox.transform * Affine.scale(scaler, scaler)

    return GeoBox(W, H, A, src_geobox.crs)


def align_down(x, align):
    return x - (x % align)


def align_up(x, align):
    return align_down(x+(align-1), align)


def scaled_down_roi(roi, scale: int):
    return tuple(slice(s.start//scale,
                       align_up(s.stop, scale)//scale) for s in roi)


def scaled_up_roi(roi, scale: int, shape=None):
    roi = tuple(slice(s.start*scale,
                      s.stop*scale) for s in roi)
    if shape is not None:
        roi = tuple(slice(min(dim, s.start),
                          min(dim, s.stop))
                    for s, dim in zip(roi, shape))
    return roi


def scaled_down_shape(shape, scale: int):
    return tuple(align_up(s, scale)//scale for s in shape)


def roi_shape(roi):
    def slice_dim(s):
        return s.stop if s.start is None else s.stop - s.start
    return tuple(slice_dim(s) for s in roi)


def roi_is_empty(roi):
    return any(d <= 0 for d in roi_shape(roi))

import numpy as np
from affine import Affine

from datacube.utils.geometry import (
    CRS,
    GeoBox,
    apply_affine,
)
from datacube.model import GridSpec

# pylint: disable=invalid-name

epsg4326 = CRS('EPSG:4326')
epsg3577 = CRS('EPSG:3577')
epsg3857 = CRS('EPSG:3857')

AlbersGS = GridSpec(crs=epsg3577,
                    tile_size=(100000.0, 100000.0),
                    resolution=(-25, 25),
                    origin=(0.0, 0.0))


def mkA(rot=0, scale=(1, 1), shear=0, translation=(0, 0)):
    return Affine.translation(*translation)*Affine.rotation(rot)*Affine.shear(shear)*Affine.scale(*scale)


def xy_from_gbox(gbox: GeoBox) -> (np.ndarray, np.ndarray):
    """
    :returns: Two images with X and Y coordinates for centers of pixels
    """
    h, w = gbox.shape

    xx, yy = np.meshgrid(np.arange(w, dtype='float64') + 0.5,
                         np.arange(h, dtype='float64') + 0.5)

    return apply_affine(gbox.transform, xx, yy)


def xy_norm(x: np.ndarray, y: np.ndarray,
            deg: float = 45.0) -> (np.ndarray, np.ndarray, Affine):
    """
    Transform output of xy_from_geobox with a reversible linear transform. On
    output x,y are in [0,1] range. Reversible Affine transform includes
    rotation by default, this is to ensure that test images don't have
    symmetries that are aligned to X/Y axis.

    1. Rotate x,y by ``deg``
    2. Offset and scale such that values are in [0, 1] range


    :returns: (x', y', A)

    - (x, y) == A*(x', y')
    - [x|y]'.min() == 0
    - [x|y]'.max() == 1

    """

    def norm_v(v):
        vmin = v.min()
        v -= vmin
        s = 1.0/v.max()
        v *= s

        return (s, -vmin*s)

    A_rot = Affine.rotation(deg)
    x, y = apply_affine(A_rot, x, y)

    sx, tx = norm_v(x)
    sy, ty = norm_v(y)

    A = Affine(sx, 0, tx,
               0, sy, ty)*A_rot

    return x, y, ~A


def to_fixed_point(a, dtype='uint16'):
    """
    Convert normalised ([0,1]) floating point image to integer fixed point fractional.

    Note for signed types: there is no offset, 0 -> 0, 1 -> (2**(nbits - 1) - 1).

    Reverse is provided by: ``from_fixed_point``
    """
    ii = np.iinfo(dtype)
    return (a*ii.max).astype(ii.dtype)


def from_fixed_point(a):
    """
    Convert fixed point image to floating point

    This is reverse of ``to_fixed_point``
    """
    ii = np.iinfo(a.dtype)
    return a.astype('float64')*(1.0/ii.max)

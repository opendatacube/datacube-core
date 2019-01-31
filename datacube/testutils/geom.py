import numpy as np
from affine import Affine
from typing import Callable, Union, Tuple

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

SAMPLE_WKT_WITHOUT_AUTHORITY = '''PROJCS["unnamed",
       GEOGCS["unnamed ellipse",
              DATUM["unknown",
                    SPHEROID["unnamed",6378137,0],
                    EXTENSION["PROJ4_GRIDS","@null"]],
              PRIMEM["Greenwich",0],
              UNIT["degree",0.0174532925199433]],
       PROJECTION["Mercator_2SP"],
       PARAMETER["standard_parallel_1",0],
       PARAMETER["central_meridian",0],
       PARAMETER["false_easting",0],
       PARAMETER["false_northing",0],
       UNIT["Meter",1]
]
'''


def mkA(rot=0, scale=(1, 1), shear=0, translation=(0, 0)):
    return Affine.translation(*translation)*Affine.rotation(rot)*Affine.shear(shear)*Affine.scale(*scale)


def xy_from_gbox(gbox: GeoBox) -> Tuple[np.ndarray, np.ndarray]:
    """
    :returns: Two images with X and Y coordinates for centers of pixels
    """
    h, w = gbox.shape

    xx, yy = np.meshgrid(np.arange(w, dtype='float64') + 0.5,
                         np.arange(h, dtype='float64') + 0.5)

    return apply_affine(gbox.transform, xx, yy)


def xy_norm(x: np.ndarray, y: np.ndarray,
            deg: float = 33.0) -> Tuple[np.ndarray, np.ndarray, Affine]:
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
    a = a*ii.max + 0.5
    a = np.clip(a, 0, ii.max, out=a)
    return a.astype(ii.dtype)


def from_fixed_point(a):
    """
    Convert fixed point image to floating point

    This is reverse of ``to_fixed_point``
    """
    ii = np.iinfo(a.dtype)
    return a.astype('float64')*(1.0/ii.max)


def gen_test_image_xy(gbox: GeoBox,
                      dtype: Union[str, np.dtype, type] = 'float32',
                      deg: float = 33.0) -> Tuple[np.ndarray, Callable]:
    """
    Generate test image that captures pixel coordinates in pixel values.
    Useful for testing reprojections/reads.

    :param gbox: GeoBox defining pixel plane

    :dtype: data type of the image, defaults to `float32`, but it can be an
            integer type in which case normalised coordinates will be
            quantised increasing error.

    :returns: 2xWxH ndarray encoding X,Y coordinates of pixel centers in some
              normalised space, and a callable that can convert from normalised
              space back to coordinate space.

    """
    dtype = np.dtype(dtype)

    x, y = xy_from_gbox(gbox)
    x, y, A = xy_norm(x, y, deg)

    xy = np.stack([x, y])

    if dtype.kind == 'f':
        xy = xy.astype(dtype)
    else:
        xy = to_fixed_point(xy, dtype)

    def denorm(xy=None, y=None, nodata=None):
        if xy is None:
            return A

        stacked = y is None
        x, y = xy if stacked else (xy, y)
        missing_mask = None

        if nodata is not None:
            if np.isnan(nodata):
                missing_mask = np.isnan(x) + np.isnan(y)
            else:
                missing_mask = (x == nodata) + (y == nodata)

        if x.dtype.kind != 'f':
            x = from_fixed_point(x)
            y = from_fixed_point(y)

        x, y = apply_affine(A, x, y)

        if missing_mask is not None:
            x[missing_mask] = np.nan
            y[missing_mask] = np.nan

        if stacked:
            return np.stack([x, y])
        else:
            return x, y

    return xy, denorm

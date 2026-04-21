# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2026 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import odc.geo.testutils as odc_geom
from odc.geo import CRS
from odc.geo.gridspec import GridSpec

TYPE_CHECKING = False
if TYPE_CHECKING:
    from collections.abc import Callable

    import numpy as np
    from affine import Affine
    from odc.geo.geobox import GeoBox

# pylint: disable=invalid-name

epsg4326 = CRS("EPSG:4326")
epsg3577 = CRS("EPSG:3577")
epsg3857 = CRS("EPSG:3857")

AlbersGS = GridSpec(crs=epsg3577, tile_shape=(4000, 4000), resolution=25)

SAMPLE_WKT_WITHOUT_AUTHORITY = """PROJCS["unnamed",
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
"""


def mkA(  # noqa: N802
    rot: float = 0.0,
    scale: tuple[int, int] = (1, 1),
    shear: float = 0.0,
    translation: tuple[float, float] = (0.0, 0.0),
) -> Affine:
    return odc_geom.mkA(rot, scale, shear, translation)


def xy_from_gbox(gbox: GeoBox) -> tuple[np.ndarray, np.ndarray]:
    """
    :returns: Two images with X and Y coordinates for centers of pixels
    """
    return odc_geom.xy_from_gbox(gbox)


def xy_norm(
    x: np.ndarray, y: np.ndarray, deg: float = 33.0
) -> tuple[np.ndarray, np.ndarray, Affine]:
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
    return odc_geom.xy_norm(x, y, deg)


def to_fixed_point(a, dtype: str | np.dtype | type = "uint16"):
    """
    Convert normalised ([0,1]) floating point image to integer fixed point fractional.

    Note for signed types: there is no offset, 0 -> 0, 1 -> (2**(nbits - 1) - 1).

    Reverse is provided by: ``from_fixed_point``
    """
    return odc_geom.to_fixed_point(a, dtype)


def from_fixed_point(a):
    """
    Convert fixed point image to floating point

    This is reverse of ``to_fixed_point``
    """
    return odc_geom.from_fixed_point(a)


def gen_test_image_xy(
    gbox: GeoBox, dtype: str | np.dtype | type = "float32", deg: float = 33.0
) -> tuple[np.ndarray, Callable]:
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
    return odc_geom.gen_test_image_xy(gbox, dtype, deg)

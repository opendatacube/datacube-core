import numpy as np
from affine import Affine

from datacube.utils.geometry import (
    CRS,
    GeoBox,
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
    from ..utils.geometry import apply_affine
    h, w = gbox.shape

    xx, yy = np.meshgrid(np.arange(w, dtype='float64') + 0.5,
                         np.arange(h, dtype='float64') + 0.5)

    return apply_affine(gbox.transform, xx, yy)

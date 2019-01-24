import numpy as np
from affine import Affine
import rasterio
from datacube.utils.geometry import warp_affine, rio_reproject, gbox as gbx
from datacube.utils.geometry._warp import resampling_s2rio, is_resampling_nn

from datacube.testutils.geom import (
    AlbersGS,
)


def test_rio_resampling_conversion():
    import pytest

    R = rasterio.warp.Resampling
    assert resampling_s2rio('nearest') == R.nearest
    assert resampling_s2rio('bilinear') == R.bilinear
    assert resampling_s2rio('Bilinear') == R.bilinear
    assert resampling_s2rio('mode') == R.mode
    assert resampling_s2rio('average') == R.average

    with pytest.raises(ValueError):
        resampling_s2rio('no_such_mode')

    # check is_resampling_nn
    assert is_resampling_nn('nearest') is True
    assert is_resampling_nn('Nearest') is True
    assert is_resampling_nn('average') is False
    assert is_resampling_nn('no_such_mode') is False

    assert is_resampling_nn(R.nearest) is True
    assert is_resampling_nn(0) is True
    assert is_resampling_nn(R.mode) is False


def test_warp():
    src = np.zeros((128, 256),
                   dtype='int16')

    src[10:20, 30:50] = 33

    dst = np.zeros_like(src)
    dst_ = warp_affine(src, dst, Affine.translation(+30, +10), resampling='nearest')
    assert dst_ is dst
    assert (dst[:10, :20] == 33).all()
    assert (dst[10:, :] == 0).all()
    assert (dst[:, 20:] == 0).all()

    # check GDAL int8 limitation work-around
    src = src.astype('int8')
    dst = np.zeros_like(src)
    dst_ = warp_affine(src, dst, Affine.translation(+30, +10), resampling='nearest')
    assert dst_ is dst
    assert (dst[:10, :20] == 33).all()
    assert (dst[10:, :] == 0).all()
    assert (dst[:, 20:] == 0).all()

    # check GDAL int8 limitation work-around, with no-data
    src = src.astype('int8')
    dst = np.zeros_like(src)
    dst_ = warp_affine(src, dst,
                       Affine.translation(+30, +10),
                       resampling='nearest',
                       src_nodata=0,
                       dst_nodata=-3)
    assert dst_ is dst
    assert (dst[:10, :20] == 33).all()
    assert (dst[10:, :] == -3).all()
    assert (dst[:, 20:] == -3).all()


def test_rio_reproject():
    src = np.zeros((128, 256),
                   dtype='int16')

    src[10:20, 30:50] = 33

    s_gbox = AlbersGS.tile_geobox((15, -40))[:src.shape[0], :src.shape[1]]

    dst = np.zeros_like(src)
    dst_ = rio_reproject(src, dst,
                         s_gbox,
                         gbx.translate_pix(s_gbox, 30, 10),
                         resampling='nearest')
    assert dst_ is dst
    assert (dst[:10, :20] == 33).all()
    assert (dst[10:, :] == 0).all()
    assert (dst[:, 20:] == 0).all()

    # check GDAL int8 limitation work-around
    src = src.astype('int8')
    dst = np.zeros_like(src)

    dst_ = rio_reproject(src, dst,
                         s_gbox,
                         gbx.translate_pix(s_gbox, 30, 10),
                         resampling='nearest')

    assert dst_ is dst
    assert (dst[:10, :20] == 33).all()
    assert (dst[10:, :] == 0).all()
    assert (dst[:, 20:] == 0).all()

    # check GDAL int8 limitation work-around, with no-data
    src = src.astype('int8')
    dst = np.zeros_like(src)
    dst_ = rio_reproject(src, dst,
                         s_gbox,
                         gbx.translate_pix(s_gbox, 30, 10),
                         src_nodata=0,
                         dst_nodata=-3,
                         resampling='nearest')
    assert dst_ is dst
    assert (dst[:10, :20] == 33).all()
    assert (dst[10:, :] == -3).all()
    assert (dst[:, 20:] == -3).all()

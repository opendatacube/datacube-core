from affine import Affine

from datacube.storage._read import can_paste, is_almost_int
from datacube.utils.geometry import compute_reproject_roi, GeoBox
from datacube.utils.geometry import gbox as gbx

from datacube.testutils.geom import (
    epsg3857,
    AlbersGS,
)


def test_is_almost_int():
    assert is_almost_int(1, 1e-10)
    assert is_almost_int(1.001, .1)
    assert is_almost_int(2 - 0.001, .1)
    assert is_almost_int(-1.001, .1)


def test_can_paste():
    src = AlbersGS.tile_geobox((17, -40))

    def check_true(dst, **kwargs):
        ok, reason = can_paste(compute_reproject_roi(src, dst), **kwargs)
        if not ok:
            assert ok is True, reason

    def check_false(dst, **kwargs):
        ok, reason = can_paste(compute_reproject_roi(src, dst), **kwargs)
        if ok:
            assert ok is False, "Expected can_paste to return False, but got True"

    check_true(gbx.pad(src, 100))
    check_true(src[:10, :10])
    check_true(gbx.translate_pix(src, 0.1, 0.3), ttol=0.5)
    check_true(gbx.translate_pix(src, 3, -4))
    check_true(gbx.flipx(src))
    check_true(gbx.flipy(src))
    check_true(gbx.flipx(gbx.flipy(src)))
    check_true(gbx.zoom_out(src, 2))
    check_true(gbx.zoom_out(src, 4))
    check_true(gbx.zoom_out(src[:9, :9], 3))

    # Check False code paths
    dst = GeoBox.from_geopolygon(src.extent.to_crs(epsg3857).buffer(10),
                                 resolution=src.resolution)
    check_false(dst)  # non ST
    check_false(gbx.affine_transform_pix(src, Affine.rotation(1)))  # non ST

    check_false(gbx.zoom_out(src, 1.9))   # non integer scale
    check_false(gbx.affine_transform_pix(src, Affine.scale(1, 2)))  # sx != sy

    dst = gbx.translate_pix(src, -1, -1)
    dst = gbx.zoom_out(dst, 2)
    check_false(dst)  # src_roi doesn't align for scale

    check_false(dst, stol=0.7)  # src_roi/scale != dst_roi
    check_false(gbx.translate_pix(src, 0.3, 0.4))  # sub-pixel translation

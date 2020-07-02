from affine import Affine
import numpy as np

from datacube.storage._read import (
    can_paste,
    read_time_slice,
    read_time_slice_v2,
    pick_read_scale,
    rdr_geobox)

from datacube.testutils.io import RasterFileDataSource
from datacube.utils.geometry import (
    compute_reproject_roi,
    GeoBox,
    roi_shape,
    roi_is_empty,
)

from datacube.utils.geometry import gbox as gbx

from datacube.testutils.io import (
    rio_slurp
)

from datacube.testutils.geom import (
    epsg3857,
    AlbersGS,
)


def test_pick_read_scale():
    assert pick_read_scale(0.7) == 1
    assert pick_read_scale(1.3) == 1
    assert pick_read_scale(2.3) == 2
    assert pick_read_scale(1.99999) == 2


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

    check_false(gbx.zoom_out(src, 1.9))   # non integer scale
    check_false(gbx.affine_transform_pix(src, Affine.scale(1, 2)))  # sx != sy

    dst = gbx.translate_pix(src, -1, -1)
    dst = gbx.zoom_out(dst, 2)
    check_false(dst)  # src_roi doesn't align for scale

    check_false(dst, stol=0.7)  # src_roi/scale != dst_roi
    check_false(gbx.translate_pix(src, 0.3, 0.4))  # sub-pixel translation


def test_read_paste(tmpdir):
    from datacube.testutils import mk_test_image
    from datacube.testutils.io import write_gtiff
    from pathlib import Path

    pp = Path(str(tmpdir))

    xx = mk_test_image(128, 64, nodata=None)
    assert (xx != -999).all()

    mm = write_gtiff(pp/'tst-read-paste-128x64-int16.tif', xx, nodata=None)

    def _read(gbox, resampling='nearest',
              fallback_nodata=-999,
              dst_nodata=-999,
              check_paste=False):
        with RasterFileDataSource(mm.path, 1, nodata=fallback_nodata).open() as rdr:
            if check_paste:
                # check that we are using paste
                paste_ok, reason = can_paste(compute_reproject_roi(rdr_geobox(rdr), gbox))
                assert paste_ok is True, reason

            yy = np.full(gbox.shape, dst_nodata, dtype=rdr.dtype)
            roi = read_time_slice(rdr, yy, gbox, resampling, dst_nodata)
            return yy, roi

    # read native whole
    yy, roi = _read(mm.gbox)
    np.testing.assert_array_equal(xx, yy)
    assert roi == np.s_[0:64, 0:128]

    # read native whole, no nodata case
    yy, roi = _read(mm.gbox, fallback_nodata=None)
    np.testing.assert_array_equal(xx, yy)
    assert roi == np.s_[0:64, 0:128]

    # read native whole, ignoring small sub-pixel translation
    yy, roi = _read(gbx.translate_pix(mm.gbox, 0.3, -0.4), fallback_nodata=-33)
    np.testing.assert_array_equal(xx, yy)
    assert roi == np.s_[0:64, 0:128]

    # no overlap between src and dst
    yy, roi = _read(gbx.translate_pix(mm.gbox, 10000, -10000))
    assert roi_is_empty(roi)

    # read with Y flipped
    yy, roi = _read(gbx.flipy(mm.gbox))
    np.testing.assert_array_equal(xx[::-1, :], yy)
    assert roi == np.s_[0:64, 0:128]

    # read with X flipped
    yy, roi = _read(gbx.flipx(mm.gbox))
    np.testing.assert_array_equal(xx[:, ::-1], yy)
    assert roi == np.s_[0:64, 0:128]

    # read with X and Y flipped
    yy, roi = _read(gbx.flipy(gbx.flipx(mm.gbox)))
    assert roi == np.s_[0:64, 0:128]
    np.testing.assert_array_equal(xx[::-1, ::-1], yy[roi])

    # dst is fully inside src
    sroi = np.s_[10:19, 31:47]
    yy, roi = _read(mm.gbox[sroi])
    np.testing.assert_array_equal(xx[sroi], yy[roi])

    # partial overlap
    yy, roi = _read(gbx.translate_pix(mm.gbox, -3, -10))
    assert roi == np.s_[10:64, 3:128]
    np.testing.assert_array_equal(xx[:-10, :-3], yy[roi])
    assert (yy[:10, :] == -999).all()
    assert (yy[:, :3] == -999).all()

    # scaling paste
    yy, roi = _read(gbx.zoom_out(mm.gbox, 2), check_paste=True)
    assert roi == np.s_[0:32, 0:64]
    np.testing.assert_array_equal(xx[1::2, 1::2], yy)


def test_read_with_reproject(tmpdir):
    from datacube.testutils import mk_test_image
    from datacube.testutils.io import write_gtiff
    from pathlib import Path

    pp = Path(str(tmpdir))

    xx = mk_test_image(128, 64, nodata=None)
    assert (xx != -999).all()
    tile = AlbersGS.tile_geobox((17, -40))[:64, :128]

    mm = write_gtiff(pp/'tst-read-with-reproject-128x64-int16.tif', xx,
                     crs=str(tile.crs),
                     resolution=tile.resolution[::-1],
                     offset=tile.transform*(0, 0),
                     nodata=-999)
    assert mm.gbox == tile

    def _read(gbox,
              resampling='nearest',
              fallback_nodata=None,
              dst_nodata=-999):
        with RasterFileDataSource(mm.path, 1, nodata=fallback_nodata).open() as rdr:
            yy = np.full(gbox.shape, dst_nodata, dtype=rdr.dtype)
            roi = read_time_slice(rdr, yy, gbox, resampling, dst_nodata)
            return yy, roi

    gbox = gbx.pad(mm.gbox, 10)
    gbox = gbx.zoom_out(gbox, 0.873)
    yy, roi = _read(gbox)

    assert roi[0].start > 0 and roi[1].start > 0
    assert (yy[0] == -999).all()

    yy_expect, _ = rio_slurp(mm.path, gbox)
    np.testing.assert_array_equal(yy, yy_expect)

    gbox = gbx.zoom_out(mm.gbox[3:-3, 10:-10], 2.1)
    yy, roi = _read(gbox)

    assert roi_shape(roi) == gbox.shape
    assert not (yy == -999).any()

    gbox = GeoBox.from_geopolygon(mm.gbox.extent.to_crs(epsg3857).buffer(50),
                                  resolution=mm.gbox.resolution)

    assert gbox.extent.contains(mm.gbox.extent.to_crs(epsg3857))
    assert gbox.crs != mm.gbox.crs
    yy, roi = _read(gbox)
    assert roi[0].start > 0 and roi[1].start > 0
    assert (yy[0] == -999).all()

    gbox = gbx.zoom_out(gbox, 4)
    yy, roi = _read(gbox, resampling='average')
    nvalid = (yy != -999).sum()
    nempty = (yy == -999).sum()
    assert nvalid > nempty


def test_read_paste_v2(tmpdir):
    from datacube.testutils import mk_test_image
    from datacube.testutils.io import write_gtiff
    from datacube.testutils.iodriver import open_reader
    from pathlib import Path

    pp = Path(str(tmpdir))

    xx = mk_test_image(128, 64, nodata=None)
    assert (xx != -999).all()

    mm = write_gtiff(pp/'tst-read-paste-128x64-int16.tif', xx, nodata=None)

    def _read(gbox, resampling='nearest',
              fallback_nodata=-999,
              dst_nodata=-999,
              check_paste=False):

        rdr = open_reader(mm.path,
                          nodata=fallback_nodata)
        if check_paste:
            # check that we are using paste
            paste_ok, reason = can_paste(compute_reproject_roi(rdr_geobox(rdr), gbox))
            assert paste_ok is True, reason

        yy = np.full(gbox.shape, dst_nodata, dtype=rdr.dtype)
        yy_, roi = read_time_slice_v2(rdr, gbox, resampling, dst_nodata)
        yy[roi] = yy_
        return yy, roi

    # read native whole
    yy, roi = _read(mm.gbox)
    np.testing.assert_array_equal(xx, yy)
    assert roi == np.s_[0:64, 0:128]

    # read native whole, no nodata case
    yy, roi = _read(mm.gbox, fallback_nodata=None)
    np.testing.assert_array_equal(xx, yy)
    assert roi == np.s_[0:64, 0:128]

    # read native whole, ignoring small sub-pixel translation
    yy, roi = _read(gbx.translate_pix(mm.gbox, 0.3, -0.4), fallback_nodata=-33)
    np.testing.assert_array_equal(xx, yy)
    assert roi == np.s_[0:64, 0:128]

    # no overlap between src and dst
    yy, roi = _read(gbx.translate_pix(mm.gbox, 10000, -10000))
    assert roi_is_empty(roi)

    # read with Y flipped
    yy, roi = _read(gbx.flipy(mm.gbox))
    np.testing.assert_array_equal(xx[::-1, :], yy)
    assert roi == np.s_[0:64, 0:128]

    # read with X flipped
    yy, roi = _read(gbx.flipx(mm.gbox))
    np.testing.assert_array_equal(xx[:, ::-1], yy)
    assert roi == np.s_[0:64, 0:128]

    # read with X and Y flipped
    yy, roi = _read(gbx.flipy(gbx.flipx(mm.gbox)))
    assert roi == np.s_[0:64, 0:128]
    np.testing.assert_array_equal(xx[::-1, ::-1], yy[roi])

    # dst is fully inside src
    sroi = np.s_[10:19, 31:47]
    yy, roi = _read(mm.gbox[sroi])
    np.testing.assert_array_equal(xx[sroi], yy[roi])

    # partial overlap
    yy, roi = _read(gbx.translate_pix(mm.gbox, -3, -10))
    assert roi == np.s_[10:64, 3:128]
    np.testing.assert_array_equal(xx[:-10, :-3], yy[roi])
    assert (yy[:10, :] == -999).all()
    assert (yy[:, :3] == -999).all()

    # scaling paste
    yy, roi = _read(gbx.zoom_out(mm.gbox, 2), check_paste=True)
    assert roi == np.s_[0:32, 0:64]
    np.testing.assert_array_equal(xx[1::2, 1::2], yy)


def test_read_with_reproject_v2(tmpdir):
    from datacube.testutils import mk_test_image
    from datacube.testutils.io import write_gtiff
    from datacube.testutils.iodriver import open_reader
    from pathlib import Path

    pp = Path(str(tmpdir))

    xx = mk_test_image(128, 64, nodata=None)
    assert (xx != -999).all()
    tile = AlbersGS.tile_geobox((17, -40))[:64, :128]

    def _read(gbox, resampling='nearest',
              fallback_nodata=-999,
              dst_nodata=-999):

        rdr = open_reader(mm.path,
                          nodata=fallback_nodata)

        yy = np.full(gbox.shape, dst_nodata, dtype=rdr.dtype)
        yy_, roi = read_time_slice_v2(rdr, gbox, resampling, dst_nodata)
        yy[roi] = yy_
        return yy, roi

    mm = write_gtiff(pp/'tst-read-with-reproject-128x64-int16.tif', xx,
                     crs=str(tile.crs),
                     resolution=tile.resolution[::-1],
                     offset=tile.transform*(0, 0),
                     nodata=-999)
    assert mm.gbox == tile

    gbox = gbx.pad(mm.gbox, 10)
    gbox = gbx.zoom_out(gbox, 0.873)
    yy, roi = _read(gbox)

    assert roi[0].start > 0 and roi[1].start > 0
    assert (yy[0] == -999).all()

    yy_expect, _ = rio_slurp(mm.path, gbox)
    np.testing.assert_array_equal(yy, yy_expect)

    gbox = gbx.zoom_out(mm.gbox[3:-3, 10:-10], 2.1)
    yy, roi = _read(gbox)

    assert roi_shape(roi) == gbox.shape
    assert not (yy == -999).any()

    gbox = GeoBox.from_geopolygon(mm.gbox.extent.to_crs(epsg3857).buffer(50),
                                  resolution=mm.gbox.resolution)

    assert gbox.extent.contains(mm.gbox.extent.to_crs(epsg3857))
    assert gbox.crs != mm.gbox.crs
    yy, roi = _read(gbox)
    assert roi[0].start > 0 and roi[1].start > 0
    assert (yy[0] == -999).all()

    gbox = gbx.zoom_out(gbox, 4)
    yy, roi = _read(gbox, resampling='average')
    nvalid = (yy != -999).sum()
    nempty = (yy == -999).sum()
    assert nvalid > nempty

from affine import Affine
import numpy as np
import pytest
from datacube.utils.geometry import gbox as gbx
from datacube.utils import geometry
from datacube.utils.geometry import GeoBox

epsg3857 = geometry.CRS('EPSG:3857')


def test_gbox_ops():
    s = GeoBox(1000, 100, Affine(10, 0, 12340, 0, -10, 316770), epsg3857)
    assert s.shape == (100, 1000)

    d = gbx.flipy(s)
    assert d.shape == s.shape
    assert d.crs is s.crs
    assert d.resolution == (-s.resolution[0], s.resolution[1])
    assert d.extent.contains(s.extent)

    d = gbx.flipx(s)
    assert d.shape == s.shape
    assert d.crs is s.crs
    assert d.resolution == (s.resolution[0], -s.resolution[1])
    assert d.extent.contains(s.extent)

    assert gbx.flipy(gbx.flipy(s)).affine == s.affine
    assert gbx.flipx(gbx.flipx(s)).affine == s.affine

    d = gbx.zoom_out(s, 2)
    assert d.shape == (50, 500)
    assert d.crs is s.crs
    assert d.extent.contains(s.extent)
    assert d.resolution == (s.resolution[0]*2, s.resolution[1]*2)

    d = gbx.zoom_out(s, 2*max(s.shape))
    assert d.shape == (1, 1)
    assert d.crs is s.crs
    assert d.extent.contains(s.extent)

    d = gbx.zoom_out(s, 1.33719)
    assert d.crs is s.crs
    assert d.extent.contains(s.extent)
    assert all(ds < ss for ds, ss in zip(d.shape, s.shape))

    d = gbx.zoom_to(s, s.shape)
    assert d == s

    d = gbx.zoom_to(s, (1, 3))
    assert d.shape == (1, 3)
    assert d.extent == s.extent

    d = gbx.zoom_to(s, (10000, 10000))
    assert d.shape == (10000, 10000)
    assert d.extent == s.extent

    d = gbx.pad(s, 1)
    assert d.crs is s.crs
    assert d.resolution == s.resolution
    assert d.extent.contains(s.extent)
    assert s.extent.contains(d.extent) is False
    assert d[1:-1, 1:-1].affine == s.affine
    assert d[1:-1, 1:-1].shape == s.shape

    d = gbx.translate_pix(s, 1, 2)
    assert d.crs is s.crs
    assert d.resolution == s.resolution
    assert d.extent != s.extent
    assert s[2:3, 1:2].extent == d[:1, :1].extent

    d = gbx.translate_pix(s, -10, -2)
    assert d.crs is s.crs
    assert d.resolution == s.resolution
    assert d.extent != s.extent
    assert s[:1, :1].extent == d[2:3, 10:11].extent

    d = gbx.translate_pix(s, 0.1, 0)
    assert d.crs is s.crs
    assert d.shape == s.shape
    assert d.resolution == s.resolution
    assert d.extent != s.extent
    assert d.extent.contains(s[:, 1:].extent)

    d = gbx.translate_pix(s, 0, -0.5)
    assert d.crs is s.crs
    assert d.shape == s.shape
    assert d.resolution == s.resolution
    assert d.extent != s.extent
    assert s.extent.contains(d[1:, :].extent)

    d = gbx.affine_transform_pix(s, Affine(1, 0, 0,
                                           0, 1, 0))
    assert d.crs is s.crs
    assert d.shape == s.shape
    assert d.resolution == s.resolution
    assert d.extent == s.extent

    d = gbx.affine_transform_pix(s, Affine.rotation(10))
    assert d.crs is s.crs
    assert d.shape == s.shape
    assert d.extent != s.extent

    for deg in (33, -33, 20, 90, 180):
        d = gbx.rotate(s, 33)
        assert d.crs is s.crs
        assert d.shape == s.shape
        assert d.extent != s.extent
        np.testing.assert_almost_equal(d.extent.area, s.extent.area, 1e-5)
        assert s[49:52, 499:502].extent.contains(d[50:51, 500:501].extent), "Check that center pixel hasn't moved"


def test_gbox_tiles():
    A = Affine.identity()
    H, W = (300, 200)
    h, w = (10, 20)
    gbox = GeoBox(W, H, A, epsg3857)
    tt = gbx.GeoboxTiles(gbox, (h, w))
    assert tt.shape == (300/10, 200/20)

    assert tt[0, 0] == gbox[0:h, 0:w]
    assert tt[0, 1] == gbox[0:h, w:w+w]

    assert tt[0, 0] is tt[0, 0]  # Should cache exact same object
    assert tt[4, 1].shape == (h, w)

    H, W = (11, 22)
    h, w = (10, 9)
    gbox = GeoBox(W, H, A, epsg3857)
    tt = gbx.GeoboxTiles(gbox, (h, w))
    assert tt.shape == (2, 3)
    assert tt[1, 2] == gbox[10:11, 18:22]

    with pytest.raises(IndexError):
        tt[tt.shape]

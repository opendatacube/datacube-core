import pytest
from datacube.testutils.geom import epsg4326, epsg3857
from datacube.testutils import mk_sample_xr_dataset, remove_crs
from datacube.utils.xarray_geoextensions import (
    _norm_crs,
    _xarray_affine,
    _xarray_geobox,
    _xarray_extent,
)


def test_xr_extension(odc_style_xr_dataset):
    xx = odc_style_xr_dataset
    assert _norm_crs(None) is None
    assert _norm_crs(epsg4326) is epsg4326
    assert _norm_crs(str(epsg4326)) == epsg4326

    with pytest.raises(ValueError):
        _norm_crs([])

    assert xx.geobox.shape == xx.B10.shape

    (sx, zz0, tx, zz1, sy, ty) = xx.affine[:6]
    assert (zz0, zz1) == (0, 0)

    xx = remove_crs(xx)

    assert _xarray_geobox(xx) is None
    assert _xarray_extent(xx) is None

    # affine should still be valid
    A = _xarray_affine(xx)
    assert A is not None
    assert A*(0.5, 0.5) == (xx.longitude[0], xx.latitude[0])
    assert A*(0.5, 1.5) == (xx.longitude[0], xx.latitude[1])


def test_xr_geobox():
    xy = (10, 111)
    rxy = (10, -100)
    resolution = rxy[::-1]

    ds = mk_sample_xr_dataset(crs=epsg3857, xy=xy, resolution=resolution)

    assert ds.geobox.crs == epsg3857
    assert ds.band.geobox.crs == epsg3857
    assert ds.band.affine*(0, 0) == xy
    assert ds.band.affine*(1, 1) == tuple(a+b for a, b in zip(xy, rxy))

    assert ds.band[:, :2, :2].affine*(0, 0) == xy
    assert ds.band[:, :2, :2].affine*(1, 1) == tuple(a+b for a, b in zip(xy, rxy))

    xx = ds.band + 1000
    assert xx.geobox is not None
    assert xx.geobox == ds.band.geobox

    assert mk_sample_xr_dataset(crs=epsg4326).geobox.crs == epsg4326
    assert mk_sample_xr_dataset(crs=epsg4326).band.geobox.crs == epsg4326

    assert mk_sample_xr_dataset(crs=None).geobox is None
    assert mk_sample_xr_dataset(crs=None).affine is not None

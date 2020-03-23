import pytest
from datacube.utils.xarray_geoextensions import (
    _norm_crs,
    _xarray_affine,
    _xarray_geobox,
    _xarray_extent,
)
from datacube.testutils.geom import epsg4326


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

    xx.attrs['crs'] = None
    xx.B10.attrs['crs'] = None
    for dim in xx.B10.dims:
        xx[dim].attrs['crs'] = None
    assert _xarray_geobox(xx) is None
    assert _xarray_extent(xx) is None

    # affine should still be valid
    A = _xarray_affine(xx)
    assert A is not None
    assert A*(0.5, 0.5) == (xx.longitude[0], xx.latitude[0])
    assert A*(0.5, 1.5) == (xx.longitude[0], xx.latitude[1])

import pytest
from datacube.utils.xarray_geoextensions import (
    _norm_crs,
    _xarray_affine,
    _xarray_geobox,
    _xarray_extent,
)
from datacube.testutils.geom import epsg4326


def test_xr_extension(odc_style_xr_dataset):
    assert _norm_crs(None) is None
    assert _norm_crs(epsg4326) is epsg4326
    assert _norm_crs(str(epsg4326)) == epsg4326

    with pytest.raises(ValueError):
        _norm_crs([])

    assert odc_style_xr_dataset.geobox.shape == odc_style_xr_dataset.B10.shape

    (sx, zz0, tx, zz1, sy, ty) = odc_style_xr_dataset.affine[:6]
    assert (zz0, zz1) == (0, 0)

    odc_style_xr_dataset.attrs['crs'] = None
    for dim in odc_style_xr_dataset.B10.dims:
        odc_style_xr_dataset[dim].attrs['crs'] = None
    assert _xarray_affine(odc_style_xr_dataset) is None
    assert _xarray_geobox(odc_style_xr_dataset) is None
    assert _xarray_extent(odc_style_xr_dataset) is None

import pytest
import xarray as xr
from datacube.testutils.geom import epsg4326, epsg3857
from datacube.testutils import mk_sample_xr_dataset, remove_crs
from datacube.utils.xarray_geoextensions import (
    _norm_crs,
    _xarray_affine,
    _xarray_geobox,
    _xarray_extent,
    _get_crs_from_attrs,
    _get_crs_from_coord,
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
    assert ds.band.isel(time=0, x=0).affine is None

    xx = ds.band + 1000
    assert xx.geobox is not None
    assert xx.geobox == ds.band.geobox

    assert mk_sample_xr_dataset(crs=epsg4326).geobox.crs == epsg4326
    assert mk_sample_xr_dataset(crs=epsg4326).band.geobox.crs == epsg4326

    assert mk_sample_xr_dataset(crs=None).geobox is None
    assert mk_sample_xr_dataset(crs=None).affine is not None


def test_xr_geobox_unhappy():
    xx = mk_sample_xr_dataset(crs=None)

    # test that exceptions from get_crs_from_{coord,attrs} are caught
    xx.band.attrs.update(grid_mapping='x')  # force exception in coord
    xx.x.attrs.update(crs='EPSG:4326')      # force exception in attr
    xx.y.attrs.update(crs='EPSG:3857')
    assert xx.band.geobox is None

    # test _norm_crs exception is caught
    xx = mk_sample_xr_dataset(crs=None)
    xx.attrs['crs'] = ['this will not parse']
    assert xx.geobox is None


def test_crs_from_coord():
    xx_none = mk_sample_xr_dataset(crs=None)
    xx_3857 = mk_sample_xr_dataset(crs=epsg3857)
    xx_4326 = mk_sample_xr_dataset(crs=epsg4326)
    cc_4326 = xx_4326.geobox.xr_coords(with_crs='epsg_4326')['epsg_4326']
    cc_3857 = xx_3857.geobox.xr_coords(with_crs='epsg_3857')['epsg_3857']

    assert _get_crs_from_coord(xx_none.band) is None
    assert _get_crs_from_coord(xx_none) is None
    assert _get_crs_from_coord(xx_3857.band) == epsg3857
    assert _get_crs_from_coord(xx_3857) == epsg3857
    assert _get_crs_from_coord(xx_4326.band) == epsg4326
    assert _get_crs_from_coord(xx_4326) == epsg4326

    xx = xx_none.band.assign_attrs(grid_mapping='x')
    with pytest.raises(ValueError):
        _get_crs_from_coord(xx)
    xx = xx_none.band.assign_attrs(grid_mapping='no_such_coord')
    assert _get_crs_from_coord(xx) is None

    xx_2crs = xx_none.assign_coords(cc_4326=cc_4326, cc_3857=cc_3857)
    assert xx_2crs.geobox is None

    # two coords, no grid mapping, strict mode
    with pytest.raises(ValueError):
        _get_crs_from_coord(xx_2crs)
    with pytest.raises(ValueError):
        _get_crs_from_coord(xx_2crs.band)

    # any should just return "first" guess, we not sure which one
    crs = _get_crs_from_coord(xx_2crs, 'any')
    assert epsg4326 == crs or epsg3857 == crs

    # all should return a list of length 2
    crss = _get_crs_from_coord(xx_2crs, 'all')
    assert len(crss) == 2
    assert any(crs == epsg3857 for crs in crss)
    assert any(crs == epsg4326 for crs in crss)

    with pytest.raises(ValueError):
        _get_crs_from_coord(xx_2crs, 'no-such-mode')


def test_crs_from_attrs():
    xx_none = mk_sample_xr_dataset(crs=None)
    xx_3857 = mk_sample_xr_dataset(crs=epsg3857)
    xx_4326 = mk_sample_xr_dataset(crs=epsg4326)

    assert _get_crs_from_attrs(xx_none) is None
    assert _get_crs_from_attrs(xx_none.band) is None
    assert _get_crs_from_attrs(xx_3857.band) == epsg3857
    assert _get_crs_from_attrs(xx_3857) == epsg3857
    assert _get_crs_from_attrs(xx_4326.band) == epsg4326
    assert _get_crs_from_attrs(xx_4326) == epsg4326

    assert _get_crs_from_attrs(xr.Dataset()) is None

    # check inconsistent CRSs
    xx = xx_3857.copy()
    xx.x.attrs['crs'] = xx_4326.attrs['crs']
    with pytest.raises(ValueError):
        _get_crs_from_attrs(xx)

""" Tests for new RIO reader driver
"""
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, Future
import numpy as np
import rasterio
from affine import Affine
import pytest
import warnings

from datacube.drivers.rio._reader import (
    RDEntry,
    _dc_crs,
    _rio_uri,
    _rio_band_idx,
    _roi_to_window,
)
from datacube.testutils.geom import SAMPLE_WKT_WITHOUT_AUTHORITY, epsg3857
from datacube.testutils.iodriver import (
    NetCDF, GeoTIFF, mk_band, mk_rio_driver, open_reader
)


def test_rio_rd_entry():
    rde = RDEntry()

    assert 'file' in rde.protocols
    assert 's3' in rde.protocols

    assert GeoTIFF in rde.formats
    assert NetCDF in rde.formats

    assert rde.supports('file', NetCDF) is True
    assert rde.supports('s3', NetCDF) is False

    assert rde.supports('file', GeoTIFF) is True
    assert rde.supports('s3', GeoTIFF) is True

    assert rde.new_instance({}) is not None
    assert rde.new_instance({'max_workers': 2}) is not None

    with pytest.raises(ValueError):
        rde.new_instance({'pool': []})

    # check pool re-use
    pool = ThreadPoolExecutor(max_workers=1)
    rdr = rde.new_instance({'pool': pool})
    assert rdr._pool is pool


def test_rd_internals_crs():
    from rasterio.crs import CRS as RioCRS

    assert _dc_crs(None) is None
    assert _dc_crs(RioCRS()) is None
    assert _dc_crs(RioCRS.from_epsg(3857)).epsg == 3857
    assert _dc_crs(RioCRS.from_wkt(SAMPLE_WKT_WITHOUT_AUTHORITY)).epsg is None


def test_rd_internals_roi():
    s_ = np.s_

    assert _roi_to_window(None, (1, 1)) is None
    assert _roi_to_window(s_[:, :], (1, 10)) == ((0, 1), (0, 10))
    assert _roi_to_window(s_[1:, -1:], (5, 10)) == ((1, 5), (9, 10))
    assert _roi_to_window(s_[:3, 3:-1], (5, 10)) == ((0, 3), (3, 9))


def test_rd_internals_bidx(data_folder):
    base = "file://" + str(data_folder) + "/metadata.yml"
    bi = mk_band('a',
                 base,
                 path="multi_doc.nc",
                 format=NetCDF,
                 timestamp=datetime.utcfromtimestamp(1),
                 layer='a')
    assert bi.uri.endswith('multi_doc.nc')

    rio_fname = _rio_uri(bi)
    assert rio_fname.startswith('NETCDF:')

    with rasterio.open(rio_fname) as src:
        # timestamp search was removed
        with pytest.raises(DeprecationWarning):
            _rio_band_idx(bi, src)

        # extract from .uri
        bi.uri = bi.uri + "#part=5"
        assert _rio_band_idx(bi, src) == 5

        # extract from .band
        bi.band = 33
        assert _rio_band_idx(bi, src) == 33

    bi = mk_band('a', base, path="test.tif", format=GeoTIFF)

    with rasterio.open(_rio_uri(bi), 'r') as src:
        # should default to 1
        assert _rio_band_idx(bi, src) == 1

        # layer containing int should become index
        bi = mk_band('a', base, path="test.tif", format=GeoTIFF, layer=2)
        assert _rio_band_idx(bi, src) == 2

        # band is the keyword
        bi = mk_band('a', base, path="test.tif", format=GeoTIFF, band=3)
        assert _rio_band_idx(bi, src) == 3

    # TODO: make single time-slice netcdf, for now pretend that this tiff is netcdf
    with rasterio.open(str(data_folder)+"/sample_tile_151_-29.tif", 'r') as src:
        bi = mk_band('a', base, path='sample_tile_151_-29.tif', format=NetCDF)
        assert src.count == 1
        assert _rio_band_idx(bi, src) == 1


def test_rd_internals_uri():
    base = "file:///some/path/"

    bi = mk_band('green', base, path="f.tiff", format=GeoTIFF)
    assert _rio_uri(bi) == '/some/path/f.tiff'

    bi = mk_band('x', base, path="x.nc", layer='x', format=NetCDF)
    assert _rio_uri(bi) == 'NETCDF:"/some/path/x.nc":x'

    bi = mk_band('jj', 's3://some/path/config.yml', "jj.tiff")
    assert _rio_uri(bi) == 's3://some/path/jj.tiff'
    assert _rio_uri(bi) is bi.uri


def test_rio_driver_fail_to_open():
    nosuch_uri = 'file:///this-file-hopefully/doesnot/exist-4718193.tiff'
    rde = RDEntry()
    rdr = rde.new_instance({})

    assert rdr is not None

    load_ctx = rdr.new_load_context(iter([]), None)
    load_ctx = rdr.new_load_context(iter([]), load_ctx)

    bi = mk_band('green', nosuch_uri)
    assert bi.uri == nosuch_uri
    fut = rdr.open(bi, load_ctx)

    assert isinstance(fut, Future)

    with pytest.raises(IOError):
        fut.result()


def test_rio_driver_open(data_folder):
    base = "file://" + str(data_folder) + "/metadata.yml"

    rdr = mk_rio_driver()
    assert rdr is not None

    load_ctx = rdr.new_load_context(iter([]), None)
    bi = mk_band('b1', base, path="test.tif", format=GeoTIFF)
    load_ctx = rdr.new_load_context(iter([bi]), load_ctx)
    fut = rdr.open(bi, load_ctx)
    assert isinstance(fut, Future)

    src = fut.result()
    assert src.crs is not None
    assert src.transform is not None
    assert src.crs.epsg == 4326
    assert src.shape == (2000, 4000)
    assert src.nodata == -999
    assert src.dtype == np.dtype(np.int16)

    xx = src.read().result()
    assert xx.shape == src.shape
    assert xx.dtype == src.dtype

    # check overrides
    bi = mk_band('b1', base, path="zeros_no_geo_int16_7x3.tif", format=GeoTIFF, nodata=None)

    # First verify that missing overrides in the band doesn't cause issues
    assert bi.crs is None
    assert bi.transform is None
    assert bi.nodata is None

    load_ctx = rdr.new_load_context(iter([bi]), load_ctx)
    with warnings.catch_warnings():
        warnings.simplefilter('ignore', rasterio.errors.NotGeoreferencedWarning)
        src = rdr.open(bi, load_ctx).result()

    assert src.crs is None
    assert src.transform is None
    assert src.nodata is None

    # Now test that overrides work
    bi.crs = epsg3857
    bi.transform = Affine.translation(10, 100)
    bi.nodata = -33

    load_ctx = rdr.new_load_context(iter([bi]), load_ctx)
    with warnings.catch_warnings():
        warnings.simplefilter('ignore', rasterio.errors.NotGeoreferencedWarning)
        src = rdr.open(bi, load_ctx).result()

    assert src.crs == bi.crs
    assert src.transform == bi.transform
    assert src.nodata == bi.nodata


def test_testutils_iodriver(data_folder):
    fpath = str(data_folder) + '/test.tif'
    src = open_reader(fpath)
    assert src is not None
    assert src.crs is not None
    assert src.transform is not None
    assert src.crs.epsg == 4326
    assert src.shape == (2000, 4000)
    assert src.nodata == -999
    assert src.dtype == np.dtype(np.int16)

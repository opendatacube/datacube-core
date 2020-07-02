"""
Test utility functions from :module:`datacube.utils`


"""
import os
import pathlib
import string
from pathlib import Path

import numpy as np
import pytest
import rasterio
import xarray as xr
from dateutil.parser import parse
from hypothesis import given
from hypothesis.strategies import integers, text
from pandas import to_datetime

from datacube.helpers import write_geotiff
from datacube.utils import gen_password, write_user_secret_file, slurp
from datacube.model.utils import xr_apply
from datacube.utils.dates import date_sequence
from datacube.utils.math import (
    num2numpy,
    is_almost_int,
    valid_mask,
    invalid_mask,
    clamp,
    unsqueeze_data_array,
    unsqueeze_dataset,
    spatial_dims,
    data_resolution_and_offset,
    affine_from_axis,
)
from datacube.utils.py import sorted_items
from datacube.utils.uris import (uri_to_local_path, mk_part_uri, get_part_from_uri, as_url, is_url,
                                 pick_uri, uri_resolve, is_vsipath,
                                 normalise_path, default_base_dir)
from datacube.utils.io import check_write_path
from datacube.testutils import mk_sample_product, remove_crs


def test_stats_dates():
    # Winter for 1990
    winter_1990 = list(date_sequence(start=to_datetime('1990-06-01'), end=to_datetime('1990-09-01'), step_size='3m',
                                     stats_duration='3m'))
    assert winter_1990 == [(parse('1990-06-01'), parse('1990-09-01'))]

    # Every winter from 1990 - 1992
    three_years_of_winter = list(date_sequence(start=to_datetime('1990-06-01'), end=to_datetime('1992-09-01'),
                                               step_size='1y',
                                               stats_duration='3m'))
    assert three_years_of_winter == [(parse('1990-06-01'), parse('1990-09-01')),
                                     (parse('1991-06-01'), parse('1991-09-01')),
                                     (parse('1992-06-01'), parse('1992-09-01'))]

    # Full years from 1990 - 1994
    five_full_years = list(date_sequence(start=to_datetime('1990-01-01'), end=to_datetime('1995'), step_size='1y',
                                         stats_duration='1y'))
    assert five_full_years == [(parse('1990-01-01'), parse('1991-01-01')),
                               (parse('1991-01-01'), parse('1992-01-01')),
                               (parse('1992-01-01'), parse('1993-01-01')),
                               (parse('1993-01-01'), parse('1994-01-01')),
                               (parse('1994-01-01'), parse('1995-01-01'))]

    # Every season (three months), starting in March, from 1990 until end 1992-02
    two_years_of_seasons = list(date_sequence(start=to_datetime('1990-03-01'), end=to_datetime('1992-03'),
                                              step_size='3m',
                                              stats_duration='3m'))
    assert len(two_years_of_seasons) == 8
    assert two_years_of_seasons == [(parse('1990-03-01'), parse('1990-06-01')),
                                    (parse('1990-06-01'), parse('1990-09-01')),
                                    (parse('1990-09-01'), parse('1990-12-01')),
                                    (parse('1990-12-01'), parse('1991-03-01')),
                                    (parse('1991-03-01'), parse('1991-06-01')),
                                    (parse('1991-06-01'), parse('1991-09-01')),
                                    (parse('1991-09-01'), parse('1991-12-01')),
                                    (parse('1991-12-01'), parse('1992-03-01'))]  # Leap year!

    # Every month from 1990-01 to 1990-06
    monthly = list(date_sequence(start=to_datetime('1990-01-01'), end=to_datetime('1990-07-01'), step_size='1m',
                                 stats_duration='1m'))
    assert len(monthly) == 6

    # Complex
    # I want the average over 5 years


def test_uri_to_local_path():
    if os.name == 'nt':
        assert 'C:\\tmp\\test.tmp' == str(uri_to_local_path('file:///C:/tmp/test.tmp'))
        assert '\\\\remote\\path\\file.txt' == str(uri_to_local_path('file://remote/path/file.txt'))

    else:
        assert '/tmp/something.txt' == str(uri_to_local_path('file:///tmp/something.txt'))

        with pytest.raises(ValueError):
            uri_to_local_path('file://remote/path/file.txt')

    assert uri_to_local_path(None) is None

    with pytest.raises(ValueError):
        uri_to_local_path('ftp://example.com/tmp/something.txt')


@pytest.mark.parametrize("base", [
    "s3://foo",
    "gs://foo",
    "wasb://foo",
    "wasbs://foo",
    "/vsizip//vsicurl/https://host.tld/some/path",
])
def test_uri_resolve(base):
    abs_path = '/abs/path/to/something'
    some_uri = 'http://example.com/file.txt'

    assert uri_resolve(base, abs_path) == "file://" + abs_path
    assert uri_resolve(base, some_uri) is some_uri
    assert uri_resolve(base, None) is base
    assert uri_resolve(base, '') is base
    assert uri_resolve(base, 'relative/path') == base + '/relative/path'
    assert uri_resolve(base + '/', 'relative/path') == base + '/relative/path'
    assert uri_resolve(base + '/some/dir/', 'relative/path') == base + '/some/dir/relative/path'

    if not is_vsipath(base):
        assert uri_resolve(base + '/some/dir/file.txt', 'relative/path') == base + '/some/dir/relative/path'


def test_pick_uri():
    f, s, h = ('file://a', 's3://b', 'http://c')

    assert pick_uri([f, s, h]) is f
    assert pick_uri([s, h, f]) is f
    assert pick_uri([s, h]) is s
    assert pick_uri([h, s]) is h
    assert pick_uri([f, s, h], 'http:') is h
    assert pick_uri([f, s, h], 's3:') is s
    assert pick_uri([f, s, h], 'file:') is f

    with pytest.raises(ValueError):
        pick_uri([])

    with pytest.raises(ValueError):
        pick_uri([f, s, h], 'ftp:')

    with pytest.raises(ValueError):
        pick_uri([s, h], 'file:')


@given(integers(), integers(), integers())
def test_clamp(x, lower_bound, upper_bound):
    if lower_bound > upper_bound:
        lower_bound, upper_bound = upper_bound, lower_bound
    new_x = clamp(x, lower_bound, upper_bound)

    # If x was already between the bounds, it shouldn't have changed
    if lower_bound <= x <= upper_bound:
        assert new_x == x
    assert lower_bound <= new_x <= upper_bound


@given(integers(min_value=10, max_value=30))
def test_gen_pass(n_bytes):
    password1 = gen_password(n_bytes)
    password2 = gen_password(n_bytes)
    assert len(password1) >= n_bytes
    assert len(password2) >= n_bytes
    assert password1 != password2


@given(text(alphabet=string.digits + string.ascii_letters + ' ,:.![]?', max_size=20))
def test_write_user_secret_file(txt):
    fname = u".tst-datacube-uefvwr4cfkkl0ijk.txt"

    write_user_secret_file(txt, fname)
    txt_back = slurp(fname)
    os.remove(fname)
    assert txt == txt_back
    assert slurp(fname) is None


def test_write_geotiff(tmpdir, odc_style_xr_dataset):
    """Ensure the geotiff helper writer works, and supports datasets smaller than 256x256."""
    filename = tmpdir + '/test.tif'

    assert len(odc_style_xr_dataset.latitude) < 256

    with pytest.warns(DeprecationWarning):
        write_geotiff(filename, odc_style_xr_dataset)

    assert filename.exists()

    with rasterio.open(str(filename)) as src:
        written_data = src.read(1)

        assert (written_data == odc_style_xr_dataset['B10']).all()


def test_write_geotiff_str_crs(tmpdir, odc_style_xr_dataset):
    """Ensure the geotiff helper writer works, and supports crs as a string."""
    filename = tmpdir + '/test.tif'

    original_crs = odc_style_xr_dataset.crs

    odc_style_xr_dataset.attrs['crs'] = str(original_crs)

    with pytest.warns(DeprecationWarning):
        write_geotiff(filename, odc_style_xr_dataset)

    assert filename.exists()

    with rasterio.open(str(filename)) as src:
        written_data = src.read(1)

        assert (written_data == odc_style_xr_dataset['B10']).all()

    odc_style_xr_dataset = remove_crs(odc_style_xr_dataset)
    with pytest.raises(ValueError):
        with pytest.warns(DeprecationWarning):
            write_geotiff(filename, odc_style_xr_dataset)


def test_testutils_mk_sample():
    pp = mk_sample_product('tt', measurements=[('aa', 'int16', -999),
                                               ('bb', 'float32', np.nan)])
    assert set(pp.measurements) == {'aa', 'bb'}

    pp = mk_sample_product('tt', measurements=['aa', 'bb'])
    assert set(pp.measurements) == {'aa', 'bb'}

    pp = mk_sample_product('tt', measurements=[dict(name=n) for n in ['aa', 'bb']])
    assert set(pp.measurements) == {'aa', 'bb'}

    with pytest.raises(ValueError):
        mk_sample_product('tt', measurements=[None])


def test_testutils_write_files():
    from datacube.testutils import write_files, assert_file_structure

    files = {'a.txt': 'string',
             'aa.txt': ('line1\n', 'line2\n')}

    pp = write_files(files)
    assert pp.exists()
    assert_file_structure(pp, files)

    # test that we detect missing files
    (pp / 'a.txt').unlink()

    with pytest.raises(AssertionError):
        assert_file_structure(pp, files)

    with pytest.raises(AssertionError):
        assert_file_structure(pp, {'aa.txt': 3})

    with pytest.raises(ValueError):
        write_files({'tt': 3})


def test_part_uri():
    base = 'file:///foo.txt'

    for i in range(10):
        assert get_part_from_uri(mk_part_uri(base, i)) == i

    assert get_part_from_uri('file:///f.txt') is None
    assert get_part_from_uri('file:///f.txt#something_else') is None
    assert get_part_from_uri('file:///f.txt#part=aa') == 'aa'
    assert get_part_from_uri('file:///f.txt#part=111') == 111


def test_xr_apply():
    src = xr.DataArray(np.asarray([1, 2, 3], dtype='uint8'), dims=['time'])
    dst = xr_apply(src, lambda _, v: v, dtype='float32')

    assert dst.dtype.name == 'float32'
    assert dst.shape == src.shape
    assert dst.values.tolist() == [1, 2, 3]

    dst = xr_apply(src, lambda _, v: v)
    assert dst.dtype.name == 'uint8'
    assert dst.shape == src.shape
    assert dst.values.tolist() == [1, 2, 3]

    dst = xr_apply(src, lambda idx, _, v: idx[0] + v, with_numeric_index=True)
    assert dst.dtype.name == 'uint8'
    assert dst.shape == src.shape
    assert dst.values.tolist() == [0 + 1, 1 + 2, 2 + 3]


def test_sorted_items():
    aa = dict(c=1, b={}, a=[])

    assert ''.join(k for k, _ in sorted_items(aa)) == 'abc'
    assert ''.join(k for k, _ in sorted_items(aa, key=lambda x: x)) == 'abc'
    assert ''.join(k for k, _ in sorted_items(aa, reverse=True)) == 'cba'

    remap = dict(c=0, a=1, b=2)
    assert ''.join(k for k, _ in sorted_items(aa, key=lambda x: remap[x])) == 'cab'


def test_default_base_dir(monkeypatch):
    def set_pwd(p):
        if p is None:
            monkeypatch.delenv('PWD')
        else:
            monkeypatch.setenv('PWD', str(p))

    cwd = Path('.').resolve()

    # Default base dir (once resolved) will never be different from cwd
    assert default_base_dir().resolve() == cwd

    # should work when PWD is not set
    set_pwd(None)
    assert 'PWD' not in os.environ
    assert default_base_dir() == cwd

    # should work when PWD is not absolute path
    set_pwd('this/is/not/a/valid/path')
    assert default_base_dir() == cwd

    # should be cwd when PWD points to some other dir
    set_pwd(cwd / 'deeper')
    assert default_base_dir() == cwd

    set_pwd(cwd.parent)
    assert default_base_dir() == cwd

    # PWD == cwd
    set_pwd(cwd)
    assert default_base_dir() == cwd

    # TODO:
    # - create symlink to current directory in temp
    # - set PWD to that link
    # - make sure that returned path is the same as symlink and different from cwd


def test_time_info():
    from datacube.model.utils import time_info
    from datetime import datetime

    date = '2019-03-03T00:00:00'
    ee = time_info(datetime(2019, 3, 3))
    assert ee['extent']['from_dt'] == date
    assert ee['extent']['to_dt'] == date
    assert ee['extent']['center_dt'] == date
    assert len(ee['extent']) == 3

    ee = time_info(datetime(2019, 3, 3), key_time=datetime(2019, 4, 4))
    assert ee['extent']['from_dt'] == date
    assert ee['extent']['to_dt'] == date
    assert ee['extent']['center_dt'] == date
    assert ee['extent']['key_time'] == '2019-04-04T00:00:00'
    assert len(ee['extent']) == 4


def test_normalise_path():
    cwd = Path('.').resolve()
    assert normalise_path('.').resolve() == cwd

    p = Path('/a/b/c/d.txt')
    assert normalise_path(p) == Path(p)
    assert normalise_path(str(p)) == Path(p)

    base = Path('/a/b/')
    p = Path('c/d.txt')
    assert normalise_path(p, base) == (base / p)
    assert normalise_path(str(p), str(base)) == (base / p)
    assert normalise_path(p) == (cwd / p)

    with pytest.raises(ValueError):
        normalise_path(p, 'not/absolute/path')


def test_testutils_testimage():
    from datacube.testutils import mk_test_image, split_test_image

    for dtype in ('uint16', 'uint32', 'int32', 'float32'):
        aa = mk_test_image(128, 64, dtype=dtype, nodata=None)
        assert aa.shape == (64, 128)
        assert aa.dtype == dtype

        xx, yy = split_test_image(aa)
        assert (xx[:, 33] == 33).all()
        assert (xx[:, 127] == 127).all()
        assert (yy[23, :] == 23).all()
        assert (yy[63, :] == 63).all()


def test_testutils_gtif(tmpdir):
    from datacube.testutils import mk_test_image
    from datacube.testutils.io import write_gtiff, rio_slurp

    w, h, dtype, nodata, ndw = 96, 64, 'int16', -999, 7

    aa = mk_test_image(w, h, dtype, nodata, nodata_width=ndw)
    bb = mk_test_image(w, h, dtype, nodata=None)

    assert aa.shape == (h, w)
    assert aa.dtype.name == dtype
    assert aa[10, 30] == (30 << 8) | 10
    assert aa[10, 11] == nodata
    assert bb[10, 11] == (11 << 8) | 10

    aa5 = np.stack((aa,) * 5)

    fname = pathlib.Path(str(tmpdir / "aa.tiff"))
    fname5 = pathlib.Path(str(tmpdir / "aa5.tiff"))

    aa_meta = write_gtiff(fname, aa, nodata=nodata,
                          blocksize=128,
                          resolution=(100, -100),
                          offset=(12300, 11100),
                          overwrite=True)

    aa5_meta = write_gtiff(str(fname5), aa5, nodata=nodata,
                           resolution=(100, -100),
                           offset=(12300, 11100),
                           overwrite=True)

    assert fname.exists()
    assert fname5.exists()

    assert aa_meta.gbox.shape == (h, w)
    assert aa_meta.path is fname

    aa_, aa_meta_ = rio_slurp(fname)
    aa5_, aa5_meta_ = rio_slurp(fname5)

    assert aa_meta_.path is fname

    (sx, _, tx,
     _, sy, ty, *_) = aa5_meta_.transform

    assert (tx, ty) == (12300, 11100)
    assert (sx, sy) == (100, -100)

    np.testing.assert_array_equal(aa, aa_)
    np.testing.assert_array_equal(aa5, aa5_)

    assert aa_meta_.transform == aa_meta.transform
    assert aa5_meta_.transform == aa5_meta.transform

    # check that overwrite is off by default
    with pytest.raises(IOError):
        write_gtiff(fname, aa, nodata=nodata,
                    blocksize=128)

    # check that overwrite re-writes file
    write_gtiff(fname, bb[:32, :32],
                gbox=aa_meta.gbox[:32, :32],
                overwrite=True)

    bb_, mm = rio_slurp(fname, (32, 32))
    np.testing.assert_array_equal(bb[:32, :32], bb_)

    assert mm.gbox == aa_meta.gbox[:32, :32]

    with pytest.raises(ValueError):
        write_gtiff(fname, np.zeros((3, 4, 5, 6)))


def test_testutils_geobox():
    from datacube.testutils.io import dc_crs_from_rio, rio_geobox
    from rasterio.crs import CRS
    from affine import Affine

    assert rio_geobox({}) is None

    transform = Affine(10, 0, 4676,
                       0, -10, 171878)

    shape = (100, 640)
    h, w = shape
    crs = CRS.from_epsg(3578)

    meta = dict(width=w, height=h, transform=transform, crs=crs)
    gbox = rio_geobox(meta)

    assert gbox.shape == shape
    assert gbox.crs.epsg == 3578
    assert gbox.transform == transform

    wkt = '''PROJCS["unnamed",
    GEOGCS["NAD83",
       DATUM["North_American_Datum_1983",
             SPHEROID["GRS 1980",6378137,298.257222101, AUTHORITY["EPSG","7019"]],
             TOWGS84[0,0,0,0,0,0,0],AUTHORITY["EPSG","6269"]],
       PRIMEM["Greenwich",0,AUTHORITY["EPSG","8901"]],
       UNIT["degree",0.0174532925199433,AUTHORITY["EPSG","9122"]],
       ],
    PROJECTION["Albers_Conic_Equal_Area"],
    PARAMETER["standard_parallel_1",61.66666666666666],
    PARAMETER["standard_parallel_2",68],
    PARAMETER["latitude_of_center",59],
    PARAMETER["longitude_of_center",-132.5],
    PARAMETER["false_easting",500000],
    PARAMETER["false_northing",500000],
    UNIT["Meter",1]]
    '''

    crs_ = dc_crs_from_rio(CRS.from_wkt(wkt))
    assert crs_.epsg is None


@pytest.mark.parametrize("test_input,expected", [
    ("/foo/bar/file.txt", False),
    ("file:///foo/bar/file.txt", True),
    ("test.bar", False),
    ("s3://mybucket/objname.tiff", True),
    ("gs://mybucket/objname.tiff", True),
    ("wasb://mybucket/objname.tiff", True),
    ("wasbs://mybucket/objname.tiff", True),
    ("ftp://host.name/filename.txt", True),
    ("https://host.name.com/path/file.txt", True),
    ("http://host.name.com/path/file.txt", True),
    ("sftp://user:pass@host.name.com/path/file.txt", True),
    ("file+gzip://host.name.com/path/file.txt", True),
    ("bongo:host.name.com/path/file.txt", False),
])
def test_is_url(test_input, expected):
    assert is_url(test_input) == expected
    if expected:
        assert as_url(test_input) is test_input


def test_is_almost_int():
    assert is_almost_int(1, 1e-10)
    assert is_almost_int(1.001, .1)
    assert is_almost_int(2 - 0.001, .1)
    assert is_almost_int(-1.001, .1)


def test_valid_mask():
    xx = np.zeros((4, 8), dtype='float32')
    mm = valid_mask(xx, 0)
    assert mm.dtype == 'bool'
    assert mm.shape == xx.shape
    assert not mm.all()
    assert not mm.any()
    nn = invalid_mask(xx, 0)
    assert nn.dtype == 'bool'
    assert nn.shape == xx.shape
    assert nn.all()
    assert nn.any()

    mm = valid_mask(xx, 13)
    assert mm.dtype == 'bool'
    assert mm.shape == xx.shape
    assert mm.all()
    nn = invalid_mask(xx, 13)
    assert nn.dtype == 'bool'
    assert nn.shape == xx.shape
    assert not nn.any()

    mm = valid_mask(xx, None)
    assert mm.dtype == 'bool'
    assert mm.shape == xx.shape
    assert mm.all()
    nn = invalid_mask(xx, None)
    assert nn.dtype == 'bool'
    assert nn.shape == xx.shape
    assert not nn.any()

    mm = valid_mask(xx, np.nan)
    assert mm.dtype == 'bool'
    assert mm.shape == xx.shape
    assert mm.all()
    nn = invalid_mask(xx, np.nan)
    assert nn.dtype == 'bool'
    assert nn.shape == xx.shape
    assert not nn.any()

    xx[0, 0] = np.nan
    mm = valid_mask(xx, np.nan)
    assert not mm[0, 0]
    assert mm.sum() == (4 * 8 - 1)
    nn = invalid_mask(xx, np.nan)
    assert nn[0, 0]
    assert nn.sum() == 1


def test_num2numpy():
    assert num2numpy(None, 'int8') is None
    assert num2numpy(-1, 'int8').dtype == np.dtype('int8')
    assert num2numpy(-1, 'int8').dtype == np.int8(-1)

    assert num2numpy(-1, 'uint8') is None
    assert num2numpy(256, 'uint8') is None
    assert num2numpy(-1, 'uint16') is None
    assert num2numpy(-1, 'uint32') is None
    assert num2numpy(-1, 'uint8', ignore_range=True) == np.uint8(255)

    assert num2numpy(0, 'uint8') == 0
    assert num2numpy(255, 'uint8') == 255
    assert num2numpy(-128, 'int8') == -128
    assert num2numpy(127, 'int8') == 127
    assert num2numpy(128, 'int8') is None

    assert num2numpy(3.3, np.dtype('float32')).dtype == np.dtype('float32')
    assert num2numpy(3.3, np.float32).dtype == np.dtype('float32')
    assert num2numpy(3.3, np.float64).dtype == np.dtype('float64')


def test_utils_datares():
    assert data_resolution_and_offset(np.array([1.5, 2.5, 3.5])) == (1.0, 1.0)
    assert data_resolution_and_offset(np.array([5, 3, 1])) == (-2.0, 6.0)
    assert data_resolution_and_offset(np.array([5, 3])) == (-2.0, 6.0)
    assert data_resolution_and_offset(np.array([1.5]), 1) == (1.0, 1.0)

    with pytest.raises(ValueError):
        data_resolution_and_offset(np.array([]))

    with pytest.raises(ValueError):
        data_resolution_and_offset(np.array([]), 10)

    with pytest.raises(ValueError):
        data_resolution_and_offset(np.array([1]))


def test_utils_affine_from_axis():
    assert affine_from_axis(np.asarray([1.5, 2.5, 3.5]),
                            np.asarray([10.5, 11.5])) * (0, 0) == (1.0, 10.0)

    assert affine_from_axis(np.asarray([1.5, 2.5, 3.5]),
                            np.asarray([10.5, 11.5])) * (2, 1) == (3, 11)

    (sx, z1, tx,
     z2, sy, ty, *_) = affine_from_axis(np.asarray([1, 2, 3]),
                                        np.asarray([10, 20]))
    assert z1 == 0 and z2 == 0
    assert sx == 1 and sy == 10
    assert tx == 0.5 and ty == 5

    (sx, _, tx,
     _, sy, ty, *_) = affine_from_axis(np.asarray([1]),
                                       np.asarray([10, 20]), 1)
    assert sx == 1 and sy == 10
    assert tx == 0.5 and ty == 5

    (sx, _, tx,
     _, sy, ty, *_) = affine_from_axis(np.asarray([1]),
                                       np.asarray([10, 20]), (1, 1))
    assert sx == 1 and sy == 10
    assert tx == 0.5 and ty == 5

    (sx, _, tx,
     _, sy, ty, *_) = affine_from_axis(np.asarray([1]),
                                       np.asarray([10]), (2, 10))
    assert sx == 2 and sy == 10
    assert tx == 0 and ty == 5


def test_utils_math():
    xx = xr.DataArray(np.zeros((3, 4)),
                      name='xx',
                      dims=('y', 'x'),
                      coords={'x': np.arange(4),
                              'y': np.arange(3)})
    xx_t = unsqueeze_data_array(xx, 'time', 0)
    assert xx_t.dims == ('time', 'y', 'x')
    assert 'time' in xx_t.coords
    assert xx_t.data.shape == (1, 3, 4)

    ds = unsqueeze_dataset(xx.to_dataset(), 'time')
    assert ds.xx.dims == ('time', 'y', 'x')
    assert 'time' in ds.xx.coords
    assert ds.xx.data.shape == (1, 3, 4)


def test_spatial_dims():
    def tt(d1, d2):
        coords = {}
        coords[d1] = np.arange(3)
        coords[d2] = np.arange(4)
        return xr.DataArray(np.zeros((3, 4)),
                            name='xx',
                            dims=(d1, d2),
                            coords=coords)

    assert spatial_dims(tt('y', 'x')) == ('y', 'x')
    assert spatial_dims(tt('x', 'y')) == ('y', 'x')
    assert spatial_dims(tt('latitude', 'longitude')) == ('latitude', 'longitude')
    assert spatial_dims(tt('lat', 'lon')) == ('lat', 'lon')
    assert spatial_dims(tt('a', 'b')) is None
    assert spatial_dims(tt('a', 'b'), relaxed=True) == ('a', 'b')


def test_check_write_path(tmpdir):
    tmpdir = Path(str(tmpdir))
    some_path = tmpdir/"_should_not_exist-5125177.txt"
    assert not some_path.exists()
    assert check_write_path(some_path, overwrite=False) is some_path
    assert check_write_path(str(some_path), overwrite=False) == some_path
    assert isinstance(check_write_path(str(some_path), overwrite=False), Path)

    p = tmpdir/"ttt.tmp"
    with open(str(p), 'wt') as f:
        f.write("text")

    assert p.exists()
    with pytest.raises(IOError):
        check_write_path(p, overwrite=False)

    assert check_write_path(p, overwrite=True) == p
    assert not p.exists()

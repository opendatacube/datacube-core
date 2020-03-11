from datacube import Datacube
from datacube.api.query import query_group_by
import numpy as np
from types import SimpleNamespace
import pytest

from pathlib import Path
from datacube.testutils import (
    mk_sample_dataset,
    mk_test_image,
    gen_tiff_dataset,
)
from datacube.testutils.io import write_gtiff, rio_slurp, rio_slurp_xarray
from datacube.testutils.iodriver import NetCDF
from datacube.utils import ignore_exceptions_if


def test_load_data(tmpdir):
    tmpdir = Path(str(tmpdir))

    group_by = query_group_by('time')
    spatial = dict(resolution=(15, -15),
                   offset=(11230, 1381110),)

    nodata = -999
    aa = mk_test_image(96, 64, 'int16', nodata=nodata)

    ds, gbox = gen_tiff_dataset([SimpleNamespace(name='aa', values=aa, nodata=nodata)],
                                tmpdir,
                                prefix='ds1-',
                                timestamp='2018-07-19',
                                **spatial)
    assert ds.time is not None

    ds2, _ = gen_tiff_dataset([SimpleNamespace(name='aa', values=aa, nodata=nodata)],
                              tmpdir,
                              prefix='ds2-',
                              timestamp='2018-07-19',
                              **spatial)
    assert ds.time is not None
    assert ds.time == ds2.time

    sources = Datacube.group_datasets([ds], 'time')
    sources2 = Datacube.group_datasets([ds, ds2], group_by)

    mm = ['aa']
    mm = [ds.type.measurements[k] for k in mm]

    ds_data = Datacube.load_data(sources, gbox, mm)
    assert ds_data.aa.nodata == nodata
    np.testing.assert_array_equal(aa, ds_data.aa.values[0])

    custom_fuser_call_count = 0

    def custom_fuser(dest, delta):
        nonlocal custom_fuser_call_count
        custom_fuser_call_count += 1
        dest[:] += delta

    progress_call_data = []

    def progress_cbk(n, nt):
        progress_call_data.append((n, nt))

    ds_data = Datacube.load_data(sources2, gbox, mm, fuse_func=custom_fuser,
                                 progress_cbk=progress_cbk)
    assert ds_data.aa.nodata == nodata
    assert custom_fuser_call_count > 0
    np.testing.assert_array_equal(nodata + aa + aa, ds_data.aa.values[0])

    assert progress_call_data == [(1, 2), (2, 2)]


def test_load_data_cbk(tmpdir):
    from datacube.api import TerminateCurrentLoad

    tmpdir = Path(str(tmpdir))

    spatial = dict(resolution=(15, -15),
                   offset=(11230, 1381110),)

    nodata = -999
    aa = mk_test_image(96, 64, 'int16', nodata=nodata)

    bands = [SimpleNamespace(name=name, values=aa, nodata=nodata)
             for name in ['aa', 'bb']]

    ds, gbox = gen_tiff_dataset(bands,
                                tmpdir,
                                prefix='ds1-',
                                timestamp='2018-07-19',
                                **spatial)
    assert ds.time is not None

    ds2, _ = gen_tiff_dataset(bands,
                              tmpdir,
                              prefix='ds2-',
                              timestamp='2018-07-19',
                              **spatial)
    assert ds.time is not None
    assert ds.time == ds2.time

    sources = Datacube.group_datasets([ds, ds2], 'time')
    progress_call_data = []

    def progress_cbk(n, nt):
        progress_call_data.append((n, nt))

    ds_data = Datacube.load_data(sources, gbox, ds.type.measurements,
                                 progress_cbk=progress_cbk)

    assert progress_call_data == [(1, 4), (2, 4), (3, 4), (4, 4)]
    np.testing.assert_array_equal(aa, ds_data.aa.values[0])
    np.testing.assert_array_equal(aa, ds_data.bb.values[0])

    def progress_cbk_fail_early(n, nt):
        progress_call_data.append((n, nt))
        raise TerminateCurrentLoad()

    def progress_cbk_fail_early2(n, nt):
        progress_call_data.append((n, nt))
        if n > 1:
            raise KeyboardInterrupt()

    progress_call_data = []
    ds_data = Datacube.load_data(sources, gbox, ds.type.measurements,
                                 progress_cbk=progress_cbk_fail_early)

    assert progress_call_data == [(1, 4)]
    assert ds_data.dc_partial_load is True
    np.testing.assert_array_equal(aa, ds_data.aa.values[0])
    np.testing.assert_array_equal(nodata, ds_data.bb.values[0])

    progress_call_data = []
    ds_data = Datacube.load_data(sources, gbox, ds.type.measurements,
                                 progress_cbk=progress_cbk_fail_early2)

    assert ds_data.dc_partial_load is True
    assert progress_call_data == [(1, 4), (2, 4)]


def test_hdf5_lock_release_on_failure():
    from datacube.storage._rio import RasterDatasetDataSource, HDF5_LOCK
    from datacube.storage import BandInfo

    band = dict(name='xx',
                layer='xx',
                dtype='uint8',
                units='K',
                nodata=33)

    ds = mk_sample_dataset([band],
                           uri='file:///tmp/this_probably_doesnot_exist_37237827513/xx.nc',
                           format=NetCDF)
    src = RasterDatasetDataSource(BandInfo(ds, 'xx'))

    with pytest.raises(OSError):
        with src.open():
            assert False and "Did not expect to get here"

    assert not HDF5_LOCK._is_owned()


def test_rio_slurp(tmpdir):
    w, h, dtype, nodata, ndw = 96, 64, 'int16', -999, 7

    pp = Path(str(tmpdir))

    aa = mk_test_image(w, h, dtype, nodata, nodata_width=ndw)

    assert aa.shape == (h, w)
    assert aa.dtype.name == dtype
    assert aa[10, 30] == (30 << 8) | 10
    assert aa[10, 11] == nodata

    aa0 = aa.copy()
    mm0 = write_gtiff(pp/"rio-slurp-aa.tif", aa, nodata=-999, overwrite=True)
    mm00 = write_gtiff(pp/"rio-slurp-aa-missing-nodata.tif", aa, nodata=None, overwrite=True)

    aa, mm = rio_slurp(mm0.path)
    np.testing.assert_array_equal(aa, aa0)
    assert mm.gbox == mm0.gbox
    assert aa.shape == mm.gbox.shape
    xx = rio_slurp_xarray(mm0.path)
    assert mm.gbox == xx.geobox
    np.testing.assert_array_equal(xx.values, aa0)

    aa, mm = rio_slurp(mm0.path, aa0.shape)
    np.testing.assert_array_equal(aa, aa0)
    assert aa.shape == mm.gbox.shape
    assert mm.gbox is mm.src_gbox
    xx = rio_slurp_xarray(mm0.path, aa0.shape)
    assert mm.gbox == xx.geobox
    np.testing.assert_array_equal(xx.values, aa0)

    aa, mm = rio_slurp(mm0.path, (3, 7))
    assert aa.shape == (3, 7)
    assert aa.shape == mm.gbox.shape
    assert mm.gbox != mm.src_gbox
    assert mm.src_gbox == mm0.gbox
    assert mm.gbox.extent == mm0.gbox.extent

    aa, mm = rio_slurp(mm0.path, aa0.shape)
    np.testing.assert_array_equal(aa, aa0)
    assert aa.shape == mm.gbox.shape

    aa, mm = rio_slurp(mm0.path, mm0.gbox, resampling='nearest')
    np.testing.assert_array_equal(aa, aa0)
    xx = rio_slurp_xarray(mm0.path, mm0.gbox)
    assert mm.gbox == xx.geobox
    np.testing.assert_array_equal(xx.values, aa0)

    aa, mm = rio_slurp(mm0.path, gbox=mm0.gbox, dtype='float32')
    assert aa.dtype == 'float32'
    np.testing.assert_array_equal(aa, aa0.astype('float32'))
    xx = rio_slurp_xarray(mm0.path, gbox=mm0.gbox)
    assert mm.gbox == xx.geobox
    assert mm.nodata == xx.nodata
    np.testing.assert_array_equal(xx.values, aa0)

    aa, mm = rio_slurp(mm0.path, mm0.gbox, dst_nodata=-33)
    np.testing.assert_array_equal(aa == -33, aa0 == -999)

    aa, mm = rio_slurp(mm00.path, mm00.gbox, dst_nodata=None)
    np.testing.assert_array_equal(aa, aa0)


def test_rio_slurp_with_gbox(tmpdir):
    w, h, dtype, nodata, ndw = 96, 64, 'int16', -999, 7

    pp = Path(str(tmpdir))
    aa = mk_test_image(w, h, dtype, nodata, nodata_width=ndw)
    assert aa.dtype.name == dtype
    assert aa[10, 30] == (30 << 8) | 10
    assert aa[10, 11] == nodata

    aa = np.stack([aa, aa[::-1, ::-1]])
    assert aa.shape == (2, h, w)
    aa0 = aa.copy()

    mm = write_gtiff(pp/"rio-slurp-aa.tif", aa, nodata=-999, overwrite=True)
    assert mm.count == 2

    aa, mm = rio_slurp(mm.path, mm.gbox)
    assert aa.shape == aa0.shape
    np.testing.assert_array_equal(aa, aa0)


def test_missing_file_handling():
    with pytest.raises(IOError):
        rio_slurp('no-such-file.tiff')

    # by default should catch any exception
    with ignore_exceptions_if(True):
        rio_slurp('no-such-file.tiff')

    # this is equivalent to previous default behaviour, note that missing http
    # resources are not OSError
    with ignore_exceptions_if(True, (OSError,)):
        rio_slurp('no-such-file.tiff')

    # check that only requested exceptions are caught
    with pytest.raises(IOError):
        with ignore_exceptions_if(True, (ValueError, ArithmeticError)):
            rio_slurp('no-such-file.tiff')


def test_native_geobox_ingested():
    from datacube.testutils.io import native_geobox
    from datacube.testutils.geom import AlbersGS

    gbox = AlbersGS.tile_geobox((15, -40))
    ds = mk_sample_dataset([dict(name='a')],
                           geobox=gbox,
                           product_opts=dict(with_grid_spec=True))

    assert native_geobox(ds) == gbox

    # check that dataset covering several tiles is detected as invalid
    ds = mk_sample_dataset([dict(name='a')],
                           geobox=gbox.buffered(10, 10),
                           product_opts=dict(with_grid_spec=True))

    with pytest.raises(ValueError):
        native_geobox(ds)


def test_native_load(tmpdir):
    from datacube.testutils.io import native_load, native_geobox

    tmpdir = Path(str(tmpdir))
    spatial = dict(resolution=(15, -15),
                   offset=(11230, 1381110),)
    nodata = -999
    aa = mk_test_image(96, 64, 'int16', nodata=nodata)
    cc = mk_test_image(32, 16, 'int16', nodata=nodata)

    bands = [SimpleNamespace(name=name, values=aa, nodata=nodata)
             for name in ['aa', 'bb']]
    bands.append(SimpleNamespace(name='cc', values=cc, nodata=nodata))

    ds, gbox = gen_tiff_dataset(bands[:2],
                                tmpdir,
                                prefix='ds1-',
                                timestamp='2018-07-19',
                                **spatial)

    xx = native_load(ds)
    assert xx.geobox == gbox
    np.testing.assert_array_equal(aa, xx.isel(time=0).aa.values)
    np.testing.assert_array_equal(aa, xx.isel(time=0).bb.values)

    ds, gbox_cc = gen_tiff_dataset(bands,
                                   tmpdir,
                                   prefix='ds2-',
                                   timestamp='2018-07-19',
                                   **spatial)

    # cc is different size from aa,bb
    with pytest.raises(ValueError):
        xx = native_load(ds)

    # cc is different size from aa,bb
    with pytest.raises(ValueError):
        xx = native_geobox(ds)

    # aa and bb are the same
    assert native_geobox(ds, ['aa', 'bb']) == gbox
    xx = native_load(ds, ['aa', 'bb'])
    assert xx.geobox == gbox
    np.testing.assert_array_equal(aa, xx.isel(time=0).aa.values)
    np.testing.assert_array_equal(aa, xx.isel(time=0).bb.values)

    # cc will be reprojected
    assert native_geobox(ds, basis='aa') == gbox
    xx = native_load(ds, basis='aa')
    assert xx.geobox == gbox
    np.testing.assert_array_equal(aa, xx.isel(time=0).aa.values)
    np.testing.assert_array_equal(aa, xx.isel(time=0).bb.values)

    # cc is compatible with self
    xx = native_load(ds, ['cc'])
    assert xx.geobox == gbox_cc
    np.testing.assert_array_equal(cc, xx.isel(time=0).cc.values)

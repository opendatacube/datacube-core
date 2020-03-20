from contextlib import contextmanager

import mock
import numpy as np
import pytest
import rasterio.warp
from affine import Affine, identity
from rasterio.warp import Resampling

from datacube.drivers.datasource import DataSource
from datacube.model import Dataset, DatasetType, MetadataType
from datacube.testutils.io import RasterFileDataSource
from datacube.storage import BandInfo
from datacube.drivers.netcdf import create_netcdf_storage_unit, Variable
from datacube.storage import reproject_and_fuse
from datacube.storage._rio import RasterDatasetDataSource, _url2rasterio
from datacube.storage._read import read_time_slice
from datacube.utils.geometry import GeoBox

from datacube.testutils.geom import epsg4326, epsg3577


def mk_gbox(shape=(2, 2), transform=identity, crs=epsg4326):
    H, W = shape
    return GeoBox(W, H, transform, crs)


def test_first_source_is_priority_in_reproject_and_fuse():
    crs = epsg4326
    shape = (2, 2)
    no_data = -1

    source1 = FakeDatasetSource([[1, 1], [1, 1]], crs=crs, shape=shape)
    source2 = FakeDatasetSource([[2, 2], [2, 2]], crs=crs, shape=shape)
    sources = [source1, source2]

    output_data = np.full(shape, fill_value=no_data, dtype='int16')
    reproject_and_fuse(sources, output_data, mk_gbox(shape, crs=crs), dst_nodata=no_data)

    assert (output_data == 1).all()


def test_second_source_used_when_first_is_empty():
    crs = epsg4326
    shape = (2, 2)
    no_data = -1

    source1 = FakeDatasetSource([[-1, -1], [-1, -1]], crs=crs, shape=shape)
    source2 = FakeDatasetSource([[2, 2], [2, 2]], crs=crs, shape=shape)
    sources = [source1, source2]

    output_data = np.full(shape, fill_value=no_data, dtype='int16')
    reproject_and_fuse(sources, output_data, mk_gbox(shape, crs=crs), dst_nodata=no_data)

    assert (output_data == 2).all()


def test_progress_cbk():
    crs = epsg4326
    shape = (2, 2)
    no_data = -1
    output_data = np.full(shape, fill_value=no_data, dtype='int16')

    src = FakeDatasetSource([[2, 2], [2, 2]], crs=crs, shape=shape)

    def _cbk(n_so_far, n_total, out):
        out.append((n_so_far, n_total))

    cbk_args = []
    reproject_and_fuse([src], output_data,
                       mk_gbox(shape, crs=crs),
                       dst_nodata=no_data, progress_cbk=lambda *a: _cbk(*a, cbk_args))

    assert cbk_args == [(1, 1)]

    cbk_args = []
    reproject_and_fuse([src, src], output_data,
                       mk_gbox(shape, crs=crs),
                       dst_nodata=no_data, progress_cbk=lambda *a: _cbk(*a, cbk_args))

    assert cbk_args == [(1, 2), (2, 2)]


def test_mixed_result_when_first_source_partially_empty():
    crs = epsg4326
    shape = (2, 2)
    no_data = -1

    source1 = FakeDatasetSource([[1, 1], [no_data, no_data]], crs=crs)
    source2 = FakeDatasetSource([[2, 2], [2, 2]], crs=crs)
    sources = [source1, source2]

    output_data = np.full(shape, fill_value=no_data, dtype='int16')
    reproject_and_fuse(sources, output_data, mk_gbox(shape, crs=crs), dst_nodata=no_data)

    assert (output_data == [[1, 1], [2, 2]]).all()


def test_when_input_empty():
    shape = (2, 2)
    no_data = -1
    out = np.full(shape, fill_value=no_data, dtype='int16')
    reproject_and_fuse([], out, mk_gbox(shape, crs=epsg4326), dst_nodata=no_data)
    assert (out == no_data).all()


def test_mixed_result_when_first_source_partially_empty_with_nan_nodata():
    crs = epsg4326
    shape = (2, 2)
    no_data = np.nan

    source1 = FakeDatasetSource([[1, 1], [no_data, no_data]], crs=crs)
    source2 = FakeDatasetSource([[2, 2], [2, 2]], crs=crs)
    sources = [source1, source2]

    output_data = np.full(shape, fill_value=no_data, dtype='float64')
    reproject_and_fuse(sources, output_data, mk_gbox(shape, crs=crs), dst_nodata=no_data)

    assert (output_data == [[1, 1], [2, 2]]).all()


class FakeBandDataSource(object):
    def __init__(self, value, nodata, shape=(2, 2), *args, **kwargs):
        self.value = value
        self.crs = epsg4326
        self.transform = Affine.identity()
        self.dtype = np.int16 if not np.isnan(nodata) else np.float64
        self.shape = shape
        self.nodata = nodata

    def read(self, window=None, out_shape=None):
        """Read data in the native format, returning a numpy array
        """
        return np.array(self.value)


class FakeDatasetSource(DataSource):
    def __init__(self, value, bandnumber=1, nodata=-999, shape=(2, 2), crs=None, transform=None,
                 band_source_class=FakeBandDataSource):
        super(FakeDatasetSource, self).__init__()
        self.value = value
        self.bandnumber = bandnumber
        self.crs = crs
        self.transform = transform
        self.band_source_class = band_source_class
        self.shape = shape
        self.nodata = nodata

    def get_bandnumber(self, src):
        return self.bandnumber

    def get_transform(self, shape):
        if self.transform is None:
            raise RuntimeError('No transform in the data and no fallback')
        return self.transform

    def get_crs(self):
        if self.crs is None:
            raise RuntimeError('No CRS in the data and no fallback')
        return self.crs

    @contextmanager
    def open(self):
        """Context manager which returns a :class:`BandDataSource`"""
        yield self.band_source_class(value=self.value, nodata=self.nodata, shape=self.shape)


class BrokenBandDataSource(FakeBandDataSource):
    def read(self, window=None, out_shape=None):
        raise OSError('Read or write failed')


def test_read_from_broken_source():
    crs = epsg4326
    shape = (2, 2)
    no_data = -1

    source1 = FakeDatasetSource(value=[[1, 1], [no_data, no_data]], crs=crs, band_source_class=BrokenBandDataSource)
    source2 = FakeDatasetSource(value=[[2, 2], [2, 2]], crs=crs)
    sources = [source1, source2]

    output_data = np.full(shape, fill_value=no_data, dtype='int16')

    gbox = mk_gbox(shape, crs=crs)

    # Check exception is raised
    with pytest.raises(OSError):
        reproject_and_fuse(sources, output_data, gbox, dst_nodata=no_data)

    # Check can ignore errors
    reproject_and_fuse(sources, output_data, gbox, dst_nodata=no_data,
                       skip_broken_datasets=True)

    assert (output_data == [[2, 2], [2, 2]]).all()


class FakeDataSource(object):
    def __init__(self):
        self.crs = epsg4326
        self.transform = Affine(0.25, 0, 100, 0, -0.25, -30)
        self.nodata = -999
        self.shape = (613, 597)

        self.data = np.full(self.shape, self.nodata, dtype='int16')
        self.data[:512, :512] = np.arange(512) + np.arange(512).reshape((512, 1))

    def read(self, window=None, out_shape=None):
        data = self.data

        if window:
            data = data[slice(*window[0]), slice(*window[1])]

        if out_shape is not None and out_shape != data.shape:
            xidx = ((np.arange(out_shape[1]) + 0.5) * (data.shape[1] / out_shape[1]) - 0.5).round().astype('int')
            yidx = ((np.arange(out_shape[0]) + 0.5) * (data.shape[0] / out_shape[0]) - 0.5).round().astype('int')
            data = data[np.meshgrid(yidx, xidx, indexing='ij')]

        return data.copy()


def assert_same_read_results(source, dst_shape, dst_dtype, dst_transform, dst_nodata, dst_projection, resampling):
    expected = np.empty(dst_shape, dtype=dst_dtype)
    with source.open() as src:
        rasterio.warp.reproject(src.data,
                                expected,
                                src_transform=src.transform,
                                src_crs=str(src.crs),
                                src_nodata=src.nodata,
                                dst_transform=dst_transform,
                                dst_crs=str(dst_projection),
                                dst_nodata=dst_nodata,
                                resampling=resampling)

    result = np.full(dst_shape, dst_nodata, dtype=dst_dtype)
    H, W = dst_shape
    dst_gbox = GeoBox(W, H, dst_transform, dst_projection)
    with source.open() as rdr:
        read_time_slice(rdr,
                        result,
                        dst_gbox,
                        dst_nodata=dst_nodata,
                        resampling=resampling)

    assert np.isclose(result, expected, atol=0, rtol=0.05, equal_nan=True).all()
    return result


def test_read_from_fake_source():
    data_source = FakeDataSource()

    @contextmanager
    def fake_open():
        yield data_source

    source = mock.Mock()
    source.open = fake_open

    # one-to-one copy
    assert_same_read_results(
        source,
        dst_shape=data_source.shape,
        dst_dtype=data_source.data.dtype,
        dst_transform=data_source.transform,
        dst_nodata=data_source.nodata,
        dst_projection=data_source.crs,
        resampling=Resampling.nearest)

    # change dtype
    assert_same_read_results(
        source,
        dst_shape=data_source.shape,
        dst_dtype='int32',
        dst_transform=data_source.transform,
        dst_nodata=data_source.nodata,
        dst_projection=data_source.crs,
        resampling=Resampling.nearest)

    # change nodata
    assert_same_read_results(
        source,
        dst_shape=data_source.shape,
        dst_dtype='float32',
        dst_transform=data_source.transform,
        dst_nodata=float('nan'),
        dst_projection=data_source.crs,
        resampling=Resampling.nearest)

    # different offsets/sizes
    assert_same_read_results(
        source,
        dst_shape=(517, 557),
        dst_dtype='float32',
        dst_transform=data_source.transform * Affine.translation(-200, -200),
        dst_nodata=float('nan'),
        dst_projection=data_source.crs,
        resampling=Resampling.nearest)

    assert_same_read_results(
        source,
        dst_shape=(807, 879),
        dst_dtype='float32',
        dst_transform=data_source.transform * Affine.translation(200, 200),
        dst_nodata=float('nan'),
        dst_projection=data_source.crs,
        resampling=Resampling.nearest)

    assert_same_read_results(
        source,
        dst_shape=(807, 879),
        dst_dtype='float32',
        dst_transform=data_source.transform * Affine.translation(1500, -1500),
        dst_nodata=float('nan'),
        dst_projection=data_source.crs,
        resampling=Resampling.nearest)

    # flip axis
    assert_same_read_results(
        source,
        dst_shape=(517, 557),
        dst_dtype='float32',
        dst_transform=data_source.transform * Affine.translation(0, 512) * Affine.scale(1, -1),
        dst_nodata=float('nan'),
        dst_projection=data_source.crs,
        resampling=Resampling.nearest)

    assert_same_read_results(
        source,
        dst_shape=(517, 557),
        dst_dtype='float32',
        dst_transform=data_source.transform * Affine.translation(512, 0) * Affine.scale(-1, 1),
        dst_nodata=float('nan'),
        dst_projection=data_source.crs,
        resampling=Resampling.nearest)

    # scale
    assert_same_read_results(
        source,
        dst_shape=(250, 500),
        dst_dtype='float32',
        dst_transform=data_source.transform * Affine.scale(1.2, 1.4),
        dst_nodata=float('nan'),
        dst_projection=data_source.crs,
        resampling=Resampling.nearest)

    assert_same_read_results(
        source,
        dst_shape=(500, 250),
        dst_dtype='float32',
        dst_transform=data_source.transform * Affine.scale(1.4, 1.2),
        dst_nodata=float('nan'),
        dst_projection=data_source.crs,
        resampling=Resampling.cubic)

    assert_same_read_results(
        source,
        dst_shape=(67, 35),
        dst_dtype='float32',
        dst_transform=data_source.transform * Affine.scale(1.16, 1.8),
        dst_nodata=float('nan'),
        dst_projection=data_source.crs,
        resampling=Resampling.cubic)

    assert_same_read_results(
        source,
        dst_shape=(35, 67),
        dst_dtype='float32',
        dst_transform=data_source.transform * Affine.translation(27, 35) * Affine.scale(1.8, 1.16),
        dst_nodata=float('nan'),
        dst_projection=data_source.crs,
        resampling=Resampling.cubic)

    assert_same_read_results(
        source,
        dst_shape=(35, 67),
        dst_dtype='float32',
        dst_transform=data_source.transform * Affine.translation(-13, -27) * Affine.scale(1.8, 1.16),
        dst_nodata=float('nan'),
        dst_projection=data_source.crs,
        resampling=Resampling.average)

    # scale + flip
    assert_same_read_results(
        source,
        dst_shape=(35, 67),
        dst_dtype='float32',
        dst_transform=data_source.transform * Affine.translation(15, 512 + 17) * Affine.scale(1.8, -1.16),
        dst_nodata=float('nan'),
        dst_projection=data_source.crs,
        resampling=Resampling.cubic)

    assert_same_read_results(
        source,
        dst_shape=(67, 35),
        dst_dtype='float32',
        dst_transform=data_source.transform * Affine.translation(512 - 23, -29) * Affine.scale(-1.16, 1.8),
        dst_nodata=float('nan'),
        dst_projection=data_source.crs,
        resampling=Resampling.cubic)

    # TODO: crs change


def _read_from_source(source, dest, dst_transform, dst_nodata, dst_projection, resampling):
    """
    Adapt old signature to new function, so that we can keep old tests at least for now
    """
    H, W = dest.shape
    gbox = GeoBox(W, H, dst_transform, dst_projection)
    dest[:] = dst_nodata  # new code assumes pre-populated image
    with source.open() as rdr:
        read_time_slice(rdr, dest, gbox, resampling=resampling, dst_nodata=dst_nodata)


class TestRasterDataReading(object):
    @pytest.mark.parametrize("dst_nodata", [
        np.nan, float("nan"), -999
    ])
    def xtest_failed_data_read(self, make_sample_geotiff, dst_nodata):
        sample_geotiff_path, geobox, written_data = make_sample_geotiff(dst_nodata)

        src_transform = Affine(25.0, 0.0, 1200000.0,
                               0.0, -25.0, -4200000.0)
        source = RasterFileDataSource(sample_geotiff_path, 1, transform=src_transform)

        dest = np.zeros((20, 100))
        dst_nodata = -999
        dst_projection = epsg3577
        dst_resampling = Resampling.nearest

        # Read exactly the hunk of data that we wrote
        dst_transform = Affine(25.0, 0.0, 127327.0,
                               0.0, -25.0, -417232.0)
        _read_from_source(source, dest, dst_transform, dst_nodata, dst_projection, dst_resampling)

        assert np.all(written_data == dest)

    @pytest.mark.parametrize("dst_nodata", [
        np.nan, float("nan"), -999
    ])
    def test_read_with_rasterfiledatasource(self, make_sample_geotiff, dst_nodata):
        sample_geotiff_path, geobox, written_data = make_sample_geotiff(dst_nodata)

        source = RasterFileDataSource(str(sample_geotiff_path), 1)

        dest = np.zeros_like(written_data)
        dst_transform = geobox.transform
        dst_projection = epsg3577
        dst_resampling = Resampling.nearest

        # Read exactly the hunk of data that we wrote
        _read_from_source(source, dest, dst_transform, dst_nodata, dst_projection, dst_resampling)

        assert np.all(written_data == dest)

        # Try reading from partially outside of our area
        xoff = 50
        offset_transform = dst_transform * Affine.translation(xoff, 0)
        dest = np.zeros_like(written_data)

        _read_from_source(source, dest, offset_transform, dst_nodata, dst_projection, dst_resampling)
        assert np.all(written_data[:, xoff:] == dest[:, :xoff])

        # Try reading from complete outside of our area, should return nodata
        xoff = 300
        offset_transform = dst_transform * Affine.translation(xoff, 0)
        dest = np.zeros_like(written_data)

        _read_from_source(source, dest, offset_transform, dst_nodata, dst_projection, dst_resampling)
        if np.isnan(dst_nodata):
            assert np.all(np.isnan(dest))
        else:
            assert np.all(dst_nodata == dest)

    @pytest.mark.parametrize("dst_transform", [
        Affine(25.0, 0.0, 1273275.0, 0.0, -25.0, -4172325.0),
        Affine(25.0, 0.0, 127327.0, 0.0, -25.0, -417232.0)
    ])
    def test_read_data_from_outside_file_region(self, make_sample_netcdf, dst_transform):
        sample_nc, geobox, written_data = make_sample_netcdf

        source = RasterFileDataSource(sample_nc, 1)

        dest = np.zeros((200, 1000))
        dst_nodata = -999
        dst_projection = epsg3577
        dst_resampling = Resampling.nearest

        # Read exactly the hunk of data that we wrote
        _read_from_source(source, dest, dst_transform, dst_nodata, dst_projection, dst_resampling)

        assert np.all(dest == -999)

    def test_read_from_file_with_missing_crs(self, no_crs_gdal_path):
        """
        We need to be able to read from data files even when GDAL can't automatically gather all the metdata.

        The :class:`RasterFileDataSource` is able to override the nodata, CRS and transform attributes if necessary.
        """
        crs = epsg4326
        nodata = -999
        transform = Affine(0.01, 0.0, 111.975,
                           0.0, 0.01, -9.975)
        data_source = RasterFileDataSource(no_crs_gdal_path, bandnumber=1, nodata=nodata, crs=crs, transform=transform)
        with pytest.warns(DeprecationWarning):
            with data_source.open() as src:
                dest1 = src.read()
                assert dest1.shape == (10, 10)


@pytest.fixture
def make_sample_netcdf(tmpdir):
    """Make a test Geospatial NetCDF file, 4000x4000 int16 random data, in a variable named `sample`.
    Return the GDAL access string."""
    sample_nc = str(tmpdir.mkdir('netcdfs').join('sample.nc'))
    geobox = GeoBox(4000, 4000, affine=Affine(25.0, 0.0, 1200000, 0.0, -25.0, -4200000), crs=epsg3577)

    sample_data = np.random.randint(10000, size=(4000, 4000), dtype=np.int16)

    variables = {'sample': Variable(sample_data.dtype, nodata=-999, dims=geobox.dimensions, units=1)}
    nco = create_netcdf_storage_unit(sample_nc, geobox.crs, geobox.coordinates, variables=variables, variable_params={})

    nco['sample'][:] = sample_data

    nco.close()

    return 'NetCDF:"%s":sample' % sample_nc, geobox, sample_data


@pytest.fixture
def make_sample_geotiff(tmpdir):
    """ Make a sample geotiff, filled with random data, and twice as tall as it is wide. """
    def internal_make_sample_geotiff(nodata=-999):
        sample_geotiff = str(tmpdir.mkdir('tiffs').join('sample.tif'))

        geobox = GeoBox(100, 200, affine=Affine(25.0, 0.0, 0, 0.0, -25.0, 0), crs=epsg3577)
        if np.isnan(nodata):
            out_dtype = 'float64'
            sample_data = 10000 * np.random.random_sample(size=geobox.shape)
        else:
            out_dtype = 'int16'
            sample_data = np.random.randint(10000, size=geobox.shape, dtype=out_dtype)
        rio_args = {
            'height': geobox.height,
            'width': geobox.width,
            'count': 1,
            'dtype': out_dtype,
            'crs': 'EPSG:3577',
            'transform': geobox.transform,
            'nodata': nodata
        }
        with rasterio.open(sample_geotiff, 'w', driver='GTiff', **rio_args) as dst:
            dst.write(sample_data, 1)

        return sample_geotiff, geobox, sample_data
    return internal_make_sample_geotiff


_EXAMPLE_METADATA_TYPE = MetadataType(
    {
        'name': 'eo',
        'dataset': dict(
            id=['id'],
            label=['ga_label'],
            creation_time=['creation_dt'],
            measurements=['image', 'bands'],
            sources=['lineage', 'source_datasets'],
            format=['format', 'name'],
        )
    },
    dataset_search_fields={}
)

_EXAMPLE_DATASET_TYPE = DatasetType(
    _EXAMPLE_METADATA_TYPE,
    {
        'name': 'ls5_nbar_scene',
        'description': "Landsat 5 NBAR 25 metre",
        'metadata_type': 'eo',
        'metadata': {},
        'measurements': [
            {'aliases': ['band_2', '2'],
             'dtype': 'int16',
             'name': 'green',
             'nodata': -999,
             'units': '1'}],
    }
)


def test_multiband_support_in_datasetsource(example_gdal_path):
    defn = {
        "id": '12345678123456781234567812345678',
        "format": {"name": "GeoTiff"},
        "image": {
            "bands": {
                'green': {
                    'type': 'reflective',
                    'cell_size': 25.0,
                    'path': example_gdal_path,
                    'label': 'Coastal Aerosol',
                    'number': '1',
                },
            }
        }
    }

    # Without new band attribute, default to band number 1
    d = Dataset(_EXAMPLE_DATASET_TYPE, defn, uris=['file:///tmp'])

    ds = RasterDatasetDataSource(BandInfo(d, 'green'))

    bandnum = ds.get_bandnumber(None)

    assert bandnum == 1

    with ds.open() as foo:
        data = foo.read()
        assert isinstance(data, np.ndarray)

    #############
    # With new 'image.bands.[band].band' attribute
    band_num = 3
    defn['image']['bands']['green']['band'] = band_num
    d = Dataset(_EXAMPLE_DATASET_TYPE, defn, uris=['file:///tmp'])

    ds = RasterDatasetDataSource(BandInfo(d, 'green'))

    assert ds.get_bandnumber(None) == band_num


def test_netcdf_multi_part():
    defn = {
        "id": '12345678123456781234567812345678',
        "format": {"name": "NetCDF CF"},
        "image": {
            "bands": {
                'green': {
                    'type': 'reflective',
                    'cell_size': 25.0,
                    'layer': 'green',
                    'path': '',
                    'label': 'Coastal Aerosol',
                },
            }
        }
    }

    def ds(uri):
        d = Dataset(_EXAMPLE_DATASET_TYPE, defn, uris=[uri])
        return RasterDatasetDataSource(BandInfo(d, 'green'))

    for i in range(3):
        assert ds('file:///tmp.nc#part=%d' % i).get_bandnumber() == (i+1)

    # can't tell without opening file
    assert ds('file:///tmp.nc').get_bandnumber() is None


def test_rasterio_nodata(tmpdir):
    from datacube.testutils.io import dc_read, write_gtiff
    from pathlib import Path

    roi = np.s_[10:20, 20:30]
    xx = np.zeros((64, 64), dtype='uint8')
    xx[roi] = 255

    pp = Path(str(tmpdir))

    mm = write_gtiff(pp/'absent_nodata.tiff', xx, nodata=None)

    yy = dc_read(mm.path, gbox=mm.gbox, fallback_nodata=None)
    np.testing.assert_array_equal(xx, yy)

    # fallback nodata is outside source range so it shouldn't be used
    yy = dc_read(mm.path, gbox=mm.gbox, fallback_nodata=-1, dst_nodata=-999, dtype='int16')
    np.testing.assert_array_equal(xx.astype('int16'), yy)

    # treat zeros as no-data + type conversion while reading
    yy_expect = xx.copy().astype('int16')
    yy_expect[xx == 0] = -999
    assert set(yy_expect.ravel()) == {-999, 255}

    yy = dc_read(mm.path, fallback_nodata=0, dst_nodata=-999, dtype='int16')
    np.testing.assert_array_equal(yy_expect, yy)

    # now check that file nodata is used instead of fallback
    mm = write_gtiff(pp/'with_nodata.tiff', xx, nodata=33)
    yy = dc_read(mm.path, fallback_nodata=0, dst_nodata=-999, dtype='int16')

    np.testing.assert_array_equal(xx, yy)

    yy = dc_read(mm.path)
    np.testing.assert_array_equal(xx, yy)


def test_rio_driver_specifics():
    assert _url2rasterio('file:///f.nc', 'NetCDF', 'band') == 'NetCDF:"/f.nc":band'
    assert _url2rasterio('file:///f.nc', 'HDF5', 'band') == 'HDF5:"/f.nc":band'
    assert _url2rasterio('file:///f.nc', 'HDF4_EOS:EOS_GRID', 'band') == 'HDF4_EOS:EOS_GRID:"/f.nc":band'
    assert _url2rasterio('file:///f.tiff', 'GeoTIFF', None) == '/f.tiff'
    s3_url = 's3://bucket/file'
    assert _url2rasterio(s3_url, 'GeoTIFF', None) is s3_url

    vsi_url = '/vsicurl/https://host.tld/path'
    assert _url2rasterio(vsi_url, 'GeoTIFF', None) is vsi_url
    assert _url2rasterio(vsi_url, 'NetCDF', 'aa') == 'NetCDF:"{}":aa'.format(vsi_url)
    assert _url2rasterio(vsi_url, 'HDF5', 'aa') == 'HDF5:"{}":aa'.format(vsi_url)

    with pytest.raises(ValueError):
        _url2rasterio('file:///f.nc', 'NetCDF', None)

    with pytest.raises(RuntimeError):
        _url2rasterio('http://example.com/f.nc', 'NetCDF', 'aa')

    with pytest.raises(ValueError):
        _url2rasterio('/some/path/', 'GeoTIFF', None)

    with pytest.raises(ValueError):
        _url2rasterio('/some/path/', 'NetCDF', 'aa')

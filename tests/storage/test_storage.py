from __future__ import absolute_import, division, print_function

import numpy
import netCDF4
from affine import Affine, identity
import xarray
import mock
import pytest
from contextlib import contextmanager

import rasterio.warp

import datacube
from datacube.utils import geometry
from datacube.storage.storage import write_dataset_to_netcdf, reproject_and_fuse, read_from_source, Resampling
from datacube.storage.storage import NetCDFDataSource

GEO_PROJ = 'GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563,AUTHORITY["EPSG","7030"]],' \
           'AUTHORITY["EPSG","6326"]],PRIMEM["Greenwich",0],UNIT["degree",0.0174532925199433],' \
           'AUTHORITY["EPSG","4326"]]'


def test_write_dataset_to_netcdf(tmpnetcdf_filename):
    affine = Affine.scale(0.1, 0.1) * Affine.translation(20, 30)
    geobox = geometry.GeoBox(100, 100, affine, geometry.CRS(GEO_PROJ))
    dataset = xarray.Dataset(attrs={'extent': geobox.extent, 'crs': geobox.crs})
    for name, coord in geobox.coordinates.items():
        dataset[name] = (name, coord.values, {'units': coord.units, 'crs': geobox.crs})

    dataset['B10'] = (geobox.dimensions,
                      numpy.arange(10000, dtype='int16').reshape(geobox.shape),
                      {'nodata': 0, 'units': '1', 'crs': geobox.crs})

    write_dataset_to_netcdf(dataset, tmpnetcdf_filename, global_attributes={'foo': 'bar'},
                            variable_params={'B10': {'attrs': {'abc': 'xyz'}}})

    with netCDF4.Dataset(tmpnetcdf_filename) as nco:
        nco.set_auto_mask(False)
        assert 'B10' in nco.variables
        var = nco.variables['B10']
        assert (var[:] == dataset['B10'].values).all()

        assert 'foo' in nco.ncattrs()
        assert nco.getncattr('foo') == 'bar'

        assert 'abc' in var.ncattrs()
        assert var.getncattr('abc') == 'xyz'


def test_netcdf_source(tmpnetcdf_filename):
    affine = Affine.scale(0.1, 0.1) * Affine.translation(20, 30)
    geobox = geometry.GeoBox(110, 100, affine, geometry.CRS(GEO_PROJ))
    dataset = xarray.Dataset(attrs={'extent': geobox.extent, 'crs': geobox.crs})
    for name, coord in geobox.coordinates.items():
        dataset[name] = (name, coord.values, {'units': coord.units, 'crs': geobox.crs})

    dataset['B10'] = (geobox.dimensions,
                      numpy.arange(11000, dtype='int16').reshape(geobox.shape),
                      {'nodata': 0, 'units': '1', 'crs': geobox.crs})

    write_dataset_to_netcdf(dataset, tmpnetcdf_filename, global_attributes={'foo': 'bar'},
                            variable_params={'B10': {'attrs': {'abc': 'xyz'}}})

    with netCDF4.Dataset(tmpnetcdf_filename) as nco:
        nco.set_auto_mask(False)
        source = NetCDFDataSource(nco, 'B10')
        assert source.crs == geobox.crs
        assert source.transform.almost_equals(affine)
        assert (source.read() == dataset['B10']).all()

        dest = numpy.empty((60, 50))
        source.reproject(dest, affine, geobox.crs, 0, Resampling.nearest)
        assert (dest == dataset['B10'][:60, :50]).all()

        source.reproject(dest, affine * Affine.translation(10, 10), geobox.crs, 0, Resampling.nearest)
        assert (dest == dataset['B10'][10:70, 10:60]).all()

        source.reproject(dest, affine * Affine.translation(-10, -10), geobox.crs, 0, Resampling.nearest)
        assert (dest[10:, 10:] == dataset['B10'][:50, :40]).all()

        dest = numpy.empty((200, 200))
        source.reproject(dest, affine, geobox.crs, 0, Resampling.nearest)
        assert (dest[:100, :110] == dataset['B10']).all()

        source.reproject(dest, affine * Affine.translation(10, 10), geobox.crs, 0, Resampling.nearest)
        assert (dest[:90, :100] == dataset['B10'][10:, 10:]).all()

        source.reproject(dest, affine * Affine.translation(-10, -10), geobox.crs, 0, Resampling.nearest)
        assert (dest[10:110, 10:120] == dataset['B10']).all()

        source.reproject(dest, affine * Affine.scale(2, 2), geobox.crs, 0, Resampling.nearest)
        assert (dest[:50, :55] == dataset['B10'][1::2, 1::2]).all()

        source.reproject(dest, affine * Affine.scale(2, 2) * Affine.translation(10, 10),
                         geobox.crs, 0, Resampling.nearest)
        assert (dest[:40, :45] == dataset['B10'][21::2, 21::2]).all()

        source.reproject(dest, affine * Affine.scale(2, 2) * Affine.translation(-10, -10),
                         geobox.crs, 0, Resampling.nearest)
        assert (dest[10:60, 10:65] == dataset['B10'][1::2, 1::2]).all()


def test_first_source_is_priority_in_reproject_and_fuse():
    crs = mock.MagicMock()
    shape = (2, 2)
    no_data = -1

    source1 = _mock_datasetsource([[1, 1], [1, 1]], crs=crs, shape=shape)
    source2 = _mock_datasetsource([[2, 2], [2, 2]], crs=crs, shape=shape)
    sources = [source1, source2]

    output_data = numpy.full(shape, fill_value=no_data, dtype='int16')
    reproject_and_fuse(sources, output_data, dst_transform=identity, dst_projection=crs, dst_nodata=no_data)

    assert (output_data == 1).all()


def test_second_source_used_when_first_is_empty():
    crs = mock.MagicMock()
    shape = (2, 2)
    no_data = -1

    source1 = _mock_datasetsource([[-1, -1], [-1, -1]], crs=crs, shape=shape)
    source2 = _mock_datasetsource([[2, 2], [2, 2]], crs=crs, shape=shape)
    sources = [source1, source2]

    output_data = numpy.full(shape, fill_value=no_data, dtype='int16')
    reproject_and_fuse(sources, output_data, dst_transform=identity, dst_projection=crs, dst_nodata=no_data)

    assert (output_data == 2).all()


def test_mixed_result_when_first_source_partially_empty():
    crs = mock.MagicMock()
    shape = (2, 2)
    no_data = -1

    source1 = _mock_datasetsource([[1, 1], [no_data, no_data]], crs=crs, shape=shape)
    source2 = _mock_datasetsource([[2, 2], [2, 2]], crs=crs, shape=shape)
    sources = [source1, source2]

    output_data = numpy.full(shape, fill_value=no_data, dtype='int16')
    reproject_and_fuse(sources, output_data, dst_transform=identity, dst_projection=crs, dst_nodata=no_data)

    assert (output_data == [[1, 1], [2, 2]]).all()


def _mock_datasetsource(value, crs=None, shape=(2, 2)):
    crs = crs or mock.MagicMock()
    dataset_source = mock.MagicMock()
    rio_reader = dataset_source.open.return_value.__enter__.return_value
    rio_reader.crs = crs
    rio_reader.transform = identity
    rio_reader.shape = shape
    rio_reader.read.return_value = numpy.array(value)

    # Use the following if a reproject were to be required
    # def fill_array(dest, *args, **kwargs):
    #     dest[:] = value
    # rio_reader.reproject.side_effect = fill_array
    return dataset_source


def test_read_from_broken_source():
    crs = mock.MagicMock()
    shape = (2, 2)
    no_data = -1

    source1 = _mock_datasetsource([[1, 1], [no_data, no_data]], crs=crs, shape=shape)
    source2 = _mock_datasetsource([[2, 2], [2, 2]], crs=crs, shape=shape)
    sources = [source1, source2]

    rio_reader = source1.open.return_value.__enter__.return_value
    rio_reader.read.side_effect = OSError('Read or write failed')

    output_data = numpy.full(shape, fill_value=no_data, dtype='int16')

    # Check exception is raised
    with pytest.raises(OSError):
        reproject_and_fuse(sources, output_data, dst_transform=identity,
                           dst_projection=crs, dst_nodata=no_data)

    # Check can ignore errors
    reproject_and_fuse(sources, output_data, dst_transform=identity,
                       dst_projection=crs, dst_nodata=no_data, skip_broken_datasets=True)

    assert (output_data == [[2, 2], [2, 2]]).all()


def _create_broken_netcdf(tmpdir):
    import os
    output_path = str(tmpdir / 'broken_netcdf_file.nc')
    with netCDF4.Dataset('broken_netcdf_file.nc', 'w') as nco:
        nco.createDimension('x', 50)
        nco.createDimension('y', 50)
        nco.createVariable('blank', 'int16', ('y', 'x'))

    with open(output_path, 'rb+') as filehandle:
        filehandle.seek(-3, os.SEEK_END)
        filehandle.truncate()

    with netCDF4.Dataset(output_path) as nco:
        blank = nco.data_vars['blank']


class FakeDataSource(object):
    def __init__(self):
        self.crs = geometry.CRS('EPSG:4326')
        self.transform = Affine(0.25, 0, 100, 0, -0.25, -30)
        self.nodata = -999
        self.shape = (613, 597)

        self.data = numpy.full(self.shape, self.nodata, dtype='int16')
        self.data[:512, :512] = numpy.arange(512) + numpy.arange(512).reshape((512, 1))

    def read(self, window=None, out_shape=None):
        data = self.data
        if window:
            data = self.data[slice(*window[0]), slice(*window[1])]
        if out_shape:
            xidx = ((numpy.arange(out_shape[1])+0.5)*(data.shape[1]/out_shape[1])-0.5).round().astype('int')
            yidx = ((numpy.arange(out_shape[0])+0.5)*(data.shape[0]/out_shape[0])-0.5).round().astype('int')
            data = data[numpy.meshgrid(yidx, xidx, indexing='ij')]
        return data

    def reproject(self, dest, dst_transform, dst_crs, dst_nodata, resampling, **kwargs):
        return rasterio.warp.reproject(self.data,
                                       dest,
                                       src_transform=self.transform,
                                       src_crs=str(self.crs),
                                       src_nodata=self.nodata,
                                       dst_transform=dst_transform,
                                       dst_crs=str(dst_crs),
                                       dst_nodata=dst_nodata,
                                       resampling=resampling,
                                       **kwargs)


def _test_helper(source, dst_shape, dst_dtype, dst_transform, dst_nodata, dst_projection, resampling):
    expected = numpy.empty(dst_shape, dtype=dst_dtype)
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

    result = numpy.empty(dst_shape, dtype=dst_dtype)
    with datacube.set_options(reproject_threads=1):
        read_from_source(source,
                         result,
                         dst_transform=dst_transform,
                         dst_nodata=dst_nodata,
                         dst_projection=dst_projection,
                         resampling=resampling)

    assert numpy.isclose(result, expected, atol=0, rtol=0.05, equal_nan=True).all()
    return result


def test_read_from_source():
    data_source = FakeDataSource()

    @contextmanager
    def fake_open():
        yield data_source
    source = mock.Mock()
    source.open = fake_open

    # one-to-one copy
    _test_helper(source,
                 dst_shape=data_source.shape,
                 dst_dtype=data_source.data.dtype,
                 dst_transform=data_source.transform,
                 dst_nodata=data_source.nodata,
                 dst_projection=data_source.crs,
                 resampling=Resampling.nearest)

    # change dtype
    _test_helper(source,
                 dst_shape=data_source.shape,
                 dst_dtype='int32',
                 dst_transform=data_source.transform,
                 dst_nodata=data_source.nodata,
                 dst_projection=data_source.crs,
                 resampling=Resampling.nearest)

    # change nodata
    _test_helper(source,
                 dst_shape=data_source.shape,
                 dst_dtype='float32',
                 dst_transform=data_source.transform,
                 dst_nodata=float('nan'),
                 dst_projection=data_source.crs,
                 resampling=Resampling.nearest)

    # different offsets/sizes
    _test_helper(source,
                 dst_shape=(517, 557),
                 dst_dtype='float32',
                 dst_transform=data_source.transform * Affine.translation(-200, -200),
                 dst_nodata=float('nan'),
                 dst_projection=data_source.crs,
                 resampling=Resampling.nearest)

    _test_helper(source,
                 dst_shape=(807, 879),
                 dst_dtype='float32',
                 dst_transform=data_source.transform * Affine.translation(200, 200),
                 dst_nodata=float('nan'),
                 dst_projection=data_source.crs,
                 resampling=Resampling.nearest)

    _test_helper(source,
                 dst_shape=(807, 879),
                 dst_dtype='float32',
                 dst_transform=data_source.transform * Affine.translation(1500, -1500),
                 dst_nodata=float('nan'),
                 dst_projection=data_source.crs,
                 resampling=Resampling.nearest)

    # flip axis
    _test_helper(source,
                 dst_shape=(517, 557),
                 dst_dtype='float32',
                 dst_transform=data_source.transform * Affine.translation(0, 512) * Affine.scale(1, -1),
                 dst_nodata=float('nan'),
                 dst_projection=data_source.crs,
                 resampling=Resampling.nearest)

    _test_helper(source,
                 dst_shape=(517, 557),
                 dst_dtype='float32',
                 dst_transform=data_source.transform * Affine.translation(512, 0) * Affine.scale(-1, 1),
                 dst_nodata=float('nan'),
                 dst_projection=data_source.crs,
                 resampling=Resampling.nearest)

    # scale
    _test_helper(source,
                 dst_shape=(250, 500),
                 dst_dtype='float32',
                 dst_transform=data_source.transform * Affine.scale(2, 4),
                 dst_nodata=float('nan'),
                 dst_projection=data_source.crs,
                 resampling=Resampling.nearest)

    _test_helper(source,
                 dst_shape=(500, 250),
                 dst_dtype='float32',
                 dst_transform=data_source.transform * Affine.scale(4, 2),
                 dst_nodata=float('nan'),
                 dst_projection=data_source.crs,
                 resampling=Resampling.cubic)

    _test_helper(source,
                 dst_shape=(67, 35),
                 dst_dtype='float32',
                 dst_transform=data_source.transform * Affine.scale(16, 8),
                 dst_nodata=float('nan'),
                 dst_projection=data_source.crs,
                 resampling=Resampling.cubic)

    _test_helper(source,
                 dst_shape=(35, 67),
                 dst_dtype='float32',
                 dst_transform=data_source.transform * Affine.translation(27, 35) * Affine.scale(8, 16),
                 dst_nodata=float('nan'),
                 dst_projection=data_source.crs,
                 resampling=Resampling.cubic)

    _test_helper(source,
                 dst_shape=(35, 67),
                 dst_dtype='float32',
                 dst_transform=data_source.transform * Affine.translation(-13, -27) * Affine.scale(8, 16),
                 dst_nodata=float('nan'),
                 dst_projection=data_source.crs,
                 resampling=Resampling.cubic)

    # scale + flip
    _test_helper(source,
                 dst_shape=(35, 67),
                 dst_dtype='float32',
                 dst_transform=data_source.transform * Affine.translation(15, 512+17) * Affine.scale(8, -16),
                 dst_nodata=float('nan'),
                 dst_projection=data_source.crs,
                 resampling=Resampling.cubic)

    _test_helper(source,
                 dst_shape=(67, 35),
                 dst_dtype='float32',
                 dst_transform=data_source.transform * Affine.translation(512-23, -29) * Affine.scale(-16, 8),
                 dst_nodata=float('nan'),
                 dst_projection=data_source.crs,
                 resampling=Resampling.cubic)

    # TODO: crs change

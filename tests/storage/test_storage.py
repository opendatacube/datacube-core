from __future__ import absolute_import, division, print_function

import numpy
import netCDF4
from affine import Affine, identity
import xarray
import mock
import pytest
from contextlib import contextmanager

import rasterio.warp

from datacube.model import GeoBox, CRS
from datacube.storage.storage import write_dataset_to_netcdf, reproject_and_fuse, read_from_source, Resampling

GEO_PROJ = 'GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563,AUTHORITY["EPSG","7030"]],' \
           'AUTHORITY["EPSG","6326"]],PRIMEM["Greenwich",0],UNIT["degree",0.0174532925199433],' \
           'AUTHORITY["EPSG","4326"]]'


def test_write_dataset_to_netcdf(tmpnetcdf_filename):
    affine = Affine.scale(0.1, 0.1) * Affine.translation(20, 30)
    geobox = GeoBox(100, 100, affine, CRS(GEO_PROJ))
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
        self.crs = CRS('EPSG:4326')
        self.transform = Affine(0.25, 0, 100, 0, -0.25, -30)
        self.nodata = -999
        self.shape = (121, 143)

        self.data = numpy.full(self.shape, self.nodata, dtype='int16')
        self.data[:50, :50] = 100
        self.data[:50, 50:100] = 200
        self.data[50:100, :50] = 300
        self.data[50:100, 50:100] = 400

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


def test_read_from_source():
    data_source = FakeDataSource()

    @contextmanager
    def open():
        yield data_source
    source = mock.Mock()
    source.open = open

    # one-to-one copy
    dest = numpy.empty(data_source.shape, dtype='int16')
    read_from_source(source,
                     dest,
                     dst_transform=data_source.transform,
                     dst_nodata=data_source.nodata,
                     dst_projection=data_source.crs,
                     resampling=Resampling.nearest)

    assert (dest == data_source.data).all()

    # change dtype
    dest = numpy.empty(data_source.shape, dtype='int32')
    read_from_source(source,
                     dest,
                     dst_transform=data_source.transform,
                     dst_nodata=data_source.nodata,
                     dst_projection=data_source.crs,
                     resampling=Resampling.nearest)

    assert (dest == data_source.data).all()

    # change nodata
    dest = numpy.empty(data_source.shape, dtype='float32')
    read_from_source(source,
                     dest,
                     dst_transform=data_source.transform,
                     dst_nodata=float('nan'),
                     dst_projection=data_source.crs,
                     resampling=Resampling.nearest)

    assert (dest[:100, :100] == data_source.data[:100, :100]).all()
    assert numpy.isnan(dest[100:, 100:]).all()

    # different offsets/sizes
    dest = numpy.empty((100, 100), dtype='float32')
    read_from_source(source,
                     dest,
                     dst_transform=data_source.transform*Affine.translation(-20, -20),
                     dst_nodata=float('nan'),
                     dst_projection=data_source.crs,
                     resampling=Resampling.nearest)
    assert (dest[20:100, 20:100] == data_source.data[:80, :80]).all()

    dest = numpy.empty((200, 200), dtype='float32')
    read_from_source(source,
                     dest,
                     dst_transform=data_source.transform*Affine.translation(20, 20),
                     dst_nodata=float('nan'),
                     dst_projection=data_source.crs,
                     resampling=Resampling.nearest)
    assert (dest[:80, :80] == data_source.data[20:100, 20:100]).all()

    dest = numpy.empty((200, 200), dtype='float32')
    read_from_source(source,
                     dest,
                     dst_transform=data_source.transform*Affine.translation(500, -500),
                     dst_nodata=float('nan'),
                     dst_projection=data_source.crs,
                     resampling=Resampling.nearest)
    assert numpy.isnan(dest).all()

    # flip axis
    dest = numpy.empty((100, 100), dtype='float32')
    read_from_source(source,
                     dest,
                     dst_transform=data_source.transform*Affine.translation(0, 100)*Affine.scale(1, -1),
                     dst_nodata=float('nan'),
                     dst_projection=data_source.crs,
                     resampling=Resampling.nearest)
    assert (dest[::-1, :] == data_source.data[:100, :100]).all()

    dest = numpy.empty((100, 100), dtype='float32')
    read_from_source(source,
                     dest,
                     dst_transform=data_source.transform*Affine.translation(100, 0)*Affine.scale(-1, 1),
                     dst_nodata=float('nan'),
                     dst_projection=data_source.crs,
                     resampling=Resampling.nearest)
    assert (dest[:, ::-1] == data_source.data[:100, :100]).all()

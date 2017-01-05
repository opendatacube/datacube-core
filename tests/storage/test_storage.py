from __future__ import absolute_import, division, print_function

import numpy
import netCDF4
from affine import Affine, identity
import xarray
import mock
import pytest

from datacube.model import GeoBox, CRS
from datacube.storage.storage import write_dataset_to_netcdf, reproject_and_fuse

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

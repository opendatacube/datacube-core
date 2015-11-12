from __future__ import absolute_import

from pytest import fixture

from datacube.storage.netcdf_indexer import index_netcdfs


@fixture
def example_netcdf_path(request):
    return str(request.fspath.dirpath('data/sample_tile.nc'))


def test_create_sample_netcdf_from_gdalds(tmpdir, example_netcdf_path):
    # filename = str(tmpdir.join('testfile_gdal.nc'))

    filenames = [example_netcdf_path, example_netcdf_path]

    files_metadata = index_netcdfs(filenames)

    assert files_metadata
    assert len(files_metadata) == 1

    required_attrs = {
        'coordinates': ('dtype', 'begin', 'end', 'length'),
        'measurements': ('units', 'dtype', 'dimensions', 'ndv')
    }

    # This seemed like a good idea... it no longer does but it works
    for filename, filedata in files_metadata.items():
        for name, fields in required_attrs.items():
            for field_name, field_data in filedata[name].items():
                for reqd_field in fields:
                    assert reqd_field in field_data
                    assert field_data[reqd_field]


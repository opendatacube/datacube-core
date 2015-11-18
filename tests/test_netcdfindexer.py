from __future__ import absolute_import

from datetime import datetime

from datacube.storage.netcdf_indexer import index_netcdfs


def test_create_sample_netcdf_from_gdalds(tmpdir, example_netcdf_path):
    # filename = str(tmpdir.join('testfile_gdal.nc'))

    filenames = [example_netcdf_path, example_netcdf_path]

    files_metadata = index_netcdfs(filenames)

    assert files_metadata
    assert len(files_metadata) == 1

    reqd_coords_attrs = set(['dtype', 'begin', 'end', 'length'])
    reqd_measurements_attrs = set(['units', 'dtype', 'dimensions', 'nodata'])
    reqd_extents_attributes = set(
        ['geospatial_lat_max', 'geospatial_lat_min', 'geospatial_lon_max', 'geospatial_lon_min', 'time_min',
         'time_max'])

    for filename, filedata in files_metadata.items():
        for coordinate in filedata['coordinates'].values():
            assert reqd_coords_attrs.issubset(coordinate.keys())
        for measurement in filedata['measurements'].values():
            assert reqd_measurements_attrs.issubset(measurement.keys())

        assert reqd_extents_attributes.issubset(filedata['extents'].keys())

        assert type(filedata['extents']['time_min']) == datetime
        assert type(filedata['extents']['time_max']) == datetime

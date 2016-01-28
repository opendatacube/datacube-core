from __future__ import absolute_import

from datetime import datetime

from datacube.storage.netcdf_indexer import read_netcdf_structure


def test_create_sample_netcdf_from_gdalds(tmpdir, example_netcdf_path):
    # filename = str(tmpdir.join('testfile_gdal.nc'))

    filename = example_netcdf_path
    filedata = read_netcdf_structure(filename)

    reqd_coords_attrs = {'dtype', 'begin', 'end', 'length'}
    reqd_extents_attributes = {'geospatial_lat_max', 'geospatial_lat_min',
                               'geospatial_lon_max', 'geospatial_lon_min',
                               'time_min', 'time_max'}

    for coordinate in filedata['coordinates'].values():
        assert reqd_coords_attrs.issubset(coordinate.keys())

    assert reqd_extents_attributes.issubset(filedata['extents'].keys())

    assert type(filedata['extents']['time_min']) == datetime
    assert type(filedata['extents']['time_max']) == datetime

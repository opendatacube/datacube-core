from __future__ import print_function, absolute_import

from datetime import datetime

from affine import Affine
import numpy as np
import numpy.testing as npt
import netCDF4
import pytest

from datacube.model import TileSpec
from datacube.storage.netcdf_writer import create_netcdf_writer

GEO_PROJ = 'GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563,AUTHORITY["EPSG","7030"]],' \
           'AUTHORITY["EPSG","6326"]],PRIMEM["Greenwich",0],UNIT["degree",0.0174532925199433],' \
           'AUTHORITY["EPSG","4326"]]'

ALBERS_PROJ = """PROJCS["GDA94 / Australian Albers",
                        GEOGCS["GDA94",
                            DATUM["Geocentric_Datum_of_Australia_1994",
                                SPHEROID["GRS 1980",6378137,298.257222101,
                                    AUTHORITY["EPSG","7019"]],
                                TOWGS84[0,0,0,0,0,0,0],
                                AUTHORITY["EPSG","6283"]],
                            PRIMEM["Greenwich",0,
                                AUTHORITY["EPSG","8901"]],
                            UNIT["degree",0.01745329251994328,
                                AUTHORITY["EPSG","9122"]],
                            AUTHORITY["EPSG","4283"]],
                        UNIT["metre",1,
                            AUTHORITY["EPSG","9001"]],
                        PROJECTION["Albers_Conic_Equal_Area"],
                        PARAMETER["standard_parallel_1",-18],
                        PARAMETER["standard_parallel_2",-36],
                        PARAMETER["latitude_of_center",0],
                        PARAMETER["longitude_of_center",132],
                        PARAMETER["false_easting",0],
                        PARAMETER["false_northing",0],
                        AUTHORITY["EPSG","3577"],
                        AXIS["Easting",EAST],
                        AXIS["Northing",NORTH]]"""


GLOBAL_ATTRS = {'test_attribute': 'test_value'}

DATA_VARIABLES = ('B1', 'B2')
LAT_LON_COORDINATES = ('latitude', 'longitude')
PROJECTED_COORDINATES = ('x', 'y')
COMMON_VARIABLES = ('crs', 'time')

DATA_WIDTH = 400
DATA_HEIGHT = 200


@pytest.fixture
def tmpnetcdf_filename(tmpdir):
    filename = str(tmpdir.join('testfile_np.nc'))

    return filename


def test_create_albers_projection_netcdf(tmpnetcdf_filename):
    affine = Affine(25.0, 0.0, 100000, 0.0, -25, 100000)
    chunking = [('time', 1), ('y', 100), ('x', 100)]

    build_test_netcdf(tmpnetcdf_filename, affine, ALBERS_PROJ, chunking)

    EXPECTED_VARIABLES = COMMON_VARIABLES + DATA_VARIABLES + PROJECTED_COORDINATES

    # Perform some basic checks
    with netCDF4.Dataset(tmpnetcdf_filename) as nco:
        for var in EXPECTED_VARIABLES:
            assert var in nco.variables
        for k, v in GLOBAL_ATTRS.items():
            assert getattr(nco, k) == v

        assert len(nco.variables['time']) == 2
        assert len(nco.variables['x']) == DATA_WIDTH
        assert len(nco.variables['y']) == DATA_HEIGHT

        for varname in PROJECTED_COORDINATES:
            assert nco.variables[varname].standard_name == 'projection_%s_coordinate' % varname


def test_create_epsg4326_netcdf(tmpnetcdf_filename):
    X_RES = 1.0/DATA_WIDTH
    Y_RES = -1.0/DATA_HEIGHT
    affine = Affine(X_RES, 0.0, 151.0, 0.0, Y_RES, -29.0)
    chunking = [('time', 1), ('latitude', 100), ('longitude', 100)]

    build_test_netcdf(tmpnetcdf_filename, affine, GEO_PROJ, chunking)

    EXPECTED_VARIABLES = COMMON_VARIABLES + DATA_VARIABLES + LAT_LON_COORDINATES

    # Perform some basic checks
    with netCDF4.Dataset(tmpnetcdf_filename) as nco:
        for var in EXPECTED_VARIABLES:
            assert var in nco.variables
        for k, v in GLOBAL_ATTRS.items():
            assert getattr(nco, k) == v

        assert len(nco.variables['time']) == 2
        assert len(nco.variables['longitude']) == DATA_WIDTH
        assert len(nco.variables['latitude']) == DATA_HEIGHT
        npt.assert_almost_equal(nco.variables['longitude'][0], 151 + X_RES / 2)
        npt.assert_almost_equal(nco.variables['longitude'][-1], 152 - X_RES / 2)
        npt.assert_almost_equal(nco.variables['latitude'][0], -29 + Y_RES / 2)
        npt.assert_almost_equal(nco.variables['latitude'][-1], -30 - Y_RES / 2)

        assert nco.variables['B1'].shape == (2, DATA_HEIGHT, DATA_WIDTH)

        # Check GDAL Attributes
        assert np.allclose(nco.variables['crs'].GeoTransform, affine.to_gdal())
        assert nco.variables['crs'].spatial_ref == GEO_PROJ


def test_extra_measurement_attrs(tmpnetcdf_filename):
    affine = Affine(0.00025, 0.0, 151.0, 0.0, -0.0005, -29.0)
    chunking = [('time', 1), ('latitude', 100), ('longitude', 100)]

    def extended_measurement_descriptor(**attrs):
        if attrs['varname'] == 'B1':
            attrs['attrs'] = {
                'wavelength': '55 meters',
                'colour': 'Blue'
            }
        else:
            attrs['attrs'] = {
                'wavelength': '65 meters',
                'colour': 'Green'
            }

        return attrs

    build_test_netcdf(tmpnetcdf_filename, affine, GEO_PROJ, chunking,
                      make_measurement_descriptor=extended_measurement_descriptor)

    print(tmpnetcdf_filename)
    with netCDF4.Dataset(tmpnetcdf_filename) as nco:
        var = nco.variables['B1']
        assert var.wavelength == '55 meters'
        assert var.colour == 'Blue'

        var = nco.variables['B2']
        assert var.wavelength == '65 meters'
        assert var.colour == 'Green'


def build_test_netcdf(filename, affine, projection, chunking, make_measurement_descriptor=dict):
    tile_spec = TileSpec(projection, affine, DATA_HEIGHT, DATA_WIDTH, global_attrs=GLOBAL_ATTRS)

    dates = [datetime(2008, month, 1) for month in (1, 2)]
    ops = [(band, time_index) for band in (1,2) for time_index in (0,1)]

    ncwriter = create_netcdf_writer(filename, tile_spec)

    ncwriter.create_time_values(dates)

    for band, time_index in ops:
        data = np.empty([DATA_HEIGHT, DATA_WIDTH])
        data[:] = band
        bandname = 'B%s' % band
        measurement_descriptor = make_measurement_descriptor(varname=bandname, dtype='int16', nodata=-999)

        var = ncwriter.ensure_variable(measurement_descriptor, chunking)
        var[time_index] = data
    ncwriter.close()

from __future__ import print_function, absolute_import

import netCDF4
import numpy
import xarray as xr
import pytest
from hypothesis import given
from hypothesis.strategies import text
from tests.conftest import tmpnetcdf_filename as get_tmpnetcdf_filename
import string

from datacube.model import Variable
from datacube.storage.netcdf_writer import create_netcdf, create_coordinate, create_variable, netcdfy_data, \
    create_grid_mapping_variable, flag_mask_meanings
from datacube.storage.storage import write_dataset_to_netcdf
from datacube.utils import geometry, DatacubeException, read_strings_from_netcdf

GEO_PROJ = geometry.CRS("""GEOGCS["WGS 84",
                           DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563,AUTHORITY["EPSG","7030"]],
                           AUTHORITY["EPSG","6326"]],PRIMEM["Greenwich",0],UNIT["degree",0.0174532925199433],
                           AUTHORITY["EPSG","4326"]]""")

ALBERS_PROJ = geometry.CRS("""PROJCS["GDA94 / Australian Albers",
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
                                AXIS["Northing",NORTH]]""")

SINIS_PROJ = geometry.CRS("""PROJCS["Sinusoidal",
                                GEOGCS["GCS_Undefined",
                                        DATUM["Undefined",
                                        SPHEROID["User_Defined_Spheroid",6371007.181,0.0]],
                                        PRIMEM["Greenwich",0.0],
                                        UNIT["Degree",0.0174532925199433]],
                                PROJECTION["Sinusoidal"],
                                PARAMETER["False_Easting",0.0],
                                PARAMETER["False_Northing",0.0],
                                PARAMETER["Central_Meridian",0.0],
                                UNIT["Meter",1.0]]""")

LCC2_PROJ = geometry.CRS("""PROJCS["unnamed",
                               GEOGCS["WGS 84",
                                       DATUM["unknown",
                                       SPHEROID["WGS84",6378137,6556752.3141]],
                                       PRIMEM["Greenwich",0],
                                       UNIT["degree",0.0174532925199433]],
                               PROJECTION["Lambert_Conformal_Conic_2SP"],
                               PARAMETER["standard_parallel_1",17.5],
                               PARAMETER["standard_parallel_2",29.5],
                               PARAMETER["latitude_of_origin",12],
                               PARAMETER["central_meridian",-102],
                               PARAMETER["false_easting",2500000],
                               PARAMETER["false_northing",0]]""")

GLOBAL_ATTRS = {'test_attribute': 'test_value'}

DATA_VARIABLES = ('B1', 'B2')
LAT_LON_COORDINATES = ('latitude', 'longitude')
PROJECTED_COORDINATES = ('x', 'y')
COMMON_VARIABLES = ('crs', 'time')

DATA_WIDTH = 400
DATA_HEIGHT = 200


def _ensure_spheroid(var):
    assert 'semi_major_axis' in var.ncattrs()
    assert 'semi_minor_axis' in var.ncattrs()
    assert 'inverse_flattening' in var.ncattrs()


def _ensure_gdal(var):
    assert 'GeoTransform' in var.ncattrs()
    assert 'spatial_ref' in var.ncattrs()


def _ensure_geospatial(nco):
    assert 'geospatial_bounds' in nco.ncattrs()
    assert 'geospatial_bounds_crs' in nco.ncattrs()
    assert nco.getncattr('geospatial_bounds_crs') == "EPSG:4326"

    assert 'geospatial_lat_min' in nco.ncattrs()
    assert 'geospatial_lat_max' in nco.ncattrs()
    assert 'geospatial_lat_units' in nco.ncattrs()
    assert nco.getncattr('geospatial_lat_units') == "degrees_north"

    assert 'geospatial_lon_min' in nco.ncattrs()
    assert 'geospatial_lon_max' in nco.ncattrs()
    assert 'geospatial_lon_units' in nco.ncattrs()
    assert nco.getncattr('geospatial_lon_units') == "degrees_east"


def test_create_albers_projection_netcdf(tmpnetcdf_filename):
    nco = create_netcdf(tmpnetcdf_filename)
    create_coordinate(nco, 'x', numpy.array([1., 2., 3.]), 'm')
    create_coordinate(nco, 'y', numpy.array([1., 2., 3.]), 'm')
    create_grid_mapping_variable(nco, ALBERS_PROJ)
    nco.close()

    with netCDF4.Dataset(tmpnetcdf_filename) as nco:
        assert 'crs' in nco.variables
        assert nco['crs'].grid_mapping_name == 'albers_conical_equal_area'
        assert 'standard_parallel' in nco['crs'].ncattrs()
        assert 'longitude_of_central_meridian' in nco['crs'].ncattrs()
        assert 'latitude_of_projection_origin' in nco['crs'].ncattrs()
        _ensure_spheroid(nco['crs'])
        _ensure_gdal(nco['crs'])
        _ensure_geospatial(nco)


def test_create_lambert_conformal_conic_2sp_projection_netcdf(tmpnetcdf_filename):
    nco = create_netcdf(tmpnetcdf_filename)
    create_coordinate(nco, 'x', numpy.array([1., 2., 3.]), 'm')
    create_coordinate(nco, 'y', numpy.array([1., 2., 3.]), 'm')
    create_grid_mapping_variable(nco, LCC2_PROJ)
    nco.close()

    with netCDF4.Dataset(tmpnetcdf_filename) as nco:
        assert 'crs' in nco.variables
        assert nco['crs'].grid_mapping_name == 'lambert_conformal_conic'
        assert 'standard_parallel' in nco['crs'].ncattrs()
        assert 'longitude_of_central_meridian' in nco['crs'].ncattrs()
        assert 'latitude_of_projection_origin' in nco['crs'].ncattrs()
        assert 'false_easting' in nco['crs'].ncattrs()
        assert 'false_northing' in nco['crs'].ncattrs()
        _ensure_spheroid(nco['crs'])
        _ensure_gdal(nco['crs'])
        _ensure_geospatial(nco)


def test_create_epsg4326_netcdf(tmpnetcdf_filename):
    nco = create_netcdf(tmpnetcdf_filename)
    create_coordinate(nco, 'latitude', numpy.array([1., 2., 3.]), 'm')
    create_coordinate(nco, 'longitude', numpy.array([1., 2., 3.]), 'm')
    create_grid_mapping_variable(nco, GEO_PROJ)
    nco.close()

    with netCDF4.Dataset(tmpnetcdf_filename) as nco:
        assert 'crs' in nco.variables
        assert nco['crs'].grid_mapping_name == 'latitude_longitude'
        _ensure_spheroid(nco['crs'])
        _ensure_geospatial(nco)


def test_create_sinus_netcdf(tmpnetcdf_filename):
    nco = create_netcdf(tmpnetcdf_filename)
    create_coordinate(nco, 'x', numpy.array([1., 2., 3.]), 'm')
    create_coordinate(nco, 'y', numpy.array([1., 2., 3.]), 'm')
    create_grid_mapping_variable(nco, SINIS_PROJ)
    nco.close()

    with netCDF4.Dataset(tmpnetcdf_filename) as nco:
        assert 'crs' in nco.variables
        assert nco['crs'].grid_mapping_name == 'sinusoidal'
        assert 'longitude_of_central_meridian' in nco['crs'].ncattrs()
        _ensure_spheroid(nco['crs'])
        _ensure_geospatial(nco)


# Work around outstanding bug with hypothesis/pytest, where function level fixtures are only run once.
# Generate a new netcdf filename for each run, so that old files don't cause permission errors on windows
# due to antivirus software filesystem lag.
# See https://github.com/HypothesisWorks/hypothesis-python/issues/377
@given(s1=text(alphabet=string.printable, max_size=100),
       s2=text(alphabet=string.printable, max_size=100),
       s3=text(alphabet=string.printable, max_size=100))
def test_create_string_variable(tmpdir, s1, s2, s3):
    tmpnetcdf_filename = get_tmpnetcdf_filename(tmpdir)
    str_var = 'str_var'
    nco = create_netcdf(tmpnetcdf_filename)
    coord = create_coordinate(nco, 'greg', numpy.array([1.0, 3.0, 9.0]), 'cubic gregs')
    assert coord is not None

    dtype = numpy.dtype('S100')
    data = numpy.array([s1, s2, s3], dtype=dtype)

    var = create_variable(nco, str_var, Variable(dtype, None, ('greg',), None))
    var[:] = netcdfy_data(data)
    nco.close()

    with netCDF4.Dataset(tmpnetcdf_filename) as nco:
        assert str_var in nco.variables

    for returned, expected in zip(read_strings_from_netcdf(tmpnetcdf_filename, variable=str_var), (s1, s2, s3)):
        assert returned == expected


def test_chunksizes(tmpnetcdf_filename):
    nco = create_netcdf(tmpnetcdf_filename)

    x = numpy.arange(3, dtype='float32')
    y = numpy.arange(5, dtype='float32')

    coord1 = create_coordinate(nco, 'x', x, 'm')
    coord2 = create_coordinate(nco, 'y', y, 'm')

    assert coord1 is not None and coord2 is not None

    no_chunks = create_variable(nco, 'no_chunks',
                                Variable(numpy.dtype('int16'), None, ('x', 'y'), None))

    min_max_chunks = create_variable(nco, 'min_max_chunks',
                                     Variable(numpy.dtype('int16'), None, ('x', 'y'), None),
                                     chunksizes=(2, 50))

    assert no_chunks is not None
    assert min_max_chunks is not None

    strings = numpy.array(["AAa", 'bbb', 'CcC'], dtype='S')
    strings = xr.DataArray(strings, dims=['x'], coords={'x': x})
    create_variable(nco, 'strings_unchunked', strings)
    create_variable(nco, 'strings_chunked', strings, chunksizes=(1,))

    nco.close()

    with netCDF4.Dataset(tmpnetcdf_filename) as nco:
        assert nco['no_chunks'].chunking() == 'contiguous'
        assert nco['min_max_chunks'].chunking() == [2, 5]
        assert nco['strings_unchunked'].chunking() == 'contiguous'
        assert nco['strings_chunked'].chunking() == [1, 3]


EXAMPLE_FLAGS_DEF = {
        'band_1_saturated': {
            'bits': 0,
            'values': {
                0: True,
                1: False
            },
            'description': 'Band 1 is saturated'},
        'band_2_saturated': {
            'bits': 1,
            'values': {
                0: True,
                1: False
            },
            'description': 'Band 2 is saturated'},
        'band_3_saturated': {
            'bits': 2,
            'values': {
                0: True,
                1: False
            },
            'description': 'Band 3 is saturated'},
        'land_sea': {
            'bits': 9,
            'values': {
                0: 'sea',
                1: 'land'
            },
            'description': 'Land/Sea observation'},
    }


def test_measurements_model_netcdfflags():
    masks, valid_range, meanings = flag_mask_meanings(EXAMPLE_FLAGS_DEF)
    assert ([0, 1023] == valid_range).all()
    assert ([1, 2, 4, 512] == masks).all()
    assert 'no_band_1_saturated no_band_2_saturated no_band_3_saturated land' == meanings


def test_useful_error_on_write_empty_dataset(tmpnetcdf_filename):
    with pytest.raises(DatacubeException) as excinfo:
        ds = xr.Dataset()
        write_dataset_to_netcdf(ds, tmpnetcdf_filename)
    assert 'empty' in str(excinfo.value)

    with pytest.raises(DatacubeException) as excinfo:
        ds = xr.Dataset(data_vars={'blue': (('time',), numpy.array([0, 1, 2]))})
        write_dataset_to_netcdf(ds, tmpnetcdf_filename)
    assert 'CRS' in str(excinfo.value)

# coding=utf-8
"""
Create netCDF4 Storage Units and write data to them
"""
from __future__ import absolute_import

from datetime import datetime
import logging

import netCDF4

from datacube import __version__
from datacube.compat import string_types

_LOG = logging.getLogger(__name__)

_STANDARD_COORDINATES = {
    'longitude': {
        'standard_name': 'longitude',
        'long_name': 'longitude',
        'axis': 'X'
    },
    'latitude': {
        'standard_name': 'latitude',
        'long_name': 'latitude',
        'axis': 'Y'
    },
    'x': {
        'standard_name': 'projection_x_coordinate',
        'long_name': 'x coordinate of projection',
        'axis': 'X'
    },
    'y': {
        'standard_name': 'projection_y_coordinate',
        'long_name': 'y coordinate of projection',
        'axis': 'Y'
    },
    'time': {
        'standard_name': 'time',
        'long_name': 'Time, unix time-stamp',
        'axis': 'T',
        'calendar': 'standard'
    }
}


def create_netcdf(netcdf_path):
    nco = netCDF4.Dataset(netcdf_path, 'w')
    nco.date_created = datetime.today().isoformat()
    nco.Conventions = 'CF-1.6, ACDD-1.3'
    nco.history = ("NetCDF-CF file created by "
                   "datacube version '{}' at {:%Y%m%d}."
                   .format(__version__, datetime.utcnow()))
    return nco


def create_coordinate(nco, name, coord):
    """
    :type nco: netCDF4.Dataset
    :type name: str
    :type coord: datacube.model.Coordinate
    :return:
    """
    nco.createDimension(name, coord.length)
    var = nco.createVariable(name, coord.dtype, name)
    var.units = coord.units
    for key, value in _STANDARD_COORDINATES.get(name, {}).items():
        setattr(var, key, value)
    return var


def create_variable(nco, name, var, **kwargs):
    """
    :param nco:
    :param name:
    :type var: datacube.model.Variable
    :param kwargs:
    :return:
    """
    if 'chunksizes' in kwargs:
        maxsizes = [len(nco.dimensions[dim]) for dim in var.dimensions]
        kwargs['chunksizes'] = [min(chunksize, maxsize) if chunksize and maxsize else chunksize
                                for maxsize, chunksize in zip(maxsizes, kwargs['chunksizes'])]

    if var.dtype.kind == 'S' and var.dtype.itemsize > 1:
        nco.createDimension(name + '_nchar', size=var.dtype.itemsize)
        data_var = nco.createVariable(varname=name,
                                      datatype='S1',
                                      dimensions=tuple(var.dimensions) + (name + '_nchar',),
                                      fill_value=var.nodata,
                                      **kwargs)
    else:
        data_var = nco.createVariable(varname=name,
                                      datatype=var.dtype,
                                      dimensions=var.dimensions,
                                      fill_value=var.nodata,
                                      **kwargs)
        data_var.grid_mapping = 'crs'
    if var.units is not None:
        data_var.units = var.units
    data_var.set_auto_maskandscale(False)
    return data_var


class _EncodedStrings(object):
    def __init__(self, var):
        self._wrapped_obj = var

    def __getattr__(self, attr):
        return getattr(self._wrapped_obj, attr)

    def __setattr__(self, name, value):
        if isinstance(value, string_types):
            self._wrapped_obj[name] = value.encode('utf8')
        else:
            super(self.__class__, self).__setattr(name, value)


def _create_variable_safe_attributes(nco, *args, **kwargs):
    var = nco.createVariable(*args, **kwargs)

    return _EncodedStrings(var)


def _create_latlon_grid_mapping_variable(nco, crs):
    crs_var = nco.createVariable('crs', 'i4')
    crs_var.long_name = crs.GetAttrValue('GEOGCS')  # "Lon/Lat Coords in WGS84"
    crs_var.grid_mapping_name = 'latitude_longitude'
    crs_var.longitude_of_prime_meridian = 0.0
    return crs_var


def _write_albers_params(crs_var, crs):
    # http://spatialreference.org/ref/epsg/gda94-australian-albers/html/
    # http://cfconventions.org/Data/cf-conventions/cf-conventions-1.7/build/cf-conventions.html#appendix-grid-mappings
    crs_var.grid_mapping_name = 'albers_conical_equal_area'
    crs_var.standard_parallel = (crs.GetProjParm('standard_parallel_1'),
                                 crs.GetProjParm('standard_parallel_2'))
    crs_var.longitude_of_central_meridian = crs.GetProjParm('longitude_of_center')
    crs_var.latitude_of_projection_origin = crs.GetProjParm('latitude_of_center')


def _write_sinusoidal_params(crs_var, crs):
    crs_var.grid_mapping_name = 'sinusoidal'
    crs_var.longitude_of_central_meridian = crs.GetProjParm('central_meridian')


CRS_PARAM_WRITERS = {
    'albers_conic_equal_area': _write_albers_params,
    'sinusoidal': _write_sinusoidal_params
}


def _create_projected_grid_mapping_variable(nco, crs):
    grid_mapping_name = crs.GetAttrValue('PROJECTION').lower()
    if grid_mapping_name not in CRS_PARAM_WRITERS:
        raise ValueError('{} CRS is not supported'.format(grid_mapping_name))

    crs_var = nco.createVariable('crs', 'i4')
    CRS_PARAM_WRITERS[grid_mapping_name](crs_var, crs)

    crs_var.false_easting = crs.GetProjParm('false_easting')
    crs_var.false_northing = crs.GetProjParm('false_northing')
    crs_var.long_name = crs.GetAttrValue('PROJCS')

    return crs_var


def write_gdal_attributes(nco, crs, affine):
    crs_var = nco['crs']
    crs_var.spatial_ref = crs.ExportToWkt()
    crs_var.GeoTransform = affine.to_gdal()


def write_geographical_extents_attributes(nco, geo_extents):
    geo_extents = geo_extents + [geo_extents[0]]
    nco.geospatial_bounds = "POLYGON((" + ", ".join("{0} {1}".format(*p) for p in geo_extents) + "))"
    nco.geospatial_bounds_crs = "EPSG:4326"

    nco.geospatial_lat_min = min(lat for lon, lat in geo_extents)
    nco.geospatial_lat_max = max(lat for lon, lat in geo_extents)
    nco.geospatial_lat_units = "degrees_north"
    nco.geospatial_lon_min = min(lon for lon, lat in geo_extents)
    nco.geospatial_lon_max = max(lon for lon, lat in geo_extents)
    nco.geospatial_lon_units = "degrees_east"

    # TODO: broken anyway...
    # nco.geospatial_lat_resolution = "{} degrees".format(abs(geobox.affine.e))
    # nco.geospatial_lon_resolution = "{} degrees".format(abs(geobox.affine.a))


def create_grid_mapping_variable(nco, crs):
    if crs.IsGeographic():
        crs_var = _create_latlon_grid_mapping_variable(nco, crs)
    elif crs.IsProjected():
        crs_var = _create_projected_grid_mapping_variable(nco, crs)
    else:
        raise ValueError('Unknown CRS')
    crs_var.semi_major_axis = crs.GetSemiMajor()
    crs_var.semi_minor_axis = crs.GetSemiMinor()
    crs_var.inverse_flattening = crs.GetInvFlattening()
    crs_var.crs_wkt = crs.ExportToWkt()


def write_attribute(obj, key, value):
    if isinstance(value, string_types):
        value = value.encode('utf8')
    setattr(obj, key, value)


def netcdfy_data(data):
    if data.dtype.kind == 'S' and data.dtype.itemsize > 1:
        return data.view('S1').reshape(data.shape + (-1,))
    else:
        return data

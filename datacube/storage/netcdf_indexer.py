# coding=utf-8
"""
Extract description dictionaries from netCDF4 storage units
"""
from __future__ import absolute_import

import netCDF4

from .access.backends import NetCDF4StorageUnit
from .utils import namedtuples2dicts


def read_netcdf_structure(filename):
    """
    Read a netcdf4 file and return a dict describing its coordinates and variables

    :param filename:
    :return:
    """
    ncsu = NetCDF4StorageUnit.from_file(filename)

    extents = {k: ncsu.attributes[k] for k in
               [u'geospatial_lat_max', u'geospatial_lat_min', u'geospatial_lon_max', u'geospatial_lon_min']}

    time_units = ncsu.coordinates[u'time'].units

    extents['time_min'] = netCDF4.num2date(ncsu.coordinates['time'].begin, time_units)
    extents['time_max'] = netCDF4.num2date(ncsu.coordinates['time'].end, time_units)

    coordinates = namedtuples2dicts(ncsu.coordinates)

    return dict(coordinates=coordinates, extents=extents)

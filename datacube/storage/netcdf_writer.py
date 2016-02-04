# coding=utf-8
"""
Create netCDF4 Storage Units and write data to them
"""
from __future__ import absolute_import

import logging
from datetime import datetime

import netCDF4
import numpy
import yaml
try:
    from yaml import CSafeDumper as SafeDumper
except ImportError:
    from yaml import SafeDumper

from datacube import __version__
from datacube.storage.utils import datetime_to_seconds_since_1970
from datacube.model import Coordinate, Variable

_LOG = logging.getLogger(__name__)
DATASET_YAML_MAX_SIZE = 30000


def map_measurement_descriptor_parameters(measurement_descriptor):
    """Map measurement descriptor parameters to netcdf variable parameters"""
    md_to_netcdf = {'zlib': 'zlib',
                    'complevel': 'complevel',
                    'shuffle': 'shuffle',
                    'fletcher32': 'fletcher32',
                    'contiguous': 'contiguous'}
    params = {ncparam: measurement_descriptor[mdkey]
              for mdkey, ncparam in md_to_netcdf.items() if mdkey in measurement_descriptor}

    return params


def create_netcdf_writer(netcdf_path, geobox):
    if geobox.crs.IsGeographic():
        return GeographicNetCDFWriter(netcdf_path, geobox)
    elif geobox.crs.IsProjected():
        return ProjectedNetCDFWriter(netcdf_path, geobox)
    else:
        raise RuntimeError("Unknown projection")


class NetCDFWriter(object):
    """
    Create NetCDF4 Storage Units, with CF Compliant metadata.

    At the moment compliance depends on what is passed in to this class
    as internal compliance checks are not performed.

    :param str netcdf_path: File path at which to create this NetCDF file
    :param datacube.model.GeoBox geobox: Storage Unit definition
    :param num_times: The number of time values allowed to be stored. Unlimited by default.
    """

    def __init__(self, netcdf_path, geobox):
        self.netcdf_path = str(netcdf_path)

        self.nco = netCDF4.Dataset(self.netcdf_path, 'w')
        self.nco.date_created = datetime.today().isoformat()
        self.nco.Conventions = 'CF-1.6, ACDD-1.3'
        self.nco.history = ("NetCDF-CF file created by "
                            "datacube version '{}' at {:%Y%m%d}."
                            .format(__version__, datetime.utcnow()))

        self.create_coordinate_variables(geobox)
        create_grid_mapping_variable(self.nco, geobox.crs)
        write_gdal_geobox_attributes(self.nco, geobox)
        write_geographical_extents_attributes(self.nco, geobox)

    def __enter__(self):
        return self

    def __exit__(self, *optional_exception_arguments):
        self.close()

    def close(self):
        self.nco.close()

    def create_coordinate_variables(self, geobox):
        raise NotImplementedError()

    def add_global_attributes(self, global_attrs):
        for name, value in global_attrs.items():
            self.nco.setncattr(name, value)

    def create_time_values(self, time_values):
        self._create_time_dimension(len(time_values))
        times = self.nco.variables['time']
        for idx, val in enumerate(time_values):
            times[idx] = datetime_to_seconds_since_1970(val)

    def _create_time_dimension(self, time_length):
        """
        Create time dimension
        """
        create_coordinate(self.nco, 'time', Coordinate(numpy.dtype(numpy.float64), 0, 0, time_length,
                                                       'seconds since 1970-01-01 00:00:00'))

    def ensure_variable(self, measurement_descriptor, chunking):
        varname = measurement_descriptor['varname']
        if varname in self.nco.variables:
            # TODO: check that var matches
            return self.nco.variables[varname]
        return self._create_data_variable(measurement_descriptor, chunking=chunking)

    def add_source_metadata(self, time_index, metadata_docs):
        """
        Save YAML metadata documents into the `extra_metadata` variable

        :type time_index: int
        :param metadata_docs: List of metadata docs for this timestamp
        :type metadata_docs: list
        """
        if 'extra_metadata' not in self.nco.variables:
            self._create_extra_metadata_variable()
        yaml_str = yaml.dump_all(metadata_docs, Dumper=SafeDumper)
        self.nco.variables['extra_metadata'][time_index] = netCDF4.stringtoarr(yaml_str, DATASET_YAML_MAX_SIZE)

    def _create_extra_metadata_variable(self):
        self.nco.createDimension('nchar', size=DATASET_YAML_MAX_SIZE)
        extra_metadata_variable = self.nco.createVariable('extra_metadata', 'S1', ('time', 'nchar'))
        extra_metadata_variable.long_name = 'Detailed source dataset information'

    def _create_data_variable(self, measurement_descriptor, chunking):
        var = Variable(dtype=measurement_descriptor['dtype'],
                       nodata=measurement_descriptor['nodata'],
                       dimensions=[c[0] for c in chunking],
                       units=measurement_descriptor.get('units', '1'))
        params = map_measurement_descriptor_parameters(measurement_descriptor)
        params['chunksizes'] = [c[1] for c in chunking]

        data_var = create_variable(self.nco, measurement_descriptor['varname'], var, **params)
        data_var.grid_mapping = 'crs'

        # Copy extra attributes from the measurement descriptor onto the netcdf variable
        if 'attrs' in measurement_descriptor:
            for name, value in measurement_descriptor['attrs'].items():
                # Unicode or str, that is the netcdf4 question
                data_var.setncattr(str(name), str(value))

        return data_var


class ProjectedNetCDFWriter(NetCDFWriter):
    def create_coordinate_variables(self, geobox):
        coordinate_labels = geobox.coordinate_labels

        units = geobox.crs.GetAttrValue('UNIT')

        xvar = create_coordinate(self.nco, 'x',
                                 Coordinate(coordinate_labels['x'].dtype, 0, 0, coordinate_labels['x'].size, units))
        xvar[:] = coordinate_labels['x']

        yvar = create_coordinate(self.nco, 'y',
                                 Coordinate(coordinate_labels['y'].dtype, 0, 0, coordinate_labels['y'].size, units))
        yvar[:] = coordinate_labels['y']


class GeographicNetCDFWriter(NetCDFWriter):
    def create_coordinate_variables(self, geobox):
        coordinate_labels = geobox.coordinate_labels

        lon = create_coordinate(self.nco, 'longitude',
                                Coordinate(coordinate_labels['longitude'].dtype, 0, 0,
                                           coordinate_labels['longitude'].size, 'degrees_east'))
        lon[:] = coordinate_labels['longitude']

        lat = create_coordinate(self.nco, 'latitude',
                                Coordinate(coordinate_labels['latitude'].dtype, 0, 0,
                                           coordinate_labels['latitude'].size, 'degrees_north'))
        lat[:] = coordinate_labels['latitude']


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
    data_var = nco.createVariable(varname=name,
                                  datatype=var.dtype,
                                  dimensions=var.dimensions,
                                  fill_value=var.nodata,
                                  **kwargs)
    data_var.set_auto_maskandscale(False)
    data_var.units = var.units
    return data_var


def _create_latlon_grid_mapping_variable(nco, crs):
    crs_var = nco.createVariable('crs', 'i4')
    crs_var.long_name = crs.GetAttrValue('GEOGCS')  # "Lon/Lat Coords in WGS84"
    crs_var.grid_mapping_name = 'latitude_longitude'
    crs_var.longitude_of_prime_meridian = 0.0
    crs_var.semi_major_axis = crs.GetSemiMajor()
    crs_var.inverse_flattening = crs.GetInvFlattening()

    return crs_var


def _create_projected_grid_mapping_variable(nco, crs):
    grid_mapping_name = crs.GetAttrValue('PROJECTION').lower()

    if grid_mapping_name != 'albers_conic_equal_area':
        raise ValueError('{} CRS is not supported'.format(grid_mapping_name))

    # http://spatialreference.org/ref/epsg/gda94-australian-albers/html/
    # http://cfconventions.org/Data/cf-conventions/cf-conventions-1.7/build/cf-conventions.html#appendix-grid-mappings
    crs_var = nco.createVariable('crs', 'i4')
    crs_var.standard_parallel = (crs.GetProjParm('standard_parallel_1'),
                                 crs.GetProjParm('standard_parallel_2'))
    crs_var.longitude_of_central_meridian = crs.GetProjParm('longitude_of_center')
    crs_var.latitude_of_projection_origin = crs.GetProjParm('latitude_of_center')
    crs_var.false_easting = crs.GetProjParm('false_easting')
    crs_var.false_northing = crs.GetProjParm('false_northing')
    crs_var.grid_mapping_name = grid_mapping_name
    crs_var.long_name = crs.GetAttrValue('PROJCS')

    return crs_var


def write_gdal_geobox_attributes(nco, geobox):
    crs_var = nco['crs']
    crs_var.spatial_ref = geobox.crs.ExportToWkt()
    crs_var.GeoTransform = geobox.affine.to_gdal()


def write_geographical_extents_attributes(nco, geobox):
    geo_extents = geobox.geographic_extent
    geo_extents.append(geo_extents[0])
    nco.geospatial_bounds = "POLYGON((" + ", ".join("{0} {1}".format(*p) for p in geo_extents) + "))"
    nco.geospatial_bounds_crs = "EPSG:4326"

    geo_aabb = geobox.geographic_boundingbox
    nco.geospatial_lat_min = geo_aabb.bottom
    nco.geospatial_lat_max = geo_aabb.top
    nco.geospatial_lat_units = "degrees_north"
    nco.geospatial_lat_resolution = "{} degrees".format(abs(geobox.affine.e))
    nco.geospatial_lon_min = geo_aabb.left
    nco.geospatial_lon_max = geo_aabb.right
    nco.geospatial_lon_units = "degrees_east"
    nco.geospatial_lon_resolution = "{} degrees".format(abs(geobox.affine.a))


def create_grid_mapping_variable(nco, crs):
    if crs.IsGeographic():
        crs_var = _create_latlon_grid_mapping_variable(nco, crs)
    elif crs.IsProjected():
        crs_var = _create_projected_grid_mapping_variable(nco, crs)
    else:
        raise ValueError('Unknown CRS')
    crs_var.crs_wkt = crs.ExportToWkt()
    return crs_var

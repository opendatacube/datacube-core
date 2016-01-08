# coding=utf-8
"""
Create netCDF4 Storage Units and write data to them
"""
from __future__ import absolute_import

import logging
from datetime import datetime
from itertools import chain

from dateutil.tz import tzutc
import netCDF4
from osgeo import osr
import yaml

from datacube.model import VariableAlreadyExists

_LOG = logging.getLogger(__name__)


def _create_variable_params(measurement_descriptor):
    identical_keys = 'varname zlib complevel shuffle fletcher32 contiguous'
    mapped_keys = {'dtype': 'datatype', 'nodata': 'fill_value'}
    params = {key: measurement_descriptor[key] for key in identical_keys.split() if key in measurement_descriptor}
    for mdkey, cvparam in mapped_keys.items():
        params[cvparam] = measurement_descriptor[mdkey]

    if 'varname' not in params:
        raise Exception('Invalid measurement configuration', measurement_descriptor)

    return params


def _seconds_since_1970(dt):
    epoch = datetime(1970, 1, 1, 0, 0, 0, tzinfo=tzutc() if dt.tzinfo else None)
    return (dt - epoch).total_seconds()


def _grid_mapping_name(crs):
    if crs.IsGeographic():
        return 'latitude_longitude'
    elif crs.IsProjected():
        return crs.GetAttrValue('PROJECTION').lower()


class NetCDFWriter(object):
    """
    Create NetCDF4 Storage Units, with CF Compliant metadata.

    At the moment compliance depends on what is passed in to this class
    as internal compliance checks are not performed.

    :param str netcdf_path: File path at which to create this NetCDF file
    :param datacube.model.TileSpec tile_spec: Storage Unit definition
    :param num_times: The number of time values allowed to be stored. Unlimited by default.
    """

    def __init__(self, netcdf_path, tile_spec, num_times=None):
        netcdf_path = str(netcdf_path)

        self.nco = netCDF4.Dataset(netcdf_path, 'w')

        self.tile_spec = tile_spec
        self.netcdf_path = netcdf_path

        self._create_time_dimension(num_times)
        self._create_spatial_variables(tile_spec)
        self._set_global_attributes(tile_spec)

        # Create Variable Length Variable to store extra metadata
        self._extra_meta = self.nco.createVariable('extra_metadata', str, 'time')
        self._extra_meta.long_name = 'Detailed '

    def close(self):
        self.nco.close()

    def _create_time_dimension(self, time_length):
        """
        Create time dimension
        """
        self.nco.createDimension('time', time_length)
        timeo = self.nco.createVariable('time', 'double', 'time')
        timeo.units = 'seconds since 1970-01-01 00:00:00'
        timeo.standard_name = 'time'
        timeo.long_name = 'Time, unix time-stamp'
        timeo.calendar = 'standard'
        timeo.axis = "T"

    def _create_spatial_variables(self, tile_spec):
        crs = tile_spec.crs
        if crs.IsGeographic():
            self._create_geo_crs(crs)
            self._create_geo_variables(tile_spec)
        elif crs.IsProjected():
            self._create_proj_crs(crs)
            self._create_proj_variables(tile_spec, crs.GetAttrValue('UNIT'))
        else:
            raise Exception("Unknown projection")

    def _create_geo_variables(self, tile_spec):
        self.nco.createDimension('longitude', len(tile_spec.lons))
        self.nco.createDimension('latitude', len(tile_spec.lats))

        lon = self.nco.createVariable('longitude', 'double', 'longitude')
        lon.units = 'degrees_east'
        lon.standard_name = 'longitude'
        lon.long_name = 'longitude'
        lon.axis = "X"
        lon[:] = tile_spec.lons

        lat = self.nco.createVariable('latitude', 'double', 'latitude')
        lat.units = 'degrees_north'
        lat.standard_name = 'latitude'
        lat.long_name = 'latitude'
        lat.axis = "Y"
        lat[:] = tile_spec.lats

    def _create_geo_crs(self, crs):
        crs_var = self.nco.createVariable(_grid_mapping_name(crs), 'i4')
        crs_var.long_name = crs.GetAttrValue('GEOGCS')  # "Lon/Lat Coords in WGS84"
        crs_var.grid_mapping_name = _grid_mapping_name(crs)
        crs_var.longitude_of_prime_meridian = 0.0
        crs_var.semi_major_axis = crs.GetSemiMajor()
        crs_var.inverse_flattening = crs.GetInvFlattening()
        crs_var.spatial_ref = crs.ExportToWkt()  # GDAL variable
        crs_var.GeoTransform = self._gdal_geotransform()  # GDAL variable
        return crs_var

    def _gdal_geotransform(self):
        return self.tile_spec.affine.to_gdal()

    def _create_albers_crs(self, crs):
        # http://spatialreference.org/ref/epsg/gda94-australian-albers/html/
        # http://cfconventions.org/Data/cf-conventions/cf-conventions-1.7/build/cf-conventions.html#appendix-grid-mappings
        crs_var = self.nco.createVariable(_grid_mapping_name(crs), 'i4')
        crs_var.standard_parallel_1 = crs.GetProjParm('standard_parallel_1')
        crs_var.standard_parallel_2 = crs.GetProjParm('standard_parallel_2')
        crs_var.longitude_of_central_meridian = crs.GetProjParm('longitude_of_center')
        crs_var.latitude_of_projection_origin = crs.GetProjParm('latitude_of_center')
        crs_var.false_easting = crs.GetProjParm('false_easting')
        crs_var.false_northing = crs.GetProjParm('false_northing')
        crs_var.grid_mapping_name = _grid_mapping_name(crs)
        crs_var.long_name = crs.GetAttrValue('PROJCS')
        crs_var.spatial_ref = crs.ExportToWkt()  # GDAL variable
        crs_var.GeoTransform = self._gdal_geotransform()  # GDAL variable
        return crs_var

    def _create_proj_crs(self, crs):
        if _grid_mapping_name(crs) != 'albers_conic_equal_area':
            raise RuntimeError('%s CRS is not supported' % _grid_mapping_name(crs))
        return self._create_albers_crs(crs)

    def _create_proj_variables(self, tile_spec, units):
        self.nco.createDimension('x', tile_spec.width)
        self.nco.createDimension('y', tile_spec.height)

        xvar = self.nco.createVariable('x', 'double', 'x')
        xvar.long_name = 'x coordinate of projection'
        xvar.units = units
        xvar.standard_name = 'projection_x_coordinate'
        xvar[:] = tile_spec.xs

        yvar = self.nco.createVariable('y', 'double', 'y')
        yvar.long_name = 'y coordinate of projection'
        yvar.units = units
        yvar.standard_name = 'projection_y_coordinate'
        yvar[:] = tile_spec.ys

    def _create_proj_geo_variables(self, projection, tile_spec):
        wgs84 = osr.SpatialReference()
        wgs84.ImportFromEPSG(4326)
        to_wgs84 = osr.CoordinateTransformation(projection, wgs84)

        lats, lons, _ = zip(*[to_wgs84.TransformPoint(x, y) for y in tile_spec.ys for x in tile_spec.xs])

        lats_var = self.nco.createVariable('lat', 'double', ('y', 'x'))
        lats_var.long_name = 'latitude coordinate'
        lats_var.standard_name = 'latitude'
        lats_var.units = 'degrees north'
        lats_var[:] = lats_var

        lons_var = self.nco.createVariable('lon', 'double', ('y', 'x'))
        lons_var.long_name = 'longitude coordinate'
        lons_var.standard_name = 'longitude'
        lons_var.units = 'degrees east'
        lons_var[:] = lons_var

    def _set_global_attributes(self, tile_spec):
        """

        :type tile_spec: datacube.model.TileSpec
        """
        # ACDD Metadata (Recommended)
        extents = chain(tile_spec.extents, [tile_spec.extents[0]])
        self.nco.geospatial_bounds = "POLYGON((" + ", ".join("{0} {1}".format(*p) for p in extents) + "))"
        self.nco.geospatial_bounds_crs = "EPSG:4326"
        self.nco.geospatial_lat_min = tile_spec.lat_min
        self.nco.geospatial_lat_max = tile_spec.lat_max
        self.nco.geospatial_lat_units = "degrees_north"
        self.nco.geospatial_lat_resolution = "{} degrees".format(abs(tile_spec.lat_res))
        self.nco.geospatial_lon_min = tile_spec.lon_min
        self.nco.geospatial_lon_max = tile_spec.lon_max
        self.nco.geospatial_lon_units = "degrees_east"
        self.nco.geospatial_lon_resolution = "{} degrees".format(abs(tile_spec.lon_res))
        self.nco.date_created = datetime.today().isoformat()
        self.nco.history = "NetCDF-CF file created by agdc-v2 at {:%Y%m%d}.".format(datetime.utcnow())

        # Follow ACDD and CF Conventions
        self.nco.Conventions = 'CF-1.6, ACDD-1.3'

        # Attributes from Dataset. For NCI Reqs MUST contain at least title, summary, source, product_version
        for name, value in tile_spec.global_attrs.items():
            self.nco.setncattr(name, value)

    def find_or_create_time_index(self, insertion_time):
        """
        :type insertion_time: datetime
        :return:
        """
        times = self.nco.variables['time']

        if len(times) == 0:
            _LOG.debug('Inserting time %s', insertion_time)
            index = len(times)
            times[index] = _seconds_since_1970(insertion_time)
        else:
            index = netCDF4.date2index(insertion_time, times)  # Blow up for a different time

        return index

    def create_time_values(self, time_values):
        times = self.nco.variables['time']
        for idx, val in enumerate(time_values):
            times[idx] = _seconds_since_1970(val)

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
        self._extra_meta[time_index] = yaml.safe_dump_all(metadata_docs)

    def _create_data_variable(self, measurement_descriptor, chunking, units=None):
        params = _create_variable_params(measurement_descriptor)
        params['dimensions'] = [c[0] for c in chunking]
        params['chunksizes'] = [c[1] for c in chunking]
        data_var = self.nco.createVariable(**params)

        data_var.grid_mapping = _grid_mapping_name(self.tile_spec.crs)
        data_var.set_auto_maskandscale(False)

        if units:
            data_var.units = units

        return data_var

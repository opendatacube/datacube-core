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

from datacube.model import VariableAlreadyExists

_LOG = logging.getLogger(__name__)


def _seconds_since_1970(dt):
    epoch = datetime(1970, 1, 1, 0, 0, 0, tzinfo=tzutc() if dt.tzinfo else None)
    return (dt - epoch).total_seconds()


def _grid_mapping_name(projection):
    if projection.IsGeographic():
        return 'latitude_longitude'
    elif projection.IsProjected():
        return projection.GetAttrValue('PROJECTION').lower()


class NetCDFWriter(object):
    """
    Base class for creating a NetCDF file based upon GeoTIFF data.

    Sub-classes will create the NetCDF in different structures.
    """

    def __init__(self, netcdf_path, tile_spec, time_length=None):
        """

        :param netcdf_path:
        :type tile_spec:  datacube.model.TileSpec
        :param time_length:
        :return:
        """
        netcdf_path = str(netcdf_path)

        self.nco = netCDF4.Dataset(netcdf_path, 'w')

        self._tile_spec = tile_spec
        self.netcdf_path = netcdf_path

        self._create_time_dimension(time_length)
        self._create_spatial_variables(tile_spec)
        self._set_global_attributes(tile_spec)

        # Create Variable Length Variable to store extra metadata
        extra_meta = self.nco.createVariable('extra_metadata', str, 'time')
        extra_meta.long_name = 'Extra source metadata'

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
        projection = osr.SpatialReference(str(tile_spec.projection))
        if projection.IsGeographic():
            self._create_geo_crs(projection)
            self._create_geo_variables(tile_spec)
        elif projection.IsProjected():
            self._create_proj_crs(projection)
            self._create_proj_variables(tile_spec, projection.GetAttrValue('UNIT'))
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

    def _create_geo_crs(self, projection):
        crs = self.nco.createVariable(_grid_mapping_name(projection), 'i4')
        crs.long_name = projection.GetAttrValue('GEOGCS')  # "Lon/Lat Coords in WGS84"
        crs.grid_mapping_name = _grid_mapping_name(projection)
        crs.longitude_of_prime_meridian = 0.0
        crs.semi_major_axis = projection.GetSemiMajor()
        crs.inverse_flattening = projection.GetInvFlattening()
        crs.spatial_ref = projection.ExportToWkt()  # GDAL variable
        crs.GeoTransform = self._gdal_geotransform()  # GDAL variable
        return crs

    def _gdal_geotransform(self):
        return " ".join(format(c, "g") for c in self._tile_spec.affine.to_gdal())

    def _create_albers_crs(self, projection):
        # http://spatialreference.org/ref/epsg/gda94-australian-albers/html/
        # http://cfconventions.org/Data/cf-conventions/cf-conventions-1.7/build/cf-conventions.html#appendix-grid-mappings
        crs = self.nco.createVariable(_grid_mapping_name(projection), 'i4')
        crs.standard_parallel_1 = projection.GetProjParm('standard_parallel_1')
        crs.standard_parallel_2 = projection.GetProjParm('standard_parallel_2')
        crs.longitude_of_central_meridian = projection.GetProjParm('longitude_of_center')
        crs.latitude_of_projection_origin = projection.GetProjParm('latitude_of_center')
        crs.false_easting = projection.GetProjParm('false_easting')
        crs.false_northing = projection.GetProjParm('false_northing')
        crs.grid_mapping_name = _grid_mapping_name(projection)
        crs.long_name = projection.GetAttrValue('PROJCS')
        crs.spatial_ref = projection.ExportToWkt()  # GDAL variable
        crs.GeoTransform = self._gdal_geotransform()  # GDAL variable
        return crs

    def _create_proj_crs(self, projection):
        if _grid_mapping_name(projection) != 'albers_conic_equal_area':
            raise RuntimeError('%s projection is not supported' % _grid_mapping_name(projection))
        return self._create_albers_crs(projection)

    def _create_proj_variables(self, tile_spec, units):
        self.nco.createDimension('x', len(tile_spec.xs))
        self.nco.createDimension('y', len(tile_spec.ys))

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
        Only allow a single time index at the moment
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

    def set_time_values(self, time_values):
        times = self.nco.variables['time']
        for idx, val in enumerate(time_values):
            times[idx] = _seconds_since_1970(val)

    def append_time_slice(self, varname, data, time, input_filename="Raw Array"):
        out_band = self.nco.variables[varname]
        src_filename = self.nco.variables[varname + "_src_filenames"]
        time_index = self.find_or_create_time_index(time)
        out_band[time_index, :, :] = data
        src_filename[time_index] = input_filename

    def ensure_variable(self, varname, dtype, chunking, ndv=None, units=None):
        if varname in self.nco.variables:
            # TODO: check that var matches
            return self.nco.variables[varname], self.nco.variables[varname + "_src_filenames"]
        return self._create_data_variable(varname, dtype, chunking, ndv, units)

    def append_np_array(self, time, nparray, varname, dtype, ndv, chunking, units):
        if varname in self.nco.variables:
            out_band = self.nco.variables[varname]
            src_filename = self.nco.variables[varname + "_src_filenames"]
        else:
            out_band, src_filename = self._create_data_variable(varname, dtype, chunking, ndv, units)

        time_index = self.find_or_create_time_index(time)

        out_band[time_index, :, :] = nparray
        src_filename[time_index] = "Raw Array"

    def append_slice(self, np_array, storage_type, measurement_descriptor, time_value, input_filename):
        varname = measurement_descriptor.varname
        if varname in self.nco.variables:
            raise VariableAlreadyExists('Error writing to {}: variable {} already exists and will not be '
                                        'overwritten.'.format(self.netcdf_path, varname))

        dtype = measurement_descriptor.dtype
        nodata = getattr(measurement_descriptor, 'nodata', None)
        units = getattr(measurement_descriptor, 'units', None)
        out_band, src_filename = self._create_data_variable(varname, dtype, storage_type.chunking, nodata, units)

        time_index = self.find_or_create_time_index(time_value)

        out_band[time_index, :, :] = np_array
        src_filename[time_index] = input_filename

    def _create_data_variable(self, varname, dtype, chunking, ndv=None, units=None):
        projection = osr.SpatialReference(str(self._tile_spec.projection))
        dimensions = [c[0] for c in chunking]
        chunksizes = [c[1] for c in chunking]
        newvar = self.nco.createVariable(varname, dtype, dimensions,
                                         zlib=True, chunksizes=chunksizes,
                                         fill_value=ndv)
        newvar.grid_mapping = _grid_mapping_name(projection)
        newvar.set_auto_maskandscale(False)

        if units:
            newvar.units = units

        src_filename = self.nco.createVariable(varname + "_src_filenames", str, 'time')
        src_filename.long_name = 'Source filename from data import'
        return newvar, src_filename

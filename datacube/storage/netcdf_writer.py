# coding=utf-8
"""
Create netCDF4 Storage Units and write data to them
"""
from __future__ import absolute_import

import logging
import os.path
from datetime import datetime

import netCDF4
from osgeo import osr

from datacube.model import VariableAlreadyExists

_LOG = logging.getLogger(__name__)

EPOCH = datetime(1970, 1, 1, 0, 0, 0)


class NetCDFWriter(object):
    """
    Base class for creating a NetCDF file based upon GeoTIFF data.

    Sub-classes will create the NetCDF in different structures.
    """

    def __init__(self, netcdf_path, tile_spec):

        if not os.path.isfile(netcdf_path):
            self.nco = netCDF4.Dataset(netcdf_path, 'w')

            self._set_crs(tile_spec)
            self._set_global_attributes(tile_spec)
            self._create_variables(tile_spec)
        else:
            self.nco = netCDF4.Dataset(netcdf_path, 'a')
        self._tile_spec = tile_spec
        self.netcdf_path = netcdf_path

    def close(self):
        self.nco.close()

    def _create_standard_dimensions(self, lats, lons):
        """
        Creates latitude, longitude and time dimension

        Time is unlimited
        Latitude and longitude are given the values in lats,lons
        """
        self.nco.createDimension('longitude', len(lons))
        self.nco.createDimension('latitude', len(lats))
        self.nco.createDimension('time', None)
        timeo = self.nco.createVariable('time', 'double', 'time')
        timeo.units = 'seconds since 1970-01-01 00:00:00'
        timeo.standard_name = 'time'
        timeo.long_name = 'Time, unix time-stamp'
        timeo.calendar = 'standard'
        timeo.axis = "T"

        lon = self.nco.createVariable('longitude', 'double', 'longitude')
        lon.units = 'degrees_east'
        lon.standard_name = 'longitude'
        lon.long_name = 'longitude'
        lon.axis = "X"

        lat = self.nco.createVariable('latitude', 'double', 'latitude')
        lat.units = 'degrees_north'
        lat.standard_name = 'latitude'
        lat.long_name = 'latitude'
        lat.axis = "Y"

        lon[:] = lons
        lat[:] = lats

    def _set_crs(self, tile_spec):
        projection = osr.SpatialReference(tile_spec.projection)
        assert projection.IsGeographic()
        crso = self.nco.createVariable('crs', 'i4')
        crso.long_name = projection.GetAttrValue('GEOGCS')  # "Lon/Lat Coords in WGS84"
        crso.grid_mapping_name = "latitude_longitude"  # TODO support other projections
        crso.longitude_of_prime_meridian = 0.0
        crso.semi_major_axis = projection.GetSemiMajor()
        crso.inverse_flattening = projection.GetInvFlattening()
        return crso

    def _set_global_attributes(self, tile_spec):
        """

        :type tile_spec: datacube.model.TileSpec
        """
        self.nco.spatial_coverage = "1.000000 degrees grid"  # FIXME: Don't hard code
        self.nco.geospatial_lat_min = tile_spec.lat_min
        self.nco.geospatial_lat_max = tile_spec.lat_max
        self.nco.geospatial_lat_units = "degrees_north"
        self.nco.geospatial_lat_resolution = tile_spec.lat_res
        self.nco.geospatial_lon_min = tile_spec.lon_min
        self.nco.geospatial_lon_max = tile_spec.lon_max
        self.nco.geospatial_lon_units = "degrees_east"
        self.nco.geospatial_lon_resolution = tile_spec.lon_res

        creation_date = datetime.utcnow()
        self.nco.history = "NetCDF-CF file created by agdc-v2 at {:%Y%m%d}.".format(creation_date)

        # Attributes from Storage Mapping
        for name, value in tile_spec.global_attrs:
            self.nco.setncattr(name, value)

        # Attributes for NCI Compliance
        self.nco.date_created = datetime.today().isoformat()
        self.nco.Conventions = 'CF-1.6'

    def find_or_create_time_index(self, insertion_time):
        """
        Only allow a single time index at the moment
        :param insertion_time:
        :return:
        """
        times = self.nco.variables['time']

        if len(times) == 0:
            _LOG.debug('Inserting time %s', insertion_time)
            start_datetime_delta = insertion_time - EPOCH
            _LOG.debug('stored time value %s', start_datetime_delta.total_seconds())
            index = len(times)
            # Save as next coordinate in file
            times[index] = start_datetime_delta.total_seconds()
        else:
            index = netCDF4.date2index(insertion_time, times)  # Blow up for a different time

        return index

    def append_np_array(self, time, nparray, varname, dtype, ndv, chunking, units):
        if varname in self.nco.variables:
            out_band = self.nco.variables[varname]
            src_filename = self.nco.variables[varname + "_src_filenames"]
        else:
            chunksizes = [chunking[dim] for dim in ['t', 'y', 'x']]
            out_band, src_filename = self._create_data_variable(varname, dtype, chunksizes, ndv, units)

        time_index = self.find_or_create_time_index(time)

        out_band[time_index, :, :] = nparray
        src_filename[time_index] = "Raw Array"

    def append_gdal_tile(self, gdal_dataset, band_info, storage_type, time_value, input_filename):
        """

        :return:
        """
        varname = band_info.varname
        if varname in self.nco.variables:
            raise VariableAlreadyExists('Error writing to {}: variable {} already exists and will not be '
                                        'overwritten.'.format(self.netcdf_path, varname))

        chunking = storage_type['chunking']
        chunksizes = [chunking[dim] for dim in ['t', 'y', 'x']]
        dtype = band_info.dtype
        nodata = getattr(band_info, 'nodata', None)
        units = getattr(band_info, 'units', None)
        out_band, src_filename = self._create_data_variable(varname, dtype, chunksizes, nodata, units)

        time_index = self.find_or_create_time_index(time_value)

        out_band[time_index, :, :] = gdal_dataset.ReadAsArray()
        src_filename[time_index] = input_filename

    def _create_variables(self, tile_spec):
        self._create_standard_dimensions(tile_spec.lats, tile_spec.lons)

        # Create Variable Length Variable to store extra metadata
        extra_meta = self.nco.createVariable('extra_metadata', str, 'time')
        extra_meta.long_name = 'Extra source metadata'

    def _create_data_variable(self, varname, dtype, chunksizes, ndv, units):
        newvar = self.nco.createVariable(varname, dtype, ('time', 'latitude', 'longitude'),
                                         zlib=True, chunksizes=chunksizes,
                                         fill_value=ndv)
        newvar.grid_mapping = 'crs'
        newvar.set_auto_maskandscale(False)

        if units:
            newvar.units = units

        src_filename = self.nco.createVariable(varname + "_src_filenames", str, 'time')
        src_filename.long_name = 'Source filename from data import'
        return newvar, src_filename

    def _get_netcdf_bands(self, bands):
        netcdfbands = []
        for i, _ in enumerate(bands, 1):
            band = self.nco.variables['band' + str(i)]
            netcdfbands.append(band)
        return netcdfbands


def append_to_netcdf(tile_spec, gdal_dataset, netcdf_path, storage_type, band_info, time_value, input_filename=""):
    """
    Append a raster slice to a new or existing NetCDF file

    :param gdal_dataset: dataset to read the slice from
    :param netcdf_path: pathname to output netcdf file
    :param input_spec:
    :param bandname:
    :param input_filename: used for metadata only
    :return:
    """
    ncfile = NetCDFWriter(netcdf_path, tile_spec)

    ncfile.append_gdal_tile(gdal_dataset, band_info, storage_type,
                            time_value, input_filename)
    ncfile.close()

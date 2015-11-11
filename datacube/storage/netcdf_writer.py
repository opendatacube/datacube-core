from __future__ import absolute_import

import logging
import os.path
from datetime import datetime

import netCDF4
import numpy as np
from netCDF4 import date2index
from osgeo import osr

from .utils import get_dataset_extent

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

        :type tile_spec: TileSpec
        :return:
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
        creation_date = datetime.utcnow().strftime("%Y%m%d")
        self.nco.history = "NetCDF-CF file created %s." % creation_date

        # Attributes for NCI Compliance
        self.nco.title = "Experimental Data files From the Australian Geoscience Data Cube - DO NOT USE"
        self.nco.summary = "These files are experimental, short lived, and the format will change."
        self.nco.source = "This data is a reprojection and retile of Landsat surface reflectance " \
                          "scene data available from /g/data/rs0/scenes/"
        self.nco.product_version = "0.0.0"
        self.nco.date_created = datetime.today().isoformat()
        self.nco.Conventions = 'CF-1.6'
        self.nco.license = "Creative Commons Attribution 4.0 International CC BY 4.0"

    def find_or_create_time_index(self, insertion_time):
        times = self.nco.variables['time']

        try:
            index = date2index(insertion_time, times)
            _LOG.debug('Found date %s at index %s', insertion_time, index)
        except (ValueError, IndexError) as e:
            _LOG.debug('%s: datetime %s not found, appending into times', e, insertion_time)
            # Append to times
            # Convert to seconds since epoch (1970-01-01)
            start_datetime_delta = insertion_time - EPOCH
            _LOG.debug('stored time value %s', start_datetime_delta.total_seconds())

            index = len(times)

            # Save as next coordinate in file
            times[index] = start_datetime_delta.total_seconds()

        return index

    def append_np_array(self, time, nparray, varname, dtype, ndv, chunking):
        if varname in self.nco.variables:
            out_band = self.nco.variables[varname]
            src_filename = self.nco.variables[varname + "_src_filenames"]
        else:
            chunksizes = [chunking[dim] for dim in ['t', 'y', 'x']]
            out_band, src_filename = self._create_data_variable(varname, dtype, chunksizes, ndv)

        time_index = self.find_or_create_time_index(time)

        out_band[time_index, :, :] = nparray
        src_filename[time_index] = "Raw Array"

    def append_gdal_tile(self, gdal_dataset, band_info, storage_type, dataset_metadata,
                         time_value, input_filename):
        """

        :return:
        """
        varname = band_info.varname
        if varname in self.nco.variables:
            out_band = self.nco.variables[varname]
            src_filename = self.nco.variables[varname + "_src_filenames"]
        else:
            chunking = storage_type['chunking']
            chunksizes = [chunking[dim] for dim in ['t', 'y', 'x']]
            dtype = band_info.dtype
            ndv = band_info.fill_value
            out_band, src_filename = self._create_data_variable(varname, dtype, chunksizes, ndv)

        time_index = self.find_or_create_time_index(time_value)

        out_band[time_index, :, :] = gdal_dataset.ReadAsArray()
        src_filename[time_index] = input_filename

    def _create_variables(self, tile_spec):
        self._create_standard_dimensions(tile_spec.lats, tile_spec.lons)

        # Create Variable Length Variable to store extra metadata
        extra_meta = self.nco.createVariable('extra_metadata', str, 'time')
        extra_meta.long_name = 'Extra source metadata'

    def _create_data_variable(self, varname, dtype, chunksizes, ndv):
        newvar = self.nco.createVariable(varname, dtype, ('time', 'latitude', 'longitude'),
                                         zlib=True, chunksizes=chunksizes,
                                         fill_value=ndv)
        newvar.grid_mapping = 'crs'
        newvar.set_auto_maskandscale(False)
        newvar.units = '1'

        src_filename = self.nco.createVariable(varname + "_src_filenames", str, 'time')
        src_filename.long_name = 'Source filename from data import'
        return newvar, src_filename

    def _get_netcdf_bands(self, bands):
        netcdfbands = []
        for i, _ in enumerate(bands, 1):
            band = self.nco.variables['band' + str(i)]
            netcdfbands.append(band)
        return netcdfbands


class TileSpec(object):
    lats = []
    lons = []

    def __init__(self, gdal_ds):
        self._gdal_ds = gdal_ds
        self._nbands = gdal_ds.RasterCount
        self._projection = gdal_ds.GetProjection()
        nlats, nlons = gdal_ds.RasterYSize, gdal_ds.RasterXSize
        geotransform = gdal_ds.GetGeoTransform()
        self._geotransform = geotransform
        self.lons = np.arange(nlons) * geotransform[1] + geotransform[0]
        self.lats = np.arange(nlats) * geotransform[5] + geotransform[3]
        self.extents = get_dataset_extent(gdal_ds)

    @property
    def num_bands(self):
        return self._nbands

    @property
    def projection(self):
        return self._projection

    @property
    def lat_min(self):
        return min(y for x, y in self.extents)

    @property
    def lat_max(self):
        return max(y for x, y in self.extents)

    @property
    def lon_min(self):
        return min(x for x, y in self.extents)

    @property
    def lon_max(self):
        return max(x for x, y in self.extents)

    @property
    def lat_res(self):
        return self._geotransform[5]

    @property
    def lon_res(self):
        return self._geotransform[1]


def append_to_netcdf(gdal_dataset, netcdf_path, storage_type, dataset_metadata, band_info,
                     time_value, input_filename=""):
    """
    Append a raster slice to a new or existing NetCDF file

    :param gdal_dataset: dataset to read the slice from
    :param netcdf_path: pathname to output netcdf file
    :param input_spec:
    :param bandname:
    :param input_filename: used for metadata only
    :return:
    """
    tile_spec = TileSpec(gdal_dataset)

    ncfile = NetCDFWriter(netcdf_path, tile_spec)

    ncfile.append_gdal_tile(gdal_dataset, band_info, storage_type, dataset_metadata,
                            time_value, input_filename)
    ncfile.close()

from datetime import datetime
from netCDF4 import date2index
from osgeo import osr

import os.path
import numpy as np
import netCDF4

from datacube.gdf import GDFNetCDF, dt2secs

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
        timeo = self.nco.createVariable('time', 'f4', 'time')
        timeo.units = 'seconds since 1970-01-01 00:00:00'
        timeo.standard_name = 'time'
        timeo.long_name = 'Time, unix time-stamp'
        timeo.calendar = 'standard'
        timeo.axis = "T"

        lon = self.nco.createVariable('longitude', 'f4', 'longitude')
        lon.units = 'degrees_east'
        lon.standard_name = 'longitude'
        lon.long_name = 'longitude'
        lon.axis = "X"

        lat = self.nco.createVariable('latitude', 'f4', 'latitude')
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
        self.nco.spatial_coverage = "1.000000 degrees grid"
        self.nco.geospatial_lat_min = tile_spec.lat_min
        self.nco.geospatial_lat_max = tile_spec.lat_max
        self.nco.geospatial_lat_units = "degrees_north"
        self.nco.geospatial_lat_resolution = "0.00025"  # FIXME Shouldn't be hard coded
        self.nco.geospatial_lon_min = tile_spec.lon_min
        self.nco.geospatial_lon_max = tile_spec.lon_max
        self.nco.geospatial_lon_units = "degrees_east"
        self.nco.geospatial_lon_resolution = "0.00025"
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

    def find_or_create_time_index(self, date_string):
        # Convert to datetime at midnight
        slice_date = datetime.combine(date_string, datetime.min.time())

        times = self.nco.variables['time']

        try:
            index = date2index(slice_date, times)
        except IndexError:
            # Append to times
            # Convert to seconds since epoch (1970-01-01)
            start_datetime_delta = slice_date - EPOCH
            stored_time_value = start_datetime_delta.total_seconds()

            index = len(times)

            # Save as next coordinate in file
            times[index] = start_datetime_delta.total_seconds()

        return index

    def append_gdal_tile(self, gdal_dataset, input_spec, varname, input_filename):
        """

        :return:
        """
        eodataset = input_spec.dataset
        if varname in self.nco.variables:
            out_band = self.nco.variables[varname]
            src_filename = self.nco.variables[varname + "_src_filenames"]
        else:
            chunking = input_spec.storage_spec['chunking']
            chunksizes = [chunking[dim] for dim in ['t', 'y', 'x']]
            dtype = input_spec.bands[varname].dtype
            ndv = input_spec.bands[varname].fill_value
            out_band, src_filename = self._create_data_variable(varname, dtype, chunksizes, ndv)

        acquisition_date = eodataset['acquisition']['aos']

        time_index = self.find_or_create_time_index(acquisition_date)

        out_band[time_index, :, :] = gdal_dataset.ReadAsArray()
        src_filename[time_index] = input_filename

    def _create_variables(self, tile_spec):
        """

        """
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
        nlats, nlons = gdal_ds.RasterYSize, gdal_ds.RasterXSize
        geotransform = gdal_ds.GetGeoTransform()
        self.lons = np.arange(nlons) * geotransform[1] + geotransform[0]
        self.lats = np.arange(nlats) * geotransform[5] + geotransform[3]

    @property
    def num_bands(self):
        return self._gdal_ds.RasterCount

    @property
    def projection(self):
        return self._gdal_ds.GetProjection()

    @property
    def lat_min(self):
        return min(self.lats)

    @property
    def lat_max(self):
        return max(self.lats)

    @property
    def lon_min(self):
        return min(self.lons)

    @property
    def lon_max(self):
        return max(self.lons)


def append_to_netcdf(gdal_dataset, netcdf_path, input_spec, bandname, input_filename):
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

    ncfile.append_gdal_tile(gdal_dataset, input_spec, bandname, input_filename)
    ncfile.close()


def create_with_gdf():
    storage_config = {}

    gdfnetcdf = GDFNetCDF(storage_config=storage_config)

    t_indices = np.array([dt2secs(record_dict['end_datetime']) for record_dict in data_descriptor])

    gdfnetcdf.create(netcdf_filename=temp_storage_path,
                     index_tuple=storage_indices,
                     dimension_index_dict={'T': t_indices}, netcdf_format=None)

    # Set georeferencing from first tile
    gdfnetcdf.georeference_from_file(data_descriptor[0]['tile_pathname'])

    if len(data_array.shape) == 3:
        gdfnetcdf.write_slice(variable_name, data_array[variable_index], {'T': slice_index})
    elif len(data_array.shape) == 2:
        gdfnetcdf.write_slice(variable_name, data_array, {'T': slice_index})



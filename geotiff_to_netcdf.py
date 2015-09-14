from datetime import datetime
import sys
from abc import ABCMeta, abstractmethod

import numpy as np
import gdal
import netCDF4
import dateutil.parser

chunk_x=400
chunk_y=400
chunk_time=1
epoch = datetime(1970,1,1,0,0,0)


class BaseNetCDF(object):

    __metaclass__ = ABCMeta

    def __init__(self, netcdf_path, mode='r'):
        print "BaseNetCDF.__init__"
        self.nco = self.open_netcdf(netcdf_path, mode)
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
        self.nco.createDimension('time',None)
        timeo = self.nco.createVariable('time','f4',('time'))
        timeo.units = 'seconds since 1970-01-01 00:00:00'
        timeo.standard_name = 'time'
        timeo.long_name = 'Time, unix time-stamp'
        timeo.calendar = 'standard'

        lon = self.nco.createVariable('longitude','f4',('longitude'))
        lon.units = 'degrees_east'
        lon.standard_name = 'longitude'
        lon.long_name = 'longitude'

        lat = self.nco.createVariable('latitude','f4',('latitude'))
        lat.units = 'degrees_north'
        lat.standard_name = 'latitude'
        lat.long_name = 'latitude'

        lon[:] = lons
        lat[:] = lats

    def _set_wgs84_crs(self):
        crso = self.nco.createVariable('crs','i4')
        crso.long_name = "Lon/Lat Coords in WGS84"
        crso.grid_mapping_name = "latitude_longitude"
        crso.longitude_of_prime_meridian = 0.0
        crso.semi_major_axis = 6378137.0
        crso.inverse_flattening = 298.257223563
        return crso

    def _set_global_attributes(nco):
        nco.Conventions = 'CF-1.6'

    def open_netcdf(self, filename, mode):
        return netCDF4.Dataset(filename, mode)

    def _add_time(self, timestamp):
        # Parse ISO 8601 date/time string
        start_datetime = dateutil.parser.parse(timestamp)
        # Convert to seconds since epoch (1970-01-01)
        start_datetime_delta = start_datetime - epoch

        times = self.nco.variables['time']

        # Save as next coordinate in file
        times[len(times)] = start_datetime_delta.total_seconds()

    def _get_nbands_lats_lons_from_gdalds(self, dataset):
        nbands, nlats, nlons = dataset.RasterCount, dataset.RasterYSize, dataset.RasterXSize
        # Calculate pixel coordinates for each x,y based on the GeoTransform
        geotransform = dataset.GetGeoTransform()
        lons = np.arange(nlons)*geotransform[1]+geotransform[0]
        lats = np.arange(nlats)*geotransform[5]+geotransform[3]

        return nbands, lats, lons

    def create_from_geotiff(self, geotiff_path):
        dataset = gdal.Open(geotiff_path)

        nbands, lats, lons = self._get_nbands_lats_lons_from_gdalds(dataset)
        ds_metadata = dataset.GetMetadata()

        self._set_wgs84_crs()
        self._set_global_attributes()
        self._create_variables(lats, lons, nbands)
        self._add_time(ds_metadata['start_datetime'])
        self._write_data_to_netcdf(dataset)

    @abstractmethod
    def _create_variables(self, lats, lons, nbands):
        pass

    @abstractmethod
    def _write_data_to_netcdf(self, dataset):
        pass

    def append_geotiff(self, geotiff):
        dataset = gdal.Open(geotiff)
        ds_metadata = dataset.GetMetadata()
        self._add_time(ds_metadata['start_datetime'])

        self._write_data_to_netcdf(dataset)


class SeparateBandsTimeSlicedNetCDF(BaseNetCDF):
    def _create_variables(self, lats, lons, nbands):
        self._create_standard_dimensions(lats, lons)
        self._create_bands(nbands)

    def _create_bands(self, nbands):
        netcdfbands = []
        for i in range(1,nbands+1):
            band = self.nco.createVariable('band' + str(i), 'i2',  ('time', 'latitude', 'longitude'),
               zlib=True,chunksizes=[chunk_time,chunk_y,chunk_x],fill_value=-9999)
            band.grid_mapping = 'crs'
            band.set_auto_maskandscale(False)
            band.units = "1"
            netcdfbands.append(band)
        #band1.units = 'degC'
        #band1.scale_factor = 0.01
        #band1.add_offset = 0.00
        #band1.long_name = 'minimum monthly temperature'
        #band1.standard_name = 'air_temperature'
        return netcdfbands

    def _get_netcdf_bands(self, nbands):
        netcdfbands = []
        for i in range(1,nbands+1):
            band = self.nco.variables['band' + str(i)]
            netcdfbands.append(band)
        return netcdfbands

    def _write_data_to_netcdf(self, gdal_dataset):
        nbands, lats, lons = self._get_nbands_lats_lons_from_gdalds(gdal_dataset)
        netcdfbands = self._get_netcdf_bands(nbands)

        for idx, out_band in enumerate(netcdfbands, start=1):
            in_band = gdal_dataset.GetRasterBand(idx)

            metadata = in_band.GetMetadata() # eg. filename: 'source.tif', name: 'Photosynthetic Vegetation'
            out_band.long_name = metadata.get('name')
            out_band.source_filename = metadata.get('filename')
            out_band.missing_value = in_band.GetNoDataValue()

            out_band[0,:,:] = in_band.ReadAsArray()
            print('.',)

#     # check that the files exist
#     # check that the files area (lat/longs) matches
#     # check that the bands in the geotiff match the netcdf
#     # check that the other metadata matches
#     # check that the time-slice doesn't already exist
#     # add the time to the netcdf
#     # add each band to the netcdf at the new time
#     pass

class BandAsDimensionNetCDF(BaseNetCDF):
    def _create_variables(self, lats, lons, nbands):
        self._create_standard_dimensions(lats, lons)
        self._create_band_dimension(nbands)

    def _create_band_dimension(self, nbands):
        self.nco.createDimension('band', nbands)
        band = self.nco.createVariable('band','f4',('band'))



def main(args):
    if len(args) < 3:
        print("Usage: geotiff_to_netcdf.py input_geotiff.tiff output_netcdf.nc")
        exit()

    netcdf_path = args[2]
    geotiff_path = args[1]
    if (args[0] == 'geotiff_to_netcdf.py'):
        dcnc = SeparateBandsTimeSlicedNetCDF(netcdf_path, mode='w')
        dcnc.create_from_geotiff(geotiff_path)
        dcnc.close()
    elif (args[0] == 'append_to_netcdf.py'):
        dcnc = SeparateBandsTimeSlicedNetCDF(netcdf_path, mode='a')
        dcnc.append_geotiff(geotiff_path)
        dcnc.close()
    else:
        print("Unknown command line name")


if __name__ == '__main__':
    main(sys.argv)
from __future__ import print_function
from datetime import datetime
import sys
from abc import ABCMeta, abstractmethod
import argparse
import numpy as np
import gdal
import netCDF4
import dateutil.parser

chunk_x=400
chunk_y=400
chunk_time=1
epoch = datetime(1970,1,1,0,0,0)

def print(s='', end='\n', file=sys.stdout):
    file.write(s + end)
    file.flush()

class BaseNetCDF(object):
    """
    Base class for creating a NetCDF file based upon GeoTIFF data.

    Sub-classes will create the NetCDF in different structures.
    """

    __metaclass__ = ABCMeta

    def __init__(self, netcdf_path, mode='r'):
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

        time_index = len(self.nco.variables['time'])

        for idx, out_band in enumerate(netcdfbands, start=1):
            in_band = gdal_dataset.GetRasterBand(idx)

            metadata = in_band.GetMetadata() # eg. filename: 'source.tif', name: 'Photosynthetic Vegetation'
            out_band.long_name = metadata.get('name')
            out_band.source_filename = metadata.get('filename')
            out_band.missing_value = in_band.GetNoDataValue()

            out_band[time_index-1,:,:] = in_band.ReadAsArray()
            print('.', end='')
        print()

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
        self._create_data_variable()

    def _create_band_dimension(self, nbands):
        self.nco.createDimension('band', nbands)
        band = self.nco.createVariable('band', str,('band'))

    def _create_data_variable(self):
        chunk_band = 1
        self.nco.createVariable('observation', 'i2',  ('time', 'band', 'latitude', 'longitude'),
               zlib=True,chunksizes=[chunk_time,chunk_band,chunk_y,chunk_x],fill_value=-9999)

    def _write_data_to_netcdf(self, gdal_dataset):
        nbands, lats, lons = self._get_nbands_lats_lons_from_gdalds(gdal_dataset)

        time_index = len(self.nco.dimensions['time']) - 1
        band_var = self.nco.variables['band']

        observation = self.nco.variables['observation']
        for band_idx in range(nbands):
            in_band = gdal_dataset.GetRasterBand(band_idx + 1)

            metadata = in_band.GetMetadata() # eg. filename: 'source.tif', name: 'Photosynthetic Vegetation'

            band_var[band_idx] = metadata.get('name')

            observation[time_index,band_idx,:,:] = in_band.ReadAsArray()
            print('.', end="")
        print()




def main():
    parser = argparse.ArgumentParser()
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--create", action='store_true', help="Overwrite or create a new NetCDF file")
    group.add_argument("--append", action='store_true', help="Append the geotiff to a new portion of the NetCDF")
    parser.add_argument("-b", "--band_as_dimension", action="store_true", help="Store bands as a dimension instead of as new dataset")
    parser.add_argument("geotiff", help="Input GeoTIFF filename")
    parser.add_argument("netcdf", help="NetCDF file to create or write to")

    args = parser.parse_args()

    if (args.band_as_dimension):
        netcdf_class = BandAsDimensionNetCDF
    else:
        netcdf_class = SeparateBandsTimeSlicedNetCDF


    if (args.create):
        dcnc = netcdf_class(args.netcdf, mode='w')
        dcnc.create_from_geotiff(args.geotiff)
        dcnc.close()
    elif (args.append):
        dcnc = netcdf_class(args.netcdf, mode='a')
        dcnc.append_geotiff(args.geotiff)
        dcnc.close()
    else:
        print("Unknown action")


if __name__ == '__main__':
    main()
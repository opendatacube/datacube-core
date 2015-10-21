from datetime import datetime
from abc import ABCMeta, abstractmethod
import argparse
import os.path

import numpy as np
import gdal
import netCDF4
import yaml

from eodatasets import serialise
from ingester.utils import _get_nbands_lats_lons_from_gdalds

EPOCH = datetime(1970, 1, 1, 0, 0, 0)


class BaseNetCDF(object):
    """
    Base class for creating a NetCDF file based upon GeoTIFF data.

    Sub-classes will create the NetCDF in different structures.
    """

    __metaclass__ = ABCMeta

    def __init__(self, netcdf_path, mode='r', chunk_x=400, chunk_y=400, chunk_time=1):
        self.nco = netCDF4.Dataset(netcdf_path, mode)
        self.netcdf_path = netcdf_path
        self.chunk_x = chunk_x
        self.chunk_y = chunk_y
        self.chunk_time = chunk_time
        self.tile_spec = TileSpec()

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

    def _set_wgs84_crs(self):
        crso = self.nco.createVariable('crs', 'i4')
        crso.long_name = "Lon/Lat Coords in WGS84"
        crso.grid_mapping_name = "latitude_longitude"
        crso.longitude_of_prime_meridian = 0.0
        crso.semi_major_axis = 6378137.0
        crso.inverse_flattening = 298.257223563
        return crso

    def _set_global_attributes(self):
        self.nco.spatial_coverage = "1.000000 degrees grid"
        self.nco.geospatial_lat_min = self.tile_spec.get_lat_min()
        self.nco.geospatial_lat_max = self.tile_spec.get_lat_max()
        self.nco.geospatial_lat_units = "degrees_north"
        self.nco.geospatial_lat_resolution = "0.00025"
        self.nco.geospatial_lon_min = self.tile_spec.get_lon_min()
        self.nco.geospatial_lon_max = self.tile_spec.get_lon_max()
        self.nco.geospatial_lon_units = "degrees_east"
        self.nco.geospatial_lon_resolution = "0.00025"
        creation_date = datetime.utcnow().strftime("%Y%m%d")
        self.nco.history = "NetCDF-CF file created %s." % creation_date

        # Attributes for NCI Compliance
        self.nco.title = "Experimental Data files From the Australian Geoscience Data Cube - DO NOT USE"
        self.nco.summary = "These files are experimental, short lived, and the format will change."
        self.nco.source = "This data is a reprojection and retile of the Landsat L1T surface reflectance " \
                          "scene data available from /g/data/rs0/scenes/"
        self.nco.product_version = "0.0.0"
        self.nco.date_created = datetime.today().isoformat()
        self.nco.Conventions = 'CF-1.6'
        self.nco.license = "Creative Commons Attribution 4.0 International CC BY 4.0"

    def _add_time(self, start_date):
        # Convert to datetime at midnight
        start_datetime = datetime.combine(start_date, datetime.min.time())

        # Convert to seconds since epoch (1970-01-01)
        start_datetime_delta = start_datetime - EPOCH

        times = self.nco.variables['time']

        # Save as next coordinate in file
        times[len(times)] = start_datetime_delta.total_seconds()

    @classmethod
    def create_from_tile_spec(cls, file_path, tile_spec):
        netcdf = cls(file_path, mode='w')
        netcdf.tile_spec = tile_spec

        netcdf._set_wgs84_crs()
        netcdf._set_global_attributes()
        netcdf._create_variables()

        return netcdf

    @classmethod
    def open_with_tile_spec(cls, file_path, tile_spec):
        netcdf = cls(file_path, mode='a')
        netcdf.tile_spec = tile_spec

        return netcdf

    @abstractmethod
    def _create_variables(self):
        """
        Create the structure of the NetCDF file, ie, which variables with which dimensions
        """
        pass

    @abstractmethod
    def _write_data_to_netcdf(self, dataset, eodataset):
        """
        Read in all the data from the geotiff `dataset` and write it as a new time
         slice to the NetCDF file
        :param dataset: open GDAL dataset
        :return:
        """
        pass

    def append_gdal_tile(self, geotiff, eodataset):
        """
        Read a geotiff file and append it to the open NetCDF file

        :param geotiff:string path to a geotiff file
        :return:
        """
        gdal_dataset = gdal.Open(geotiff)
        self._add_time(eodataset.acquisition.aos)

        self._write_data_to_netcdf(gdal_dataset, eodataset)

        del gdal_dataset


class MultiVariableNetCDF(BaseNetCDF):
    """
    Create individual datasets for each `band` of data

    This closely matches the existing GeoTiff tile file structure
    """
    def _create_variables(self):
        self._create_standard_dimensions(self.tile_spec.lats, self.tile_spec.lons)
        self._create_bands(self.tile_spec.bands)

        # Create Variable Length Variable to store extra metadata
        extra_meta = self.nco.createVariable('extra_metadata', str, 'time')
        extra_meta.long_name = 'Extra source metadata'

    def _create_bands(self, bands):
        for i, band in enumerate(bands, 1):
            band = self.nco.createVariable('band' + str(i), 'i2',  ('time', 'latitude', 'longitude'),
                                           zlib=True, chunksizes=[self.chunk_time, self.chunk_y, self.chunk_x],
                                           fill_value=-999)
            band.grid_mapping = 'crs'
            band.set_auto_maskandscale(False)
            band.units = '1'

            srcfilename = self.nco.createVariable('srcfilename_band' + str(i), str, 'time')
            srcfilename.long_name = 'Source filename from data import'

    def _get_netcdf_bands(self, bands):
        netcdfbands = []
        for i, _ in enumerate(bands, 1):
            band = self.nco.variables['band' + str(i)]
            netcdfbands.append(band)
        return netcdfbands

    def _write_data_to_netcdf(self, gdal_dataset, eodataset):
        netcdfbands = self._get_netcdf_bands(self.tile_spec.bands)

        gdal_bands = [gdal_dataset.GetRasterBand(idx + 1) for idx in range(gdal_dataset.RasterCount)]

        metadata_bands = sorted(eodataset.image.bands.values(), key=lambda band: band.number)

        time_index = len(self.nco.variables['time']) - 1

        for in_band, out_band, metadata in zip(gdal_bands, netcdfbands, metadata_bands):
            out_band.long_name = metadata.number
            out_band.missing_value = -999

            out_band[time_index, :, :] = in_band.ReadAsArray()

        extra_meta = self.nco.variables['extra_metadata']
        # FIXME Yucky, we don't really want to be using yaml and private methods here
        extra_meta[time_index] = yaml.dump(eodataset, Dumper=serialise._create_relative_dumper('/'))



class SingleVariableNetCDF(BaseNetCDF):
    """
    Store all data values in a single dataset with an extra dimension for `band`
    """
    def _create_variables(self):
        lats = self.tile_spec.lats
        lons = self.tile_spec.lons

        self._create_standard_dimensions(lats, lons)
        self._create_band_dimension()
        self._create_data_variable()

    def _create_band_dimension(self):
        nbands = len(self.tile_spec.bands)
        self.nco.createDimension('band', nbands)
        band = self.nco.createVariable('band_name', str, 'band')
        band.long_name = "Surface reflectance band name/number"

    def _create_data_variable(self):
        chunk_band = 1
        observations = self.nco.createVariable('observation', 'i2',  ('band', 'time', 'latitude', 'longitude'),
                                               zlib=True,
                                               chunksizes=[chunk_band, self.chunk_time, self.chunk_y, self.chunk_x],
                                               fill_value=-999)
        observations.long_name = "Surface reflectance factor"
        observations.units = '1'
        observations.grid_mapping = 'crs'
        observations.set_auto_maskandscale(False)
        observations.coordinates = 'band_name'

    def _write_data_to_netcdf(self, gdal_dataset, eodataset):
        nbands, lats, lons = _get_nbands_lats_lons_from_gdalds(gdal_dataset)

        time_index = len(self.nco.dimensions['time']) - 1
        band_var = self.nco.variables['band_name']

        ds_bands = sorted(eodataset.image.bands.values(), key=lambda band: band.number)

        observation = self.nco.variables['observation']
        for band_idx in range(nbands):
            in_band = gdal_dataset.GetRasterBand(band_idx + 1)
            metadata = ds_bands[band_idx]

            band_var[band_idx] = metadata.number

            observation[band_idx, time_index, :, :] = in_band.ReadAsArray()


class TileSpec(object):
    bands = []
    lats = []
    lons = []
    lat_resolution = None
    lon_resolution = None

    def __init__(self, bands=None, lats=None, lons=None, lat_resultion=None, lon_resolution=None):
        self.bands = [] if bands is None else bands
        self.lats = [] if lats is None else lats
        self.lons = [] if lons is None else lons
        self.lat_resolution = lat_resultion
        self.lon_resolution = lon_resolution

    def get_lat_min(self):
        return min(self.lats)

    def get_lat_max(self):
        return max(self.lats)

    def get_lon_min(self):
        return min(self.lons)

    def get_lon_max(self):
        return max(self.lons)


class Messenger:
    def __init__(self, **kwargs):
        self.__dict__ = kwargs


def get_input_spec_from_file(filename):
    gdal_dataset = gdal.Open(filename)
    return tile_spec_from_gdal_dataset(gdal_dataset)


def input_spec_from_eodataset(eodataset):
    pass


def tile_spec_from_gdal_dataset(gdal_dataset):
    """
    Return a specification of a GDAL dataset, used for creating a new NetCDF file to hold the same data

    Example specification:
    dict(bands=[{'dtype': 'Int16',
                     'name': 'Photosynthetic Vegetation',
                     'no_data': -999.0},
                    {'dtype': 'Int16',
                     'name': 'Non-Photosynthetic Vegetation',
                     'no_data': -999.0},
                    {'dtype': 'Int16', 'name': 'Bare Soil', 'no_data': -999.0},
                    {'dtype': 'Int16', 'name': 'Unmixing Error', 'no_data': -999.0}],
             lats=array([-33., -33.00025, -33.0005, ..., -33.99925, -33.9995,
                         -33.99975]),
             lons=array([150., 150.00025, 150.0005, ..., 150.99925, 150.9995,
                                                  150.99975]))
    :param gdal_dataset: a gdal dataset
    :return: nested dictionary describing the structure
    """
    nbands, nlats, nlons = gdal_dataset.RasterCount, gdal_dataset.RasterYSize, gdal_dataset.RasterXSize
    geotransform = gdal_dataset.GetGeoTransform()
    lons = np.arange(nlons)*geotransform[1]+geotransform[0]
    lats = np.arange(nlats)*geotransform[5]+geotransform[3]
    bands = []
    for band_idx in range(nbands):
        src_band = gdal_dataset.GetRasterBand(band_idx + 1)
        src_metadata = src_band.GetMetadata()  # eg. filename: 'source.tif', name: 'Photosynthetic Vegetation'

        name = src_metadata.get('name')
        dtype = gdal.GetDataTypeName(src_band.DataType)
        no_data = src_band.GetNoDataValue()

        bands.append(dict(name=name, dtype=dtype, no_data=no_data))

    return TileSpec(bands=bands, lats=lats, lons=lons, lat_resultion=geotransform[5], lon_resolution=geotransform[1])


def append_to_netcdf(gdal_tile, netcdf_path, eodataset, netcdf_class=MultiVariableNetCDF):
    """
    Append a raster slice to a new or existing NetCDF file

    :param gdal_tile: pathname to raster slice, readable by gdal
    :param netcdf_path: pathname to
    :param eodataset:
    :param netcdf_class:
    :return:
    """
    tile_spec = get_input_spec_from_file(gdal_tile)

    if not os.path.isfile(netcdf_path):
        ncfile = netcdf_class.create_from_tile_spec(netcdf_path, tile_spec)
    else:
        ncfile = netcdf_class.open_with_tile_spec(netcdf_path, tile_spec)

    ncfile.append_gdal_tile(gdal_tile, eodataset)
    ncfile.close()


def main():
    parser = argparse.ArgumentParser()
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--create", action='store_true', help="Create a new, empty, NetCDF file")
    group.add_argument("--append", action='store_true', help="Append the geotiff to a new portion of the NetCDF")
    parser.add_argument("-b", "--band_as_dimension", action="store_true",
                        help="Store bands as a dimension instead of as new dataset")
    parser.add_argument("geotiff", help="Input GeoTIFF filename")
    parser.add_argument("netcdf", help="NetCDF file to create or write to")

    args = parser.parse_args()

    if args.band_as_dimension:
        netcdf_class = SingleVariableNetCDF
    else:
        netcdf_class = MultiVariableNetCDF

    if args.create:
        dcnc = netcdf_class(args.netcdf, mode='w')
        tile_spec = get_input_spec_from_file(args.geotiff)
        dcnc.create_from_tile_spec(tile_spec)
        dcnc.close()
    elif args.append:
        dcnc = netcdf_class(args.netcdf, mode='a')
        dcnc.append_gdal_tile(args.geotiff)
        dcnc.close()
    else:
        print 'Unknown action'


if __name__ == '__main__':
    main()

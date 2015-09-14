from os.path import join
import numpy as np
from datetime import datetime
import os
import sys
import gdal
import netCDF4
import re
import dateutil.parser

chunk_x=400
chunk_y=400
chunk_time=1
epoch = datetime(1970,1,1,0,0,0)


def get_nbands_lats_lons_from_gdalds(dataset):
    nbands, nlats, nlons = dataset.RasterCount, dataset.RasterYSize, dataset.RasterXSize
    # Calculate pixel coordinates for each x,y based on the GeoTransform
    geotransform = dataset.GetGeoTransform()
    lons = np.arange(nlons)*geotransform[1]+geotransform[0]
    lats = np.arange(nlats)*geotransform[5]+geotransform[3]

    return nbands, lats, lons


def create_netcdf(filename):
    nco = netCDF4.Dataset(filename,'w')
    return nco


def create_netcdf_variables(nco, lats, lons):
    nco.createDimension('longitude', len(lons))
    nco.createDimension('latitude', len(lats))
    nco.createDimension('time',None)
    timeo = nco.createVariable('time','f4',('time'))
    timeo.units = 'seconds since 1970-01-01 00:00:00'
    timeo.standard_name = 'time'
    timeo.long_name = 'Time, unix time-stamp'
    timeo.calendar = 'standard'

    lon = nco.createVariable('longitude','f4',('longitude'))
    lon.units = 'degrees_east'
    lon.standard_name = 'longitude'
    lon.long_name = 'longitude'

    lat = nco.createVariable('latitude','f4',('latitude'))
    lat.units = 'degrees_north'
    lat.standard_name = 'latitude'
    lat.long_name = 'latitude'

    lon[:] = lons
    lat[:] = lats

    return timeo, lon, lat

def set_wgs84_crs(nco):
    crso = nco.createVariable('crs','i4')
    crso.long_name = "Lon/Lat Coords in WGS84"
    crso.grid_mapping_name = "latitude_longitude"
    crso.longitude_of_prime_meridian = 0.0
    crso.semi_major_axis = 6378137.0
    crso.inverse_flattening = 298.257223563
    return crso

def set_global_attributes(nco):
    nco.Conventions = 'CF-1.6'

def create_bands(nco, nbands):
    netcdfbands = []
    for i in range(1,nbands+1):
        band = nco.createVariable('band' + str(i), 'i2',  ('time', 'latitude', 'longitude'),
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

def get_netcdf_bands(nco, nbands):
    netcdfbands = []
    for i in range(1,nbands+1):
        band = nco.variables['band' + str(i)]
        netcdfbands.append(band)
    return netcdfbands




def add_time_to_netcdf(timestamp, nco):
    # Parse ISO 8601 date/time string
    start_datetime = dateutil.parser.parse(timestamp)
    # Convert to seconds since epoch (1970-01-01)
    start_datetime_delta = start_datetime - epoch

    times = nco.variables['time']

    # Save as next coordinate in file
    times[len(times)] = start_datetime_delta.total_seconds()

def write_data_to_netcdf(gdal_dataset, netcdfbands):

    for idx, out_band in enumerate(netcdfbands, start=1):
        in_band = gdal_dataset.GetRasterBand(idx)

        metadata = in_band.GetMetadata() # eg. filename: 'source.tif', name: 'Photosynthetic Vegetation'
        out_band.long_name = metadata.get('name')
        out_band.source_filename = metadata.get('filename')
        out_band.missing_value = in_band.GetNoDataValue()

        out_band[0,:,:] = in_band.ReadAsArray()
        print('.',)

def create_netcdf_from_geotiff(geotiff, netcdf):
    # Open Geotiff
    dataset = gdal.Open(geotiff)

    nbands, lats, lons = get_nbands_lats_lons_from_gdalds(dataset)
    ds_metadata = dataset.GetMetadata()

    nco = create_netcdf(netcdf)
    set_wgs84_crs(nco)
    set_global_attributes(nco)
    create_netcdf_variables(nco, lats, lons)

    add_time_to_netcdf(ds_metadata['start_datetime'], nco)

    netcdfbands = create_bands(nco, nbands)

    write_data_to_netcdf(dataset, netcdfbands)

    nco.close()

def append_geotiff_to_netcdf(geotiff, netcdf):
    nco = netCDF4.Dataset(netcdf, 'a')

    dataset = gdal.Open(geotiff)
    ds_metadata = dataset.GetMetadata()
    nbands, lats, lons = get_nbands_lats_lons_from_gdalds(dataset)



    add_time_to_netcdf(ds_metadata['start_datetime'], nco)

    netcdfbands = get_netcdf_bands(nco, nbands)

    write_data_to_netcdf(dataset, netcdfbands)

    nco.close()

#     # check that the files exist
#     # check that the files area (lat/longs) matches
#     # check that the bands in the geotiff match the netcdf
#     # check that the other metadata matches
#     # check that the time-slice doesn't already exist
#     # add the time to the netcdf
#     # add each band to the netcdf at the new time
#     pass




def main(args):
    if len(args) < 3:
        print("Usage: geotiff_to_netcdf.py input_geotiff.tiff output_netcdf.nc")
        exit()

    if (args[0] == 'geotiff_to_netcdf.py'):
        create_netcdf_from_geotiff(*args[1:])
    elif (args[0] == 'append_to_netcdf.py'):
        append_geotiff_to_netcdf(*args[1:])
    else:
        print("Unknown command line name")


if __name__ == '__main__':
    main(sys.argv)
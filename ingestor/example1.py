from __future__ import print_function
from datacube.api.model import DatasetType, Satellite, Ls57Arg25Bands, Fc25Bands, Pq25Bands
from datacube.api.query import list_tiles_as_list
from datacube.api.utils import get_dataset_metadata
from datacube.api.utils import get_dataset_data

from geotiff_to_netcdf import SingleVariableNetCDF, MultiVariableNetCDF, get_description_from_dataset
import gdal

from datetime import date
import sys


# python geotiff_to_netcdf.py --append -b /g/data/rs0/tiles/EPSG4326_1deg_0.00025pixel/LS5_TM/150_-034/2006/LS5_TM_FC_150_-034_2006-02-10T23-40-08.399006.tif out.nc

# Define a DatasetType mapping
DS_TYPES_MAP = {'arg25': DatasetType.ARG25,
                'fc25': DatasetType.FC25,
                'pq25': DatasetType.PQ25}

cell_x = 146
cell_y = -34
min_date = date(2014, 1, 1)
max_date = date(2014, 12, 31)
satellites = 'LS5,LS7'
satellites = [Satellite(i) for i in satellites.split(',')]
dataset_types = 'ARG25,FC25,PQ25'
dataset_types = [i.lower() for i in dataset_types.split(',')]
dataset_types = [DS_TYPES_MAP[i] for i in dataset_types]


tiles = list_tiles_as_list(x=[cell_x], y=[cell_y], acq_min=min_date,
                           acq_max=max_date, dataset_types=dataset_types,
                           satellites=satellites)

netcdf_filename = 'multi_band.nc'


arg25paths = [dstile.path for tile in tiles for dstile in tile.datasets.itervalues() if dstile.bands == Ls57Arg25Bands]

# Find an actual tiff file to use the structure of, not a VRT
initial_file = [path for path in arg25paths if path.endswith("tif")][0]


dataset = gdal.Open(initial_file)
description = get_description_from_dataset(dataset)

print("Creating {}".format(netcdf_filename))
multi_band_file = MultiVariableNetCDF(netcdf_filename, mode='w')
print("Creating netcdf structure from {}".format(initial_file))
multi_band_file.create_from_description(description)
print("Appending from {}".format(arg25paths[0]))
multi_band_file.append_geotiff(arg25paths[0])
multi_band_file.close()
# for path in arg25paths:
    # multi_band_file

# /short/u46/gxr547/GA/NBAR/LS8_OLI_TIRS_NBAR_P54_GANBAR01-015_101_078_20141012
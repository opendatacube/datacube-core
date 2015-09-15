from __future__ import print_function
from datacube.api.model import DatasetType, Satellite, Ls57Arg25Bands, Fc25Bands, Pq25Bands
from datacube.api.query import list_tiles_as_list
from datacube.api.utils import get_dataset_metadata
from datacube.api.utils import get_dataset_data

from geotiff_to_netcdf import BandAsDimensionNetCDF, SeparateBandsTimeSlicedNetCDF

from datetime import date
import sys

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

initial_file = [path for path in arg25paths if path.endswith("tif")][0]
arg25paths.remove(initial_file)


print("Creating {}".format(netcdf_filename))
multi_band_file = SeparateBandsTimeSlicedNetCDF(netcdf_filename, mode='w')
print("Creating netcdf structure from {}".format(initial_file))
multi_band_file.create_from_geotiff(initial_file)
print("Appending from {}".format(arg25paths[1]))
multi_band_file.append_geotiff(arg25paths[1])
multi_band_file.close()
# for path in arg25paths:
    # multi_band_file
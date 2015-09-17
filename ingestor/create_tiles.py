
import subprocess

src_files = '/g/data/rs0/scenes/ARG25_V0.0/2015-04/LS7_ETM_NBAR_P54_GANBAR01-002_089_081_20150425/scene01/*.tif'
src_vrt = 'LS7_ETM_NBAR_P54_GANBAR01-002_089_081_20150425.vrt'


subprocess.call(['gdalbuildvrt', src_vrt, src_files], shell=True).wait()




target_src = 'EPSG:4326'
reprojected_vrt = 'LS7_ETM_NBAR_P54_GANBAR01-002_089_081_20150425.{}.vrt'.format(target_src.lower().replace(':', ''))
target_pixel_res = "0.00025"

subprocess.call(['gdalwarp',
                 '-t', target_src,
                 '-of', 'VRT',
                 '-tr', target_pixel_res, target_pixel_res,
                 src_vrt, reprojected_vrt]).wait()

target_dir = 'tiles/'
pixel_size = '4000'
tile_index = 'tile_grid.shp'
output_format = 'NetCDF'
create_options = 'FORMAT=NC4'

subprocess.call(['gdal_retile.py', '-v', '-targetDir', target_dir,
                 '-ps', pixel_size, pixel_size,
                 '-tileIndex', tile_index,
                 '-of', output_format,
                 '-co', create_options,
                 reprojected_vrt]).wait()

cfa_format = 'NETCDF4'
nc4_aggregate_name = 'cfa_aggregate.nc'
input_files = '*.nc'

subprocess.call(['cfa', '-f', cfa_format, '-o', nc4_aggregate_name, input_files], shell=True).wait()


#Pixel resolution (Fraction of a degree)
#-tr 0.00025 0.00025

#Force to nest within the grid definition
#-tap


#Nearest neighbour vs convolution. Depends on whether discrete values
#-r resampling_method




import subprocess
import os
import click
import sys
from glob import glob
from osgeo import gdal,ogr,osr
from math import floor, ceil

# From Metageta and http://gis.stackexchange.com/a/57837/2910
def GetExtent(gt,cols,rows):
    ''' Return list of corner coordinates from a geotransform

        @type gt:   C{tuple/list}
        @param gt: geotransform
        @type cols:   C{int}
        @param cols: number of columns in the dataset
        @type rows:   C{int}
        @param rows: number of rows in the dataset
        @rtype:    C{[float,...,float]}
        @return:   coordinates of each corner
    '''
    ext=[]
    xarr=[0,cols]
    yarr=[0,rows]

    for px in xarr:
        for py in yarr:
            x=gt[0]+(px*gt[1])+(py*gt[2])
            y=gt[3]+(px*gt[4])+(py*gt[5])
            ext.append([x,y])
            print x,y
        yarr.reverse()
    return ext


def reproject_coords(coords,src_srs,tgt_srs):
    ''' Reproject a list of x,y coordinates.

        @type geom:     C{tuple/list}
        @param geom:    List of [[x,y],...[x,y]] coordinates
        @type src_srs:  C{osr.SpatialReference}
        @param src_srs: OSR SpatialReference object
        @type tgt_srs:  C{osr.SpatialReference}
        @param tgt_srs: OSR SpatialReference object
        @rtype:         C{tuple/list}
        @return:        List of transformed [[x,y],...[x,y]] coordinates
    '''
    trans_coords=[]
    transform = osr.CoordinateTransformation( src_srs, tgt_srs)
    for x,y in coords:
        x,y,z = transform.TransformPoint(x,y)
        trans_coords.append([x,y])
    return trans_coords


def get_extents(raster_filename):
    ds = gdal.Open(raster_filename)

    gt = ds.GetGeoTransform()
    cols = ds.RasterXSize
    rows = ds.RasterYSize
    ext = GetExtent(gt,cols,rows)

    src_srs = osr.SpatialReference()
    src_srs.ImportFromWkt(ds.GetProjection())
    #tgt_srs=osr.SpatialReference()
    #tgt_srs.ImportFromEPSG(4326)
    tgt_srs = src_srs.CloneGeogCS()

    geo_ext = reproject_coords(ext,src_srs,tgt_srs)

    return geo_ext

project_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
shapefile_path = os.path.join(project_path, 'example_grid', 'example_grid.shp')

def execute(command_list):
    print("Running command: " + ' '.join(command_list))
    subprocess.check_call(command_list)

def combine_bands_to_vrt(src_files, basename):

    scene_vrt = '{}.vrt'.format(basename)

    execute(['gdalbuildvrt', '-separate', scene_vrt] + src_files)

    return scene_vrt


def create_vrt_with_correct_srs(input_vrt, basename, target_srs="EPSG:4326"):
    reprojected_vrt = '{}.{}.vrt'.format(basename, target_srs.lower().replace(':', ''))
    target_pixel_res = "0.00025"

    execute(['gdalwarp',
             '-t_srs', target_srs,
             '-of', 'VRT',
             '-tr', target_pixel_res, target_pixel_res,  # Pixel resolution x,y (Fraction of a degree)
             '-tap',  # Force to nest within the grid definition
             '-srcnodata', '-999', '-dstnodata', '-999',
             input_vrt, reprojected_vrt])
    return reprojected_vrt


def create_vrt_with_extended_extents(input_vrt, basename):
    extents = get_extents(input_vrt)
    print("Extents: " + str(extents))
    xmin = str(floor(min(p[0] for p in extents)))
    xmax = str(ceil(max(p[0] for p in extents)))
    ymin = str(floor(min(p[1] for p in extents)))
    ymax = str(ceil(max(p[1] for p in extents)))

    extended_vrt = '{}.extended.vrt'.format(basename)

    execute(['gdalbuildvrt', '-te', xmin, ymin, xmax, ymax, extended_vrt, input_vrt])

    return extended_vrt


def create_tile_files(input_vrt):
    # target_dir = 'tiles/'
    target_dir = '.'
    pixel_size = '4000'
    output_format = 'NetCDF'
    create_options = 'FORMAT=NC4'

    execute(['gdal_retile.py', '-v', '-targetDir', target_dir,
             '-ps', pixel_size, pixel_size,
             '-of', output_format,
             '-co', create_options,
             '-co', 'COMPRESS=DEFLATE',
             '-co', 'ZLEVEL=1',
             '-v',
             input_vrt])


def create_aggregated_netcdf():
    cfa_format = 'NETCDF4'
    nc4_aggregate_name = 'cfa_aggregate.nc'
    input_files = '*.nc'

    execute(['cfa',
             '-f', cfa_format,
             '-o', nc4_aggregate_name]+ glob(input_files))

config = {
    'output_dir': '/short/v10/dra547/tmp/today',
    'srs': 'EPSG:4326',
    'grid_lats': [],
    'grid_lons': []
}

injest_task = {
    'src_files': '/g/data/rs0/scenes/ARG25_V0.0/2015-04/LS7_ETM_NBAR_P54_GANBAR01-002_089_081_20150425/scene01/*.tif',
    'basename': 'LS7_ETM_NBAR_P54_GANBAR01-002_089_081_20150425'
}
injest_task = {
    'src_files': '/g/data/rs0/scenes/ARG25_V0.0/1994-02/LS5_TM_NBAR_P54_GANBAR01-002_099_081_19940209/scene01/*.tif',
    'basename': 'LS5_TM_NBAR_P54_GANBAR01-002_099_081_19940209'
}



@click.command()
@click.option('--output-dir', default='.')
@click.argument('basename')
@click.argument('src-files',
                type=click.Path(exists=True, readable=True),
                nargs=-1)
def main(basename, src_files, output_dir):
    os.chdir(output_dir)

    print('Source files: ' + str(src_files))

    combined_vrt = combine_bands_to_vrt(src_files=list(src_files),
                                        basename=basename)
    reprojected_vrt = create_vrt_with_correct_srs(combined_vrt, basename=basename)
    extended_vrt = create_vrt_with_extended_extents(reprojected_vrt, basename=basename)
    create_tile_files(extended_vrt)
# create_aggregated_netcdf()

if __name__ == '__main__':
    main()

#Nearest neighbour vs convolution. Depends on whether discrete values
#-r resampling_method



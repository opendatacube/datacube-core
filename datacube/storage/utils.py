from __future__ import absolute_import, division, print_function
import functools
import logging
import os
import subprocess

import click
from osgeo import osr, gdal
import numpy as np

_LOG = logging.getLogger(__name__)


def get_extent(geotransform, cols, rows):
    """
    Return list of corner coordinates from a geotransform

    From Metageta and http://gis.stackexchange.com/a/57837/2910

    @type geotransform:   C{tuple/list}
    @param geotransform: geotransform
    @type cols:   C{int}
    @param cols: number of columns in the dataset
    @type rows:   C{int}
    @param rows: number of rows in the dataset
    @rtype:    C{[float,...,float]}
    @return:   coordinates of each corner
    """
    ext = []
    xarr = [0, cols]
    yarr = [0, rows]

    for px in xarr:
        for py in yarr:
            x = geotransform[0] + (px * geotransform[1]) + (py * geotransform[2])
            y = geotransform[3] + (px * geotransform[4]) + (py * geotransform[5])
            ext.append([x, y])
        yarr.reverse()
    return ext


def get_dataset_extent(gdal_dataset):
    return get_extent(gdal_dataset.GetGeoTransform(), gdal_dataset.RasterXSize, gdal_dataset.RasterYSize)


def reproject_coords(coords, src_srs, tgt_srs):
    """
    Reproject a list of x,y coordinates.

    @type coords:     C{tuple/list}
    @param coords:    List of [[x,y],...[x,y]] coordinates
    @type src_srs:  C{osr.SpatialReference}
    @param src_srs: OSR SpatialReference object
    @type tgt_srs:  C{osr.SpatialReference}
    @param tgt_srs: OSR SpatialReference object
    @rtype:         C{tuple/list}
    @return:        List of transformed [[x,y],...[x,y]] coordinates
    """
    transformed_coords = []
    transform = osr.CoordinateTransformation(src_srs, tgt_srs)
    for x, y in coords:
        x, y, z = transform.TransformPoint(x, y)
        transformed_coords.append([x, y])
    return transformed_coords


def get_file_extents(raster_filename, epsg_ref=4326):
    """
    Calculate file extents with a specific geoprojection

    :param raster_filename:
    :return: [(x,y),...]
    """
    ds = gdal.Open(raster_filename)

    extents = get_dataset_extent(ds)

    src_srs = osr.SpatialReference()
    src_srs.ImportFromWkt(ds.GetProjection())

    tgt_srs = osr.SpatialReference()
    tgt_srs.ImportFromEPSG(epsg_ref)

    return reproject_coords(extents, src_srs, tgt_srs)


def preserve_cwd(function):
    """
    Decorator function to preserve the current working directory

    Thanks to http://stackoverflow.com/a/170174/119603

    :param function:
    :return: wrapped function
    """
    @functools.wraps(function)
    def decorator(*args, **kwargs):
        cwd = os.getcwd()
        try:
            return function(*args, **kwargs)
        finally:
            os.chdir(cwd)
    return decorator


def execute(command_list):
    """
    Execute an external command and log any stdout/stderr messages to logging
    :param command_list:
    """
    _LOG.debug("Running command: " + ' '.join(command_list))
    proc = subprocess.Popen(command_list, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                            env=dict(os.environ, GDAL_CACHEMAX="250"))
    # see http://stackoverflow.com/a/13924218/119603
    for line in proc.stdout:
        _LOG.debug(line)
    # subprocess.check_call(command_list, env=dict(os.environ, GDAL_CACHEMAX="250"))


def _get_nbands_lats_lons_from_gdalds(gdal_dataset):
    nbands, nlats, nlons = gdal_dataset.RasterCount, gdal_dataset.RasterYSize, gdal_dataset.RasterXSize

    # Calculate pixel coordinates for each x,y based on the GeoTransform
    geotransform = gdal_dataset.GetGeoTransform()
    lons = np.arange(nlons) * geotransform[1] + geotransform[0]
    lats = np.arange(nlats) * geotransform[5] + geotransform[3]

    return nbands, lats, lons


def create_empty_dataset(src_filename, out_filename):
    """
    Create a new GDAL dataset based on an existing one, but with no data.

    Will contain the same projection, extents, etc, but have a very small filesize.

    These files can be used for automated testing without having to lug enormous files around.

    :param src_filename: Source Filename
    :param out_filename: Output Filename
    """
    inds = gdal.Open(src_filename)
    driver = inds.GetDriver()
    band = inds.GetRasterBand(1)

    out = driver.Create(out_filename,
                        inds.RasterXSize,
                        inds.RasterYSize,
                        inds.RasterCount,
                        band.DataType)
    out.SetGeoTransform(inds.GetGeoTransform())
    out.SetProjection(inds.GetProjection())
    out.FlushCache()


@click.command(help="Create an empty dataset.\n\n"
                    "Copies extents, cols, rows, projection and datatype from the source dataset, \n"
                    "but doesn't copy any data.\n"
                    "This should produce a tiny file suitable for testing ingestion and tiling.")
@click.argument('src_filename', type=click.Path(exists=True, readable=True))
@click.argument('out_filename', type=click.Path())
def create_empty_dataset_cli(src_filename, out_filename):
    create_empty_dataset(src_filename, out_filename)

import functools
import logging
import os
import subprocess
import warnings

import click
from osgeo import osr, gdal
import numpy as np
import scipy.ndimage

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


@click.command(help="Print an image to the terminal ")
@click.option('--size', '-s')
@click.argument('filename', type=click.Path(exists=True, readable=True))
def print_image(filename, size=50):
    """
    Output an ASCII representation of a GDAL image to the terminal

    :param filename:
    :return:
    """
    chars = np.asarray(list(' .,:;irsXA253hMHGS#9B&@'))

    character_size_ratio = 7/4.0  # width to height
    output_height = float(size)

    ds = gdal.Open(filename)
    band = ds.GetRasterBand(1)
    ar = band.ReadAsArray()

    input_width, input_height = ar.shape

    scale_factor = output_height / input_height

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")  # We don't care that the output shape may have changed
        image = scipy.ndimage.interpolation.zoom(ar, (scale_factor, scale_factor*character_size_ratio), order=0)

    image *= (22.0/image.max())
    image = image.clip(0)
    print("\n".join(("".join(r) for r in chars[image.astype(int)])))


def _get_nbands_lats_lons_from_gdalds(gdal_dataset):
    nbands, nlats, nlons = gdal_dataset.RasterCount, gdal_dataset.RasterYSize, gdal_dataset.RasterXSize

    # Calculate pixel coordinates for each x,y based on the GeoTransform
    geotransform = gdal_dataset.GetGeoTransform()
    lons = np.arange(nlons)*geotransform[1]+geotransform[0]
    lats = np.arange(nlats)*geotransform[5]+geotransform[3]

    return nbands, lats, lons


class Messenger:
    def __init__(self, **kwargs):
        self.__dict__ = kwargs
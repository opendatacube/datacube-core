import logging
from osgeo import osr, gdal
import subprocess
from osgeo import gdal

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


def execute(command_list):
    _LOG.debug("Running command: " + ' '.join(command_list))
    subprocess.check_call(command_list)


# coding=utf-8
"""
Utility functions used in storage access
"""
from __future__ import absolute_import, division, print_function

import logging
from pathlib import Path

from osgeo import osr, gdal
import numpy as np
from datacube.model import TileSpec

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


def _get_nbands_lats_lons_from_gdalds(gdal_dataset):
    nbands, nlats, nlons = gdal_dataset.RasterCount, gdal_dataset.RasterYSize, gdal_dataset.RasterXSize

    # Calculate pixel coordinates for each x,y based on the GeoTransform
    geotransform = gdal_dataset.GetGeoTransform()
    lons = np.arange(nlons) * geotransform[1] + geotransform[0]
    lats = np.arange(nlats) * geotransform[5] + geotransform[3]

    return nbands, lats, lons


def namedtuples2dicts(namedtuples):
    """
    Convert a dict of namedtuples to a dict of dicts

    :param namedtuples: dict of namedtuples
    :return: dict of dicts
    """
    return {k: dict(vars(v)) for k, v in namedtuples.items()}


def tilespec_from_gdaldataset(gdal_ds, global_attrs=None):
    """
    Create a TileSpec pulling all required attributes from an open gdal dataset

    :param gdal_ds:
    :param global_attrs:
    :rtype: TileSpec
    """
    projection = gdal_ds.GetProjection()
    nlats, nlons = gdal_ds.RasterYSize, gdal_ds.RasterXSize
    geotransform = gdal_ds.GetGeoTransform()
    extents = get_dataset_extent(gdal_ds)
    return TileSpec(projection, geotransform, nlats, nlons, extents, global_attrs)


def tilespec_from_riodataset(rio, global_attrs=None):
    projection = rio.crs_wkt
    width, height = rio.width, rio.height
    return TileSpec(str(projection), rio.affine, height, width, rio.bounds, global_attrs)


def ensure_path_exists(filename):
    file_dir = Path(filename).parent
    if not file_dir.exists:
        file_dir.parent.mkdir(parents=True)

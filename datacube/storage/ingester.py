# coding=utf-8
"""
This module extracts tile regions from a supplied dataset, and creates and writes to storage units
"""
from __future__ import absolute_import, division, print_function

import logging

import affine
import dateutil.parser
import numpy
import rasterio
import rasterio.coords
from rasterio.warp import transform_bounds

from datacube import compat
from datacube.model import TileSpec
from datacube.storage.utils import ensure_path_exists
from .netcdf_writer import append_to_netcdf

_LOG = logging.getLogger(__name__)


class SimpleObject(object):
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


def expand_bounds(bounds, tile_size):
    """
    Expand the bounds to a multiple of the tile size

    Requires positive x and negative y

    :param bounds: iterable with left, bottom, right, top
    :param tile_size: dict with 'x' and 'y'
    :return:
    """
    left, bottom, right, top = bounds
    x = tile_size['x']
    y = tile_size['y']

    if x <= 0:
        raise ValueError("Tiles must have a positive x size")
    if y >= 0:
        raise ValueError("Tiles must have a negative y size")

    left = (left // x) * x
    right = ((right // x) * x) + x

    top = (top // y) * y
    bottom = ((bottom // y) * y) + y

    return rasterio.coords.BoundingBox(left, bottom, right, top)


def create_tiles(src_ds, tile_size, tile_res, tile_crs, tile_dtype=None):
    """
    Generate to yield a set of tiled data of a dataset

    :param src_ds:
    :param tile_size: dict of form {'x': , 'y': }
    :param tile_res: dict of form {'x': , 'y': }
    :param tile_crs:
    :param tile_dtype:
    :return:
    """
    tile_dtype = tile_dtype or src_ds.dtypes[0]

    bounds = transform_bounds(src_ds.crs, tile_crs, *src_ds.bounds)
    outer_bounds = expand_bounds(bounds, tile_size)

    width = int(tile_size['x'] / tile_res['x'])
    height = int(tile_size['y'] / tile_res['y'])

    for y in numpy.arange(outer_bounds.top, outer_bounds.bottom, tile_size['y']):
        for x in numpy.arange(outer_bounds.left, outer_bounds.right, tile_size['x']):
            tile_transform = affine.Affine(tile_res['x'], 0.0, x,
                                           0.0, tile_res['y'], y)
            dst_region = numpy.full((height, width), -999, dtype=tile_dtype)

            rasterio.warp.reproject(rasterio.band(src_ds, 1), dst_region, dst_transform=tile_transform,
                                    dst_crs=tile_crs)
            yield dst_region, tile_transform


class InputSpec(object):
    def __init__(self, storage_spec, bands, dataset):
        self.storage_spec = storage_spec
        self.bands = bands
        self.dataset = dataset


def make_input_specs(ingest_config, storage_configs, eodataset):
    for storage in ingest_config['storage']:
        if storage['name'] not in storage_configs:
            _LOG.warning('Storage name "%s" is not found Storage Configurations. Skipping', storage['name'])
            continue
        storage_spec = storage_configs[storage['name']]

        yield InputSpec(
            storage_spec=storage_spec,
            bands={
                name: SimpleObject(**vals) for name, vals in storage['bands'].items()
                },
            dataset=eodataset
        )


def generate_filename(filename_format, eodataset, tile_spec):
    merged = eodataset.copy()

    # Until we can use parsed dataset fields:
    if isinstance(merged['creation_dt'], compat.string_types):
        merged['creation_dt'] = dateutil.parser.parse(merged['creation_dt'])
    if isinstance(merged['extent']['center_dt'], compat.string_types):
        merged['extent']['center_dt'] = dateutil.parser.parse(merged['extent']['center_dt'])

    merged.update(tile_spec.__dict__)
    return filename_format.format(**merged)


class ImportFromNDArraysNotSupported(Exception):
    """Can only currently import from single layer rasters"""


def storage_unit_tiler(measurement_descriptor, input_filename, storage_spec, time_value, dataset_metadata):
    """

    :param measurement_descriptor:
    :param input_filename:
    :param storage_spec:
    :param time_value:
    :param dataset_metadata:
    :return:
    """

    input_filename = str(input_filename)

    src_ds = rasterio.open(input_filename)
    if src_ds.count > 1:
        raise ImportFromNDArraysNotSupported

    tile_crs = str(storage_spec['projection']['spatial_ref']).strip()
    _LOG.debug("Ingesting: %s %s", measurement_descriptor, input_filename)
    for im, transform in create_tiles(src_ds,
                                      storage_spec['tile_size'],
                                      storage_spec['resolution'],
                                      tile_crs=tile_crs):
        nlats, nlons = im.shape

        tile_spec = TileSpec(nlats, nlons, tile_crs, transform, data=im)

        output_filename = generate_filename(storage_spec['filename_format'], dataset_metadata, tile_spec)
        ensure_path_exists(output_filename)

        _LOG.debug("Adding extracted tile to %s", output_filename)

        append_to_netcdf(tile_spec, output_filename, storage_spec, measurement_descriptor, time_value,
                         input_filename)
        _LOG.debug("Wrote %s to %s", measurement_descriptor.__dict__, output_filename)
        yield output_filename

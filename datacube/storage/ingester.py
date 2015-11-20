from __future__ import absolute_import, division, print_function

import logging
import os
from datacube import compat

import yaml
from osgeo import gdal, gdalconst, osr
from pathlib import Path
from datacube.storage.utils import tilespec_from_gdaldataset

from .netcdf_writer import append_to_netcdf
from datacube.model import TileSpec

_LOG = logging.getLogger(__name__)


class SimpleObject(object):
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


def _get_point(gt, px, py):
    x = gt[0] + (px * gt[1]) + (py * gt[2])
    y = gt[3] + (px * gt[4]) + (py * gt[5])
    return x, y


def _get_extent(gt, cols, rows):
    return (
        _get_point(gt, 0, 0),
        _get_point(gt, 0, rows),
        _get_point(gt, cols, 0),
        _get_point(gt, cols, rows),
    )


def extract_region(width, height, geotransform, projection, type_, bands=1):
    driver = gdal.GetDriverByName('MEM')
    img = driver.Create('temp', width, height, 1, eType=type_)
    img.SetGeoTransform(geotransform)
    img.SetProjection(projection)
    return img


def create_tiles(src_ds, dst_size, dst_res, dst_srs=None, src_srs=None, src_tr=None, dst_type=None):
    """
    Takes a gdal dataset, and yield a set of tiles
    """
    src_tr = src_tr or src_ds.GetGeoTransform()
    src_srs = src_srs or osr.SpatialReference(src_ds.GetProjectionRef())
    dst_srs = dst_srs or src_srs
    dst_type = dst_type or src_ds.GetRasterBand(1).DataType

    src_ext = _get_extent(src_ds.GetGeoTransform(), src_ds.RasterXSize, src_ds.RasterYSize)
    transform = osr.CoordinateTransformation(src_srs, dst_srs)
    dst_ext = [transform.TransformPoint(x, y)[:2] for x, y in src_ext]

    min_x = int(min(x // dst_size['x'] for x, _ in dst_ext))
    min_y = int(min(y // dst_size['y'] for _, y in dst_ext))

    max_x = int(max(x // dst_size['x'] for x, _ in dst_ext))
    max_y = int(max(y // dst_size['y'] for _, y in dst_ext))

    for y in compat.range(min_y, max_y + 1):
        for x in compat.range(min_x, max_x + 1):
            yield _calc_region(dst_res, dst_size, dst_srs, dst_type, src_ds, x, y)


def _calc_region(dst_res, dst_size, dst_srs, dst_type, src_ds, x, y):
    # TODO: check that it intersects with dst_ext
    transform = [x * dst_size['x'], dst_res['x'], 0.0, y * dst_size['y'], 0.0, dst_res['y']]
    _LOG.debug(transform)
    _LOG.debug([x * dst_size['x'], y * dst_size['y'], (x + 1) * dst_size['x'], (y + 1) * dst_size['y']])
    width = int(dst_size['x'] / dst_res['x'])
    height = int(dst_size['y'] / dst_res['y'])
    region = extract_region(width, height, transform, dst_srs.ExportToWkt(), dst_type)
    r = gdal.ReprojectImage(src_ds, region)
    assert r == 0
    return region


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
    merged.update(tile_spec.__dict__)
    return filename_format.format(**merged)


def ensure_path_exists(filename):
    file_dir = Path(filename).parent
    if not file_dir.exists:
        file_dir.parent.mkdir(parents=True)


class StorageSegmentMetadata(object):
    def __init__(self, coordinates, measurements, spatial_extent, time_extent):
        self.coordinates = coordinates
        self.measurements = measurements
        self.spatial_extent = spatial_extent
        self.time_extent = time_extent


def crazy_band_tiler(band_info, input_filename, storage_spec, time_value, dataset_metadata):
    src_ds = gdal.Open(input_filename, gdalconst.GA_ReadOnly)
    _LOG.debug("Ingesting: %s %s", band_info, input_filename)
    for im in create_tiles(src_ds,
                           storage_spec['tile_size'],
                           storage_spec['resolution'],
                           dst_srs=osr.SpatialReference(str(storage_spec['projection']['spatial_ref']))):
        tile_spec = tilespec_from_gdaldataset(im)

        out_filename = generate_filename(storage_spec['filename_format'], dataset_metadata, tile_spec)
        ensure_path_exists(out_filename)

        _LOG.debug((os.getcwd(), out_filename))

        append_to_netcdf(tile_spec, im, out_filename, storage_spec, band_info, time_value, input_filename)
        _LOG.debug(im)
        yield out_filename


def load_yaml(filename):
    # yaml_path = os.path.join(os.path.dirname(__file__), filename)
    return yaml.load(open(filename).read())


def load_eodataset(dataset_path):
    # dataset_path = "/short/u46/gxr547/GA/NBAR/LS5_TM_NBAR_P54_GANBAR01-002_100_081_20100228/"
    with open(os.path.join(dataset_path, "ga-metadata.yaml"), "r") as stream:
        dataset_config = yaml.load(stream)

    for name in dataset_config['image']['bands']:
        dataset_config['image']['bands'][name]['path'] = dataset_path + dataset_config['image']['bands'][name]['path']

    return dataset_config

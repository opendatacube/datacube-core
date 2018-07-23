import string
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pytest
import rasterio
import yaml
from hypothesis.strategies import (
    composite, floats, sampled_from, lists, tuples, datetimes, uuids, text)

from datacube.utils.geometry import CRS, point
from integration_tests.utils import prepare_test_ingestion_configuration

DATE_FORMAT = '%Y-%m-%d %H:%M:%S'

# Bounds retrieved from http://www.epsg-registry.org
# Code, lat_bounds, lon_bounds
epsg_codes = [('EPSG:32755', (-80, 0), (144, 150))]
crses = sampled_from(epsg_codes)

reasonable_dates = datetimes(min_value=datetime(1980, 1, 1), max_value=datetime(2030, 1, 1))


@composite
def positions(draw, lat_bounds=(-90, 90), lon_bounds=(-180, 180)):
    return draw(tuples(
        floats(*lon_bounds, allow_nan=False, allow_infinity=False),
        floats(*lat_bounds, allow_nan=False, allow_infinity=False)
    ))


@composite
def bounds(draw, bounds):
    return draw(
        lists(
            floats(*bounds), min_size=2, max_size=2
        ).map(sorted).filter(lambda x: x[0] < x[1]))


@composite
def bboxes(draw, lat_bounds=(-90, 90), lon_bounds=(-180, 180)):
    """
    https://tools.ietf.org/html/rfc7946#section-5
    """
    # Use 3 dim positions even if we only need 2
    min_lat, max_lat = draw(bounds(lat_bounds))
    min_lon, max_lon = draw(bounds(lon_bounds))

    return min_lon, min_lat, max_lon, max_lat


@composite
def image_paths(draw):
    pass


@composite
def extents(draw, lat_bounds, lon_bounds):
    center_dt = draw(reasonable_dates)

    time_range = timedelta(seconds=12)
    min_lon, min_lat, max_lon, max_lat = draw(bboxes(lat_bounds, lon_bounds))
    return {
        'center_dt': center_dt.strftime(DATE_FORMAT),
        'from_dt': (center_dt - time_range).strftime(DATE_FORMAT),
        'to_dt': (center_dt + time_range).strftime(DATE_FORMAT),
        'coord': {
            'll': {'lat': min_lat, 'lon': min_lon},
            'lr': {'lat': min_lat, 'lon': max_lon},
            'ul': {'lat': max_lat, 'lon': min_lon},
            'ur': {'lat': max_lat, 'lon': max_lon},
        }
    }


def extent_point_projector(crs):
    crs = CRS(crs)

    def reproject_point(pos):
        pos = point(pos['lon'], pos['lat'], CRS('EPSG:4326'))
        coords = pos.to_crs(crs).coords[0]
        return {'x': coords[0], 'y': coords[1]}

    return reproject_point


def extent_to_grid_spatial(extent, crs):
    """Convert an extent in WGS84 to a grid spatial in the supplied CRS"""
    reprojector = extent_point_projector(crs)
    return {
        'projection': {
            'geo_ref_points': {
                corner: reprojector(pos)
                for corner, pos in extent['coord'].items()
            },
            'spatial_reference': crs
        }
    }


@composite
def acquisition_details(draw):
    return {
        'aos': draw(reasonable_dates),
        'groundstation': {
            'code': draw(text(alphabet=string.ascii_letters, min_size=3, max_size=5))
        },
        'los': draw(reasonable_dates)
    }


def image():
    return {
        'bands': {
            '1': {
                'path': 'LS5_TM_NBAR_P54_GANBAR01-002_090_084_19900302_B10.tif'
            }
        }
    }


@composite
def scene_datasets(draw):
    crs, lat_bounds, lon_bounds = draw(crses)
    extent = draw(extents(lat_bounds, lon_bounds))
    return {
        'id': str(draw(uuids())),
        'acquisition': draw(acquisition_details()),
        'creation_dt': draw(reasonable_dates),
        'extent': extent,
        'grid_spatial': extent_to_grid_spatial(extent, crs),
        'image': image(),
        'format': {
            'name': 'GeoTIFF'},
        'instrument': {
            'name': 'TM'},
        'lineage': {
            'source_datasets': {}},
        'platform': {
            'code': 'LANDSAT_5'},
        'processing_level': 'P54',
        'product_type': 'nbar'
    }


def create_test_scene_datasets(tmpdir, num=2):
    paths_to_datasets = []
    for i in range(num):
        ls5_dataset = scene_datasets().example()
        dataset_file = _create_test_dataset_directory(ls5_dataset, tmpdir)
        paths_to_datasets.append(dataset_file)
    return paths_to_datasets


def _create_test_dataset_directory(dataset_dict, tmpdir):
    # Make directory name
    dir_name = dataset_dict['platform']['code'] + dataset_dict['id'][:5]

    # Create directory
    new_dir = tmpdir.mkdir(dir_name)

    _make_geotiffs(new_dir, dataset_dict)
    dataset_file = new_dir.join('agdc-metadata.yaml')
    dataset_file.write(yaml.safe_dump(dataset_dict))
    return dataset_file


def _make_geotiffs(output_dir, dataset_dict, shape=(100, 100)):
    """
    Generate custom geotiff files, one per band.

    Create appropriate GeoTIFF files

    Create ``num_bands`` TIFF files inside ``tiffs_dir``.

    Return a dictionary mapping band_number to filename, eg::

        {
            0: '/tmp/tiffs/band01_time01.tif',
            1: '/tmp/tiffs/band02_time01.tif'
        }
    """
    tiffs = {}
    width, height = shape
    pixel_width = (dataset_dict['grid_spatial']['projection']['geo_ref_points']['ul']['x'] -
                   dataset_dict['grid_spatial']['projection']['geo_ref_points']['ur']['x']) / width
    pixel_height = (dataset_dict['grid_spatial']['projection']['geo_ref_points']['ul']['y'] -
                    dataset_dict['grid_spatial']['projection']['geo_ref_points']['ll']['y']) / height
    metadata = {'count': 1,
                'crs': dataset_dict['grid_spatial']['projection']['spatial_reference'],
                'driver': 'GTiff',
                'dtype': 'int16',
                'width': width,
                'height': height,
                'nodata': -999.0,
                'transform': [pixel_width,
                              0.0,
                              dataset_dict['grid_spatial']['projection']['geo_ref_points']['ul']['x'],
                              0.0,
                              pixel_height,
                              dataset_dict['grid_spatial']['projection']['geo_ref_points']['ul']['y']]}

    for band_num, band_info in dataset_dict['image']['bands'].items():
        path = Path(output_dir) / band_info['path']
        with rasterio.open(path, 'w', **metadata) as dst:
            # Write data in "corners" (rounded down by 100, for a size of 100x100)
            data = np.zeros((height, width), dtype=np.int16)
            data[:] = np.arange(height * width
                                ).reshape((height, width)) + 10 * int(band_num)
            dst.write(data, 1)
        tiffs[band_num] = path
    return tiffs


PROJECT_ROOT = Path(__file__).parents[1]

INGESTER_CONFIGS = PROJECT_ROOT / 'docs/config_samples/' / 'ingester'


@pytest.mark.parametrize('datacube_env_name', ('datacube',), indirect=True)
@pytest.mark.usefixtures('default_metadata_type',
                         'indexed_ls5_scene_products')
def test_double_ingestion(clirunner, index, tmpdir, ingest_configs):
    # Make a test ingestor configuration
    config = INGESTER_CONFIGS / ingest_configs['ls5_nbar_albers']
    config_path, config = prepare_test_ingestion_configuration(tmpdir, None,
                                                               config, mode='fast_ingest')

    index_dataset = lambda path: clirunner(['dataset', 'add', str(path)])

    # Create and Index some example scene datasets
    dataset_paths = create_test_scene_datasets(tmpdir)
    for path in dataset_paths:
        index_dataset(path)

    # Ingest them
    clirunner([
        'ingest',
        '--config-file',
        str(config_path)
    ])

    # Create and Index some more scene datasets
    dataset_paths = create_test_scene_datasets(tmpdir)
    for path in dataset_paths:
        index_dataset(path)

    # Make sure that we can ingest the new scenes
    clirunner([
        'ingest',
        '--config-file',
        str(config_path)
    ])

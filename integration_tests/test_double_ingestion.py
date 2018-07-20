import string
from datetime import datetime, timedelta
from pathlib import Path

import pytest
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
                'path': 'scene01/LS5_TM_NBAR_P54_GANBAR01-002_090_084_19900302_B10.tif'
            }
        }
    }


@composite
def scene_datasets(draw):
    crs, lat_bounds, lon_bounds = draw(crses)
    extent = draw(extents(lat_bounds, lon_bounds))
    return {
        'id': draw(uuids()),
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

def create_tmp_scene_datasets(tmpdir, num=2):
    for i in range(num):



PROJECT_ROOT = Path(__file__).parents[1]
CONFIG_SAMPLES = PROJECT_ROOT / 'docs/config_samples/'
LS5_SAMPLES = CONFIG_SAMPLES / 'ga_landsat_5/'
LS5_MATCH_RULES = CONFIG_SAMPLES / 'match_rules' / 'ls5_scenes.yaml'
LS5_NBAR_STORAGE_TYPE = LS5_SAMPLES / 'ls5_geographic.yaml'
LS5_NBAR_ALBERS_STORAGE_TYPE = LS5_SAMPLES / 'ls5_albers.yaml'

INGESTER_CONFIGS = CONFIG_SAMPLES / 'ingester'


@pytest.mark.parametrize('datacube_env_name', ('datacube',), indirect=True)
@pytest.mark.usefixtures('default_metadata_type',
                         'indexed_ls5_scene_products')
def test_full_ingestion(clirunner, index, tmpdir, ingest_configs):
    config = INGESTER_CONFIGS / ingest_configs['ls5_nbar_albers']
    config_path, config = prepare_test_ingestion_configuration(tmpdir, None, config, mode='fast_ingest')
    valid_uuids = []
    for uuid, example_ls5_dataset_path in example_ls5_dataset_paths.items():
        valid_uuids.append(uuid)
        clirunner([
            'dataset',
            'add',
            str(example_ls5_dataset_path)
        ])

    ensure_datasets_are_indexed(index, valid_uuids)

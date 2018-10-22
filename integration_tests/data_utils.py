"""
The start of some general purpose utilities for generating test data.

At the moment it's not very generic, but can continue to be extended in that fashion.

Hypothesis is used for most of the data generation, which will hopefully improve the rigour
of our tests when we can roll it into more tests.
"""
import string
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import rasterio
import yaml
from hypothesis.strategies import sampled_from, datetimes, composite, floats, lists, text, uuids

from datacube.utils.geometry import CRS, point

DATE_FORMAT = '%Y-%m-%d %H:%M:%S'

# Bounds retrieved from http://www.epsg-registry.org
# Code, lat_bounds, lon_bounds
EPSG_CODES = [('EPSG:32755', (-80, 0), (144, 150))]
crses = sampled_from(EPSG_CODES)
REASONABLE_DATES = datetimes(min_value=datetime(1980, 1, 1), max_value=datetime(2030, 1, 1))
FILENAMES = text(alphabet=string.ascii_letters + string.digits + '-_', min_size=3)


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
def extents(draw, lat_bounds, lon_bounds):
    center_dt = draw(REASONABLE_DATES)

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


def _extent_point_projector(crs):
    crs = CRS(crs)

    def reproject_point(pos):
        pos = point(pos['lon'], pos['lat'], CRS('EPSG:4326'))
        coords = pos.to_crs(crs).coords[0]
        return {'x': coords[0], 'y': coords[1]}

    return reproject_point


def extent_to_grid_spatial(extent, crs):
    """Convert an extent in WGS84 to a grid spatial in the supplied CRS"""
    reprojector = _extent_point_projector(crs)
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
        'aos': draw(REASONABLE_DATES),
        'groundstation': {
            'code': draw(text(alphabet=string.ascii_letters, min_size=3, max_size=5))
        },
        'los': draw(REASONABLE_DATES)
    }


@composite
def scene_datasets(draw):
    """
    Generate random test Landsat 5 Scene Datasets
    """
    crs, lat_bounds, lon_bounds = draw(crses)
    extent = draw(extents(lat_bounds, lon_bounds))
    return {
        'id': str(draw(uuids())),
        'acquisition': draw(acquisition_details()),
        'creation_dt': draw(REASONABLE_DATES),
        'extent': extent,
        'grid_spatial': extent_to_grid_spatial(extent, crs),
        'image': {
            'bands': {
                '1': {
                    'path': draw(FILENAMES) + '.tif'
                }
            }
        },
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


def write_test_scene_to_disk(dataset_dict, tmpdir):
    tmpdir = Path(str(tmpdir))
    # Make directory name
    dir_name = dataset_dict['platform']['code'] + dataset_dict['id']

    # Create directory
    new_dir = tmpdir / dir_name
    new_dir.mkdir()

    _make_geotiffs(new_dir, dataset_dict)
    dataset_file = new_dir / 'agdc-metadata.yaml'
    with dataset_file.open('w') as out:
        yaml.safe_dump(dataset_dict, out)
    return dataset_file


def _make_geotiffs(output_dir, dataset_dict, shape=(100, 100)):
    """
    Generate test GeoTIFF files into ``output_dir``, one per band, from a dataset dictionary

    :return: a dictionary mapping band_number to filename, eg::

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

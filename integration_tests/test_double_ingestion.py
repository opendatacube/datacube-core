import string
from datetime import datetime, timedelta

from hypothesis.strategies import (
    composite, floats, sampled_from, lists, tuples, datetimes, uuids, text)

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
def extents(draw):
    crs, lat_bounds, lon_bounds = draw(crses)
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


def grid_spatial():
    return {
        'projection': {
            'geo_ref_points'
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
    return {
        'id': draw(uuids()),
        'acquisition': draw(acquisition_details()),
        'creation_dt': draw(reasonable_dates),
        'extent': draw(extents()),
        'image': draw(image()),
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

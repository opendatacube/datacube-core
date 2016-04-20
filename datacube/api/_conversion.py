# coding=utf-8
"""
Functions for converting between properties.
"""
from __future__ import absolute_import, division, print_function

import logging
import datetime

import rasterio.warp
from dateutil import tz

from datacube import compat
from datacube.model import Range
from datacube.index import index_connect
from datacube.index.postgres._fields import RangeDocField  # pylint: disable=protected-access
from datacube.utils import datetime_to_seconds_since_1970

_LOG = logging.getLogger(__name__)

FLOAT_TOLERANCE = 0.0000001 # TODO: For DB query, use some sort of 'contains' query, rather than range overlap.


def datetime_to_timestamp(dt):
    if not isinstance(dt, datetime.datetime) and not isinstance(dt, datetime.date):
        dt = to_datetime(dt)
    return datetime_to_seconds_since_1970(dt)


def to_datetime(t):
    if isinstance(t, compat.integer_types + (float,)):
        t = datetime.datetime.fromtimestamp(t, tz=tz.tzutc())
    if isinstance(t, tuple):
        t = datetime.datetime(*t, tzinfo=tz.tzutc())
    elif isinstance(t, compat.string_types):
        try:
            t = datetime.datetime.strptime(t, "%Y-%m-%dT%H:%M:%S.%fZ")
        except ValueError:
            pass
        try:
            from pandas import to_datetime as pandas_to_datetime
            return pandas_to_datetime(t, utc=True, infer_datetime_format=True).to_pydatetime()
        except ImportError:
            pass

    if isinstance(t, datetime.datetime):
        if t.tzinfo is None:
            t = t.replace(tzinfo=tz.tzutc())
        return t
    raise ValueError('Could not parse the time for {}'.format(t))


def dimension_ranges_to_selector(dimension_ranges, reverse_sort):
    ranges = dict((dim_name, dim['range']) for dim_name, dim in dimension_ranges.items() if 'range' in dim)
    # if 'time' in ranges:
    #     ranges['time'] = tuple(datetime_to_timestamp(r) for r in ranges['time'])
    return dict((c, slice(*sorted(r, reverse=(c in reverse_sort and reverse_sort[c])))
                 if isinstance(r, tuple) else r) for c, r in ranges.items())


def dimension_ranges_to_iselector(dim_ranges):
    array_ranges = dict((dim_name, dim['array_range']) for dim_name, dim in dim_ranges.items() if 'array_range' in dim)
    #TODO: Check if 'end' of array range should be inclusive or exclusive. Prefer exclusive to match with slice
    return dict((c, slice(*r)) for c, r in array_ranges.items())


def convert_descriptor_query_to_search_query(descriptor=None, index=None):
    descriptor = descriptor or {}
    index = index or index_connect()

    known_fields = index.datasets.get_fields().keys()

    search_query = {key: descriptor[key] for key in descriptor.keys() if key in known_fields}
    unknown_fields = [key for key in descriptor.keys()
                      if key not in known_fields
                      and key not in ['variables', 'variable', 'dimensions', 'dimension', 'storage_type']]
    if unknown_fields:
        _LOG.warning("Some of the fields in the query are unknown and will be ignored: %s",
                     ', '.join(unknown_fields))

    if 'storage_type' in descriptor:
        storage_type_name = descriptor.get('storage_type', '')
        storage_type = index.storage.types.get_by_name(storage_type_name)
        if storage_type:
            search_query['type'] = storage_type.id

    descriptor_dimensions = descriptor.get('dimensions', {})
    descriptor_dimensions.update(descriptor.get('dimension', {}))
    search_query.update(convert_descriptor_dims_to_search_dims(descriptor_dimensions))
    return search_query


def convert_descriptor_dims_to_search_dims(descriptor_query_dimensions):
    search_query = {}
    input_coords = {'left': None, 'bottom': None, 'right': None, 'top': None}
    input_crs = None  # Get spatial CRS from either spatial dimension
    for dim, data in descriptor_query_dimensions.items():
        if 'range' in data:
            # Convert any known dimension CRS
            if dim in ['latitude', 'lat', 'y']:
                input_crs = input_crs or data.get('crs', 'EPSG:4326')
                if isinstance(data['range'], compat.string_types + compat.integer_types + (float,)):
                    input_coords['top'] = float(data['range'])
                    input_coords['bottom'] = float(data['range'])
                else:
                    input_coords['top'] = data['range'][0]
                    input_coords['bottom'] = data['range'][-1]
            elif dim in ['longitude', 'lon', 'long', 'x']:
                input_crs = input_crs or data.get('crs', 'EPSG:4326')
                if isinstance(data['range'], compat.string_types + compat.integer_types + (float,)):
                    input_coords['left'] = float(data['range'])
                    input_coords['right'] = float(data['range'])
                else:
                    input_coords['left'] = data['range'][0]
                    input_coords['right'] = data['range'][-1]
            elif dim in ['time', 't']:
                # TODO: Handle time formatting strings & other CRS's
                # Assume dateime object or seconds since UNIX epoch 1970-01-01 for now...
                search_query['time'] = time_to_search_dims(data['range'])
            else:
                # Assume the search function will sort it out, add it to the query
                if 'range' in data and hasattr(data['range'], '__iter__'):
                    search_query[dim] = Range(*data['range'])
                else:
                    search_query[dim] = data

    if any(v is not None for v in input_coords.values()):
        search_coords = geospatial_warp_bounds(input_coords, input_crs, tolerance=FLOAT_TOLERANCE)
        search_query['lat'] = Range(search_coords['bottom'], search_coords['top'])
        search_query['lon'] = Range(search_coords['left'], search_coords['right'])

    return search_query


def time_to_search_dims(time_range):
    # TODO: Handle time formatting strings & other CRS's
    # Assume dateime object or seconds since UNIX epoch 1970-01-01 for now...
    if hasattr(time_range, '__iter__') and len(time_range) == 2:
        return Range(to_datetime(time_range[0]), to_datetime(time_range[1]))
    else:
        single_query_time = to_datetime(time_range)
        end_time = single_query_time + datetime.timedelta(milliseconds=1)
        return Range(single_query_time, end_time)


def convert_descriptor_dims_to_selector_dims(dimension_ranges_descriptor, storage_crs='EPSG:4326'):
    dimension_ranges = {}
    input_coord = {'left': None, 'bottom': None, 'right': None, 'top': None}
    input_crs = None
    mapped_vars = {}
    single_value_vars = []
    for dim, data in dimension_ranges_descriptor.items():
        dimension_ranges[dim] = dict((k, v) for k, v in data.items() if k != 'range')
        if 'range' in data:
            # Convert any known dimension CRS
            if dim in ['latitude', 'lat', 'y']:
                input_crs = input_crs or data.get('crs', 'EPSG:4326')
                if isinstance(data['range'], compat.string_types + compat.integer_types + (float,)):
                    input_coord['top'] = float(data['range'])
                    input_coord['bottom'] = float(data['range'])
                    single_value_vars.append('lat')
                else:
                    input_coord['top'] = data['range'][0]
                    input_coord['bottom'] = data['range'][-1]
                mapped_vars['lat'] = dim
            elif dim in ['longitude', 'lon', 'long', 'x']:
                input_crs = input_crs or data.get('crs', 'EPSG:4326')
                if isinstance(data['range'], compat.string_types + compat.integer_types+ (float,)):
                    input_coord['left'] = float(data['range'])
                    input_coord['right'] = float(data['range'])
                    single_value_vars.append('lon')
                else:
                    input_coord['left'] = data['range'][0]
                    input_coord['right'] = data['range'][-1]
                mapped_vars['lon'] = dim
            elif dim in ['time']:
                # TODO: Handle time formatting strings & other CRS's
                # Assume dateime object or seconds since UNIX epoch 1970-01-01 for now...
                dimension_ranges[dim]['range'] = time_to_selector_dim(data['range'])
            else:
                # Add to ranges unchanged
                dimension_ranges[dim]['range'] = data['range']

    if any(v is not None for v in input_coord.values()):
        storage_coords = geospatial_warp_bounds(input_coord, input_crs, storage_crs)
        def make_range(a, b, single_var=False):
            if single_var:
                return a
            return (a, b)
        dimension_ranges[mapped_vars['lat']]['range'] = make_range(storage_coords['top'],
                                                                   storage_coords['bottom'],
                                                                   'lat' in single_value_vars)
        dimension_ranges[mapped_vars['lat']]['crs'] = storage_crs
        dimension_ranges[mapped_vars['lon']]['range'] = make_range(storage_coords['left'],
                                                                   storage_coords['right'],
                                                                   'lon' in single_value_vars)
        dimension_ranges[mapped_vars['lon']]['crs'] = storage_crs

    return dimension_ranges


def time_to_selector_dim(time_range):
    # TODO: Handle time formatting strings & other CRS's
    if hasattr(time_range, '__iter__') and len(time_range) == 2:
        return (datetime_to_timestamp(time_range[0]), datetime_to_timestamp(time_range[1]))
    else:
        return datetime_to_timestamp(time_range)


def convert_request_args_to_descriptor_query(request=None, index=None):
    request_remaining = request.copy() or {}
    index = index or index_connect()

    descriptor_request = dict()

    if 'variables' in request:
        descriptor_request['variables'] = request_remaining.pop('variables')

    if 'storage_type' in request_remaining:
        descriptor_request['storage_type'] = request_remaining.pop('storage_type')

    known_fields = [field_name for field_name, field in index.datasets.get_fields().items()
                    if not isinstance(field, RangeDocField)]  # Assume range fields will be also dimensions...

    for field in request:
        if field in known_fields:
            descriptor_request[field] = request_remaining.pop(field)

    dimensions = request_remaining.pop('dimensions', {})

    for k, v in request_remaining.items():
        if isinstance(v, slice):
            dimensions[k] = {'array_range': (v.start, v.stop)}
        else:
            dimensions[k] = {'range': v} # assume range or single value
    descriptor_request['dimensions'] = dimensions
    return descriptor_request


def geospatial_warp_bounds(input_coord, input_crs='EPSG:4326', output_crs='EPSG:4326', tolerance=0.):
    '''
    Converts coordinates, adding tolerance if they are the same point for index searching
    :param input_crs: EPSG or other GDAL string
    :param output_crs: EPSG or other GDAL string (Search is 'EPSG:4326')
    :param input_coord: {'left': float, 'bottom': float, 'right': float, 'top': float}
    :return: {'lat':Range,'lon':Range}
    '''
    if any(v is None for v in input_coord.values()):
        raise ValueError('Unable to convert spatial bounds: Missing coordinate in input_coord {}'.format(input_coord))
    left, bottom, right, top = rasterio.warp.transform_bounds(input_crs, output_crs, **input_coord)
    output_coords = {'left': left, 'bottom': bottom, 'right': right, 'top': top}

    if bottom == top:
        output_coords['top'] = top + tolerance
        output_coords['bottom'] = bottom - tolerance
    if left == right:
        output_coords['left'] = left - tolerance
        output_coords['right'] = right + tolerance
    return output_coords

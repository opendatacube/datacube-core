#    Copyright 2015 Geoscience Australia
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.

"""
Storage Query and Access API module
"""

from __future__ import absolute_import, division, print_function

import logging
import datetime

from dateutil import tz

from ..compat import string_types, integer_types
from ..model import GeoBox, GeoPolygon, Range, CRS
from ..utils import datetime_to_seconds_since_1970

_LOG = logging.getLogger(__name__)

FLOAT_TOLERANCE = 0.0000001 # TODO: For DB query, use some sort of 'contains' query, rather than range overlap.


class Query(object):
    def __init__(self, dataset_type=None, variables=None, **kwargs):
        self.type = dataset_type
        self.variables = variables
        self.search = kwargs
        self.geopolygon = None
        self.group_by = {}
        self.slices = {}

    @classmethod
    def from_descriptor_request(cls, descriptor_request):
        if descriptor_request is None:
            descriptor_request = {}
        query = cls()

        type_keys = ('type', 'storage_type', 'dataset_type')
        dataset_type = [value for key, value in descriptor_request.items() if key in type_keys]
        if dataset_type:
            query.type = dataset_type[0]
        defined_keys = ('dimensions', 'variables') + type_keys
        query.search = {key: value for key, value in descriptor_request.items() if key not in defined_keys}

        if 'variables' in descriptor_request:
            query.variables = descriptor_request['variables']

        if 'dimensions' in descriptor_request:
            dims = descriptor_request['dimensions']

            spatial_dims = {dim: v for dim, v in dims.items()
                            if dim in ['latitude', 'lat', 'y', 'longitude', 'lon', 'x']}
            range_params = {dim: v['range'] for dim, v in spatial_dims.items() if 'range' in v}
            crs = {c for dim, v in dims.items() for k, c in v.items() if k in ['crs', 'coordinate_reference_system']}
            if len(crs) == 1:
                range_params['crs'] = crs.pop()
            elif len(crs) > 1:
                raise ValueError('Spatial dimensions must be in the same coordinate reference system: {}'.format(crs))
            query.geopolygon = _range_to_geopolygon(**range_params)

            other_dims = {dim: v for dim, v in dims.items()
                          if dim not in ['latitude', 'lat', 'y', 'longitude', 'lon', 'x']}
            query.search.update(_range_to_search(**other_dims))
            query.slices = {dim: slice(*v['array_range']) for dim, v in dims.items() if 'array_range' in v}
            query.group_by = {dim: v['group_by'] for dim, v in dims.items() if 'group_by' in v}
        return query

    @property
    def search_terms(self):
        kwargs = {}
        kwargs.update(self.search)
        if self.geopolygon:
            geo_bb = self.geopolygon.to_crs(CRS('EPSG:4326')).boundingbox
            if geo_bb.bottom != geo_bb.top:
                kwargs['lat'] = Range(geo_bb.bottom, geo_bb.top)
            else:
                kwargs['lat'] = Range(geo_bb.bottom - FLOAT_TOLERANCE, geo_bb.top + FLOAT_TOLERANCE)
            if geo_bb.left != geo_bb.right:
                kwargs['lon'] = Range(geo_bb.left, geo_bb.right)
            else:
                kwargs['lon'] = Range(geo_bb.left - FLOAT_TOLERANCE, geo_bb.right + FLOAT_TOLERANCE)
        if self.type:
            kwargs['type'] = self.type
        return kwargs

    @property
    def group_by_func(self):
        if not self.group_by:
            return _get_group_by_func()
        if len(self.group_by) == 1:
            return _get_group_by_func(self.group_by.values().next())
        else:
            raise NotImplementedError('Grouping across multiple dimensions ({dims}) not yet supported'.format(
                dims=', '.join(self.group_by.keys())
            ))


def _range_to_geopolygon(**kwargs):
    input_crs = None
    input_coords = {'left': None, 'bottom': None, 'right': None, 'top': None}
    for key, value in kwargs.items():
        key = key.lower()
        if key in ['latitude', 'lat', 'y']:
            input_coords['top'], input_coords['bottom'] = _value_to_range(value)
        if key in ['longitude', 'lon', 'long', 'x']:
            input_coords['left'], input_coords['right'] = _value_to_range(value)
        if key in ['crs', 'coordinate_reference_system']:
            input_crs = CRS(value)
    input_crs = input_crs or CRS('EPSG:4326')
    if any(v is not None for v in input_coords.values()):
        points = [
            (input_coords['left'], input_coords['top']),
            (input_coords['right'], input_coords['top']),
            (input_coords['right'], input_coords['bottom']),
            (input_coords['left'], input_coords['bottom']),
        ]
        return GeoPolygon(points, input_crs)
    return None


def _value_to_range(value):
    if isinstance(value, string_types + integer_types + (float,)):
        value = float(value)
        return value, value
    else:
        return float(value[0]), float(value[-1])


def _range_to_search(**kwargs):
    search = {}
    for key, value in kwargs.items():
        if key.lower() in ('time', 't'):
            search['time'] = _time_to_search_dims(value)
        elif key not in ['latitude', 'lat', 'y'] + ['longitude', 'lon', 'x']:
            if hasattr(value, '__iter__') and len(value) == 2:
                search[key] = Range(*value)
            else:
                search[key] = value
    return search


def _datetime_to_timestamp(dt):
    if not isinstance(dt, datetime.datetime) and not isinstance(dt, datetime.date):
        dt = _to_datetime(dt)
    return datetime_to_seconds_since_1970(dt)


def _to_datetime(t):
    if isinstance(t, integer_types + (float,)):
        t = datetime.datetime.fromtimestamp(t, tz=tz.tzutc())
    if isinstance(t, tuple):
        t = datetime.datetime(*t, tzinfo=tz.tzutc())
    elif isinstance(t, string_types):
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


def _time_to_search_dims(time_range):
    # TODO: Handle time formatting strings & other CRS's
    # Assume dateime object or seconds since UNIX epoch 1970-01-01 for now...
    time_range = time_range['range']
    if hasattr(time_range, '__iter__') and len(time_range) == 2:
        return Range(_to_datetime(time_range[0]), _to_datetime(time_range[1]))
    else:
        single_query_time = _to_datetime(time_range)
        end_time = single_query_time + datetime.timedelta(milliseconds=1)
        return Range(single_query_time, end_time)


def _get_group_by_func(group_by=None):
    if hasattr(group_by, '__call__'):
        return group_by
    if group_by is None or group_by == 'time':
        def just_time(ds):
            try:
                return ds.time
            except KeyError:
                # TODO: Remove this when issue #119 is resolved
                return ds.metadata_doc['acquisition']['aos']
        return just_time
    elif group_by == 'day':
        return lambda ds: ds.time.date()
    elif group_by == 'solar_day':
        raise NotImplementedError('The group by `solar_day` feature is coming soon.')
    else:
        raise LookupError('No group_by function found called {}'.format(group_by))

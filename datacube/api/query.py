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
import collections

from dateutil import tz
import numpy as np

from ..compat import string_types, integer_types
from ..model import GeoPolygon, Range, CRS
from ..utils import datetime_to_seconds_since_1970

_LOG = logging.getLogger(__name__)


GroupBy = collections.namedtuple('GroupBy', ['dimension', 'group_by_func', 'units'])

FLOAT_TOLERANCE = 0.0000001 # TODO: For DB query, use some sort of 'contains' query, rather than range overlap.
SPATIAL_KEYS = ('latitude', 'lat', 'y', 'longitude', 'lon', 'long', 'x')
CRS_KEYS = ('crs', 'coordinate_reference_system')
OTHER_KEYS = ('measurements', 'variables', 'group_by', 'output_crs', 'resolution', 'set_nan')


class Query(object):
    def __init__(self, dataset_type=None, measurements=None, **kwargs):
        self.type = dataset_type
        self.measurements = measurements
        self.search = kwargs
        self.geopolygon = None
        self.group_by_name = None
        self.slices = {}
        self.set_nan = False
        self.resolution = None
        self.output_crs = None

    @classmethod
    def from_kwargs(cls, index=None, **kwargs):
        """Parses a kwarg dict for query parameters

        :param index: An optional `index` object, if checking of field names is desired.
        :param kwargs:
         * `dataset_type` Name of the dataset type
         * `measurements` List of measurements
         * `crs` Spatial coordinate reference system to interpret the spatial dimensions
        :return: :class:`Query`
        """
        query = cls()

        if 'product' in kwargs:
            query.type = kwargs['product']

        query.measurements = _get_as_list(kwargs, 'measurements', None)
        if query.measurements is None:
            query.measurements = _get_as_list(kwargs, 'variables', None)

        spatial_dims = {dim: v for dim, v in kwargs.items() if dim in SPATIAL_KEYS}

        crs = {v for k, v in kwargs.items() if k in CRS_KEYS}
        if len(crs) == 1:
            spatial_dims['crs'] = crs.pop()
        elif len(crs) > 1:
            raise ValueError('Spatial dimensions must be in the same coordinate reference system: {}'.format(crs))

        query.geopolygon = _range_to_geopolygon(**spatial_dims)

        if 'group_by' in kwargs:
            query.group_by_name = kwargs['group_by']

        if 'output_crs' in kwargs:
            query.output_crs = CRS(kwargs['output_crs'])

        if 'resolution' in kwargs:
            query.resolution = kwargs['resolution']

        if 'set_nan' in kwargs:
            query.set_nan = kwargs['set_nan']

        remaining_keys = set(kwargs.keys()) - set(('product',) + SPATIAL_KEYS + CRS_KEYS + OTHER_KEYS)
        if index:
            known_fields = set(index.datasets.get_field_names())
            unknown_keys = remaining_keys - known_fields
            if unknown_keys:
                raise LookupError('Unknown arguments: ', unknown_keys)

        for key in remaining_keys:
            query.search.update(_values_to_search(**{key:kwargs[key]}))

        return query

    @classmethod
    def from_descriptor_request(cls, descriptor_request):
        if descriptor_request is None:
            descriptor_request = {}
        if not isinstance(descriptor_request, collections.Mapping):
            raise ValueError('Could not understand descriptor {}'.format(descriptor_request))
        query = cls()

        if 'storage_type' in descriptor_request:
            query.type = descriptor_request['storage_type']
        defined_keys = ('dimensions', 'variables', 'product', 'storage_type')
        query.search = {key: value for key, value in descriptor_request.items() if key not in defined_keys}

        if 'product' in descriptor_request:
            query.search['product_type'] = descriptor_request['product']

        if 'variables' in descriptor_request:
            query.measurements = descriptor_request['variables']

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
            groups = [v['group_by'] for dim, v in dims.items() if 'group_by' in v]
            if groups:
                query.group_by_name = groups[0]
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
            kwargs['product'] = self.type
        return kwargs

    @property
    def group_by(self):
        if not self.group_by_name or self.group_by_name == 'time':
            return GroupBy(dimension='time',
                           group_by_func=lambda ds: ds.center_time,
                           units='seconds since 1970-01-01 00:00:00')
        elif self.group_by_name == 'solar_day':
            return GroupBy(dimension='time',
                           group_by_func=solar_day,
                           units='seconds since 1970-01-01 00:00:00')
        else:
            raise LookupError('No group by function for', self.group_by_name)

    def __repr__(self):
        return self.__str__()

    def __str__(self):
        return """Datacube Query:
        type = {type}
        measurements = {measurements}
        search = {search}
        geopolygon = {geopolygon}
        group_by = {group_by}
        slices = {slices}
        set_nan = {set_nan}
        """.format(type=self.type,
                   measurements=self.measurements,
                   search=self.search,
                   geopolygon=self.geopolygon,
                   group_by=self.group_by,
                   slices=self.slices,
                   set_nan=self.set_nan, )


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
            time_range = value['range']
            search['time'] = _time_to_search_dims(time_range)
        elif key not in ['latitude', 'lat', 'y'] + ['longitude', 'lon', 'x']:
            if isinstance(value, collections.Sequence) and len(value) == 2:
                search[key] = Range(*value)
            else:
                search[key] = value
    return search


def _values_to_search(**kwargs):
    search = {}
    for key, value in kwargs.items():
        if key.lower() in ('time', 't'):
            search['time'] = _time_to_search_dims(value)
        elif key not in ['latitude', 'lat', 'y'] + ['longitude', 'lon', 'x']:
            if isinstance(value, collections.Sequence) and len(value) == 2:
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
    if hasattr(time_range, '__iter__') and len(time_range) == 2:
        return Range(_to_datetime(time_range[0]), _to_datetime(time_range[1]))
    else:
        single_query_time = _to_datetime(time_range)
        end_time = single_query_time + datetime.timedelta(milliseconds=1)
        return Range(single_query_time, end_time)


def _get_as_list(mapping, key, default=None):
    if key not in mapping:
        return default
    value = mapping[key]
    if isinstance(value, string_types) or not isinstance(value, collections.Sequence):
        value = [value]
    return list(value)


def _convert_to_solar_time(utc, longitude):
    seconds_per_degree = 240
    offset_seconds = int(longitude * seconds_per_degree)
    offset = datetime.timedelta(seconds=offset_seconds)
    return utc + offset


def solar_day(dataset):
    utc = dataset.center_time
    bb = dataset.extent.to_crs(CRS('WGS84')).boundingbox
    assert bb.left < bb.right  # TODO: Handle dateline?
    longitude = (bb.left + bb.right) * 0.5
    solar_time = _convert_to_solar_time(utc, longitude)
    return np.datetime64(solar_time.date(), 'D')

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
from pandas import to_datetime as pandas_to_datetime
import numpy as np

from ..compat import string_types, integer_types
from ..model import Range, CRS
from ..utils import geometry, datetime_to_seconds_since_1970

_LOG = logging.getLogger(__name__)


GroupBy = collections.namedtuple('GroupBy', ['dimension', 'group_by_func', 'units', 'sort_key'])

FLOAT_TOLERANCE = 0.0000001  # TODO: For DB query, use some sort of 'contains' query, rather than range overlap.
SPATIAL_KEYS = ('latitude', 'lat', 'y', 'longitude', 'lon', 'long', 'x')
CRS_KEYS = ('crs', 'coordinate_reference_system')
OTHER_KEYS = ('measurements', 'group_by', 'output_crs', 'resolution', 'set_nan', 'product', 'geopolygon', 'like',
              'source_filter')


class Query(object):
    def __init__(self, index=None, product=None, geopolygon=None, like=None, **kwargs):
        """Parses a kwarg dict for query parameters

        :param datacube.index._api.Index index: An optional `index` object, if checking of field names is desired.
        :param str product: name of product
        :param Union[datacube.utils.Geometry|None] geopolygon: spatial bounds of the search
        :param xarray.Dataset like: spatio-temporal bounds of `like` are used for the search
        :param kwargs:
         * `measurements` - list of measurements to retrieve
         * `latitude`, `lat`, `y`, `longitude`, `lon`, `long`, `x` - tuples (min, max) bounding spatial dimensions
         * `crs` - spatial coordinate reference system to interpret the spatial bounds
         * `group_by` - observation grouping method. One of 'time', 'solar_day'. Default is 'time'
        """
        self.product = product
        self.geopolygon = query_geopolygon(geopolygon=geopolygon, **kwargs)
        if 'source_filter' in kwargs and kwargs['source_filter'] is not None:
            self.source_filter = Query(**kwargs['source_filter'])
        else:
            self.source_filter = None

        remaining_keys = set(kwargs.keys()) - set(SPATIAL_KEYS + CRS_KEYS + OTHER_KEYS)
        if index:
            unknown_keys = remaining_keys - set(index.datasets.get_field_names())
            # TODO: What about keys source filters, and what if the keys don't match up with this product...
            if unknown_keys:
                raise LookupError('Unknown arguments: ', unknown_keys)

        self.search = {}
        for key in remaining_keys:
            self.search.update(_values_to_search(**{key: kwargs[key]}))

        if like:
            assert self.geopolygon is None, "'like' with other spatial bounding parameters is not supported"
            self.geopolygon = getattr(like, 'extent', self.geopolygon)

            if 'time' not in self.search:
                time_coord = like.coords.get('time')
                if time_coord is not None:
                    self.search['time'] = _time_to_search_dims(
                        (pandas_to_datetime(time_coord.values[0]).to_pydatetime(),
                         pandas_to_datetime(time_coord.values[-1]).to_pydatetime()
                         + datetime.timedelta(milliseconds=1))  # TODO: inclusive time searches
                    )

    @property
    def search_terms(self):
        kwargs = {}
        kwargs.update(self.search)
        if self.geopolygon:
            geo_bb = self.geopolygon.to_crs(CRS('EPSG:4326')).boundingbox
            if geo_bb.bottom != geo_bb.top:
                kwargs['lat'] = Range(geo_bb.bottom, geo_bb.top)
            else:
                kwargs['lat'] = geo_bb.bottom
            if geo_bb.left != geo_bb.right:
                kwargs['lon'] = Range(geo_bb.left, geo_bb.right)
            else:
                kwargs['lon'] = geo_bb.left
        if self.product:
            kwargs['product'] = self.product
        if self.source_filter:
            kwargs['source_filter'] = self.source_filter.search_terms
        return kwargs

    def __repr__(self):
        return self.__str__()

    def __str__(self):
        return """Datacube Query:
        type = {type}
        search = {search}
        geopolygon = {geopolygon}
        """.format(type=self.product,
                   search=self.search,
                   geopolygon=self.geopolygon)


class DescriptorQuery(Query):
    def __init__(self, descriptor_request=None):
        super(DescriptorQuery, self).__init__()

        if descriptor_request is None:
            descriptor_request = {}
        if not isinstance(descriptor_request, collections.Mapping):
            raise ValueError('Could not understand descriptor {}'.format(descriptor_request))

        if 'storage_type' in descriptor_request:
            self.product = descriptor_request['storage_type']
        defined_keys = ('dimensions', 'variables', 'product', 'storage_type')
        self.search = {key: value for key, value in descriptor_request.items() if key not in defined_keys}

        if 'product' in descriptor_request:
            self.search['product_type'] = descriptor_request['product']

        if 'variables' in descriptor_request:
            self.measurements = descriptor_request['variables']

        group_by_name = 'time'
        if 'dimensions' in descriptor_request:
            dims = descriptor_request['dimensions']

            spatial_dims = {dim: v for dim, v in dims.items() if dim in SPATIAL_KEYS}
            range_params = {dim: v['range'] for dim, v in spatial_dims.items() if 'range' in v}
            crs = {c for dim, v in dims.items() for k, c in v.items() if k in CRS_KEYS}
            if len(crs) == 1:
                range_params['crs'] = crs.pop()
            elif len(crs) > 1:
                raise ValueError('Spatial dimensions must be in the same coordinate reference system: {}'.format(crs))
            self.geopolygon = _range_to_geopolygon(**range_params)

            other_dims = {dim: v for dim, v in dims.items()
                          if dim not in ['latitude', 'lat', 'y', 'longitude', 'lon', 'x']}
            self.search.update(_range_to_search(**other_dims))
            self.slices = {dim: slice(*v['array_range']) for dim, v in dims.items() if 'array_range' in v}
            groups = [v['group_by'] for dim, v in dims.items() if 'group_by' in v]
            if groups:
                group_by_name = groups[0]
        self.group_by = query_group_by(group_by_name)


def query_geopolygon(geopolygon=None, **kwargs):
    spatial_dims = {dim: v for dim, v in kwargs.items() if dim in SPATIAL_KEYS}
    crs = {v for k, v in kwargs.items() if k in CRS_KEYS}
    if len(crs) == 1:
        spatial_dims['crs'] = crs.pop()
    elif len(crs) > 1:
        raise ValueError('Spatial dimensions must be in the same coordinate reference system: {}'.format(crs))

    if geopolygon and spatial_dims:
        raise ValueError('Cannot specify "geopolygon" and one of %s at the same time' % (SPATIAL_KEYS + CRS_KEYS))

    return geopolygon or _range_to_geopolygon(**spatial_dims)


def query_group_by(group_by='time', **kwargs):
    time_grouper = GroupBy(dimension='time',
                           group_by_func=lambda ds: ds.center_time,
                           units='seconds since 1970-01-01 00:00:00',
                           sort_key=lambda ds: ds.center_time)

    solar_day_grouper = GroupBy(dimension='time',
                                group_by_func=solar_day,
                                units='seconds since 1970-01-01 00:00:00',
                                sort_key=lambda ds: ds.center_time)

    group_by_map = {
        None: time_grouper,
        'time': time_grouper,
        'solar_day': solar_day_grouper
    }

    try:
        return group_by_map[group_by]
    except KeyError:
        raise LookupError('No group by function for', group_by)


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
        if input_coords['left'] == input_coords['right']:
            if input_coords['top'] == input_coords['bottom']:
                return geometry.point(input_coords['left'], input_coords['top'], crs=input_crs)
            else:
                points = [(input_coords['left'], input_coords['bottom']),
                          (input_coords['left'], input_coords['top'])]
                return geometry.line(points, crs=input_crs)
        else:
            if input_coords['top'] == input_coords['bottom']:
                points = [(input_coords['left'], input_coords['top']),
                          (input_coords['right'], input_coords['top'])]
                return geometry.line(points, crs=input_crs)
            else:
                points = [
                    (input_coords['left'], input_coords['top']),
                    (input_coords['right'], input_coords['top']),
                    (input_coords['right'], input_coords['bottom']),
                    (input_coords['left'], input_coords['bottom']),
                    (input_coords['left'], input_coords['top'])
                ]
                return geometry.polygon(points, crs=input_crs)
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
    elif isinstance(t, datetime.datetime):
        if t.tzinfo is None:
            t = t.replace(tzinfo=tz.tzutc())
        return t

    return pandas_to_datetime(t, utc=True, infer_datetime_format=True).to_pydatetime()


def _time_to_search_dims(time_range):
    if hasattr(time_range, '__iter__') and len(time_range) == 2:
        time_range = Range(_to_datetime(time_range[0]), _to_datetime(time_range[1]))
        if time_range[0] == time_range[1]:
            return time_range[0]
        return time_range
    else:
        return _to_datetime(time_range)


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

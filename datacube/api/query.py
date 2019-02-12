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


import logging
import datetime
import collections
import warnings
import pandas

from dateutil import tz
from pandas import to_datetime as pandas_to_datetime
import numpy as np


from ..model import Range
from ..utils import geometry, datetime_to_seconds_since_1970

_LOG = logging.getLogger(__name__)


GroupBy = collections.namedtuple('GroupBy', ['dimension', 'group_by_func', 'units', 'sort_key'])

FLOAT_TOLERANCE = 0.0000001  # TODO: For DB query, use some sort of 'contains' query, rather than range overlap.
SPATIAL_KEYS = ('latitude', 'lat', 'y', 'longitude', 'lon', 'long', 'x')
CRS_KEYS = ('crs', 'coordinate_reference_system')
OTHER_KEYS = ('measurements', 'group_by', 'output_crs', 'resolution', 'set_nan', 'product', 'geopolygon', 'like',
              'source_filter')


class Query(object):
    def __init__(self, index=None, product=None, geopolygon=None, like=None, **search_terms):
        """Parses search terms in preparation for querying the Data Cube Index.

        Create a :class:`Query` object by passing it a set of search terms as keyword arguments.

        >>> query = Query(product='ls5_nbar_albers', time=('2001-01-01', '2002-01-01'))

        Use by accessing :attr:`search_terms`:

        >>> query.search_terms['time']  # doctest: +NORMALIZE_WHITESPACE
        Range(begin=datetime.datetime(2001, 1, 1, 0, 0, tzinfo=<UTC>), \
        end=datetime.datetime(2002, 1, 1, 23, 59, 59, 999999, tzinfo=tzutc()))

        By passing in an ``index``, the search parameters will be validated as existing on the ``product``.

        Used by :meth:`datacube.Datacube.find_datasets` and :meth:`datacube.Datacube.load`.

        :param datacube.index.Index index: An optional `index` object, if checking of field names is desired.
        :param str product: name of product
        :param geopolygon: spatial bounds of the search
        :type geopolygon: geometry.Geometry or None
        :param xarray.Dataset like: spatio-temporal bounds of `like` are used for the search
        :param search_terms:
         * `measurements` - list of measurements to retrieve
         * `latitude`, `lat`, `y`, `longitude`, `lon`, `long`, `x` - tuples (min, max) bounding spatial dimensions
         * `crs` - spatial coordinate reference system to interpret the spatial bounds
         * `group_by` - observation grouping method. One of `time`, `solar_day`. Default is `time`
        """
        self.product = product
        self.geopolygon = query_geopolygon(geopolygon=geopolygon, **search_terms)
        if 'source_filter' in search_terms and search_terms['source_filter'] is not None:
            self.source_filter = Query(**search_terms['source_filter'])
        else:
            self.source_filter = None

        remaining_keys = set(search_terms.keys()) - set(SPATIAL_KEYS + CRS_KEYS + OTHER_KEYS)
        if index:
            unknown_keys = remaining_keys - set(index.datasets.get_field_names())
            # TODO: What about keys source filters, and what if the keys don't match up with this product...
            if unknown_keys:
                raise LookupError('Unknown arguments: ', unknown_keys)

        self.search = {}
        for key in remaining_keys:
            self.search.update(_values_to_search(**{key: search_terms[key]}))

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
        """
        Access the search terms as a dictionary.

        :type: dict
        """
        kwargs = {}
        kwargs.update(self.search)
        if self.geopolygon:
            geo_bb = self.geopolygon.to_crs(geometry.CRS('EPSG:4326')).boundingbox
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


def query_geopolygon(geopolygon=None, **kwargs):
    spatial_dims = {dim: v for dim, v in kwargs.items() if dim in SPATIAL_KEYS}
    crs = {v for k, v in kwargs.items() if k in CRS_KEYS}
    if len(crs) == 1:
        spatial_dims['crs'] = crs.pop()
    elif len(crs) > 1:
        raise ValueError('Spatial dimensions must be in the same coordinate reference system: {}'.format(crs))

    if geopolygon is not None and len(spatial_dims) > 0:
        raise ValueError('Cannot specify "geopolygon" and one of %s at the same time' % (SPATIAL_KEYS + CRS_KEYS,))

    if geopolygon is None:
        return _range_to_geopolygon(**spatial_dims)

    return geopolygon


def query_group_by(group_by='time', **kwargs):
    if not isinstance(group_by, str):
        return group_by

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
        if value is None:
            continue
        key = key.lower()
        if key in ['latitude', 'lat', 'y']:
            input_coords['top'], input_coords['bottom'] = _value_to_range(value)
        if key in ['longitude', 'lon', 'long', 'x']:
            input_coords['left'], input_coords['right'] = _value_to_range(value)
        if key in ['crs', 'coordinate_reference_system']:
            input_crs = geometry.CRS(value)
    input_crs = input_crs or geometry.CRS('EPSG:4326')
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
    if isinstance(value, (str, float, int)):
        value = float(value)
        return value, value
    else:
        return float(value[0]), float(value[-1])


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
    if isinstance(t, (float, int)):
        t = datetime.datetime.fromtimestamp(t, tz=tz.tzutc())

    if isinstance(t, tuple):
        t = datetime.datetime(*t, tzinfo=tz.tzutc())
    elif isinstance(t, str):
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
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)

        tr_start, tr_end = time_range, time_range

        if hasattr(time_range, '__iter__') and not isinstance(time_range, str):
            l = list(time_range)
            tr_start, tr_end = l[0], l[-1]

        # Attempt conversion to isoformat
        # allows pandas.Period to handle
        # date and datetime objects
        if hasattr(tr_start, 'isoformat'):
            tr_start = tr_start.isoformat()
        if hasattr(tr_end, 'isoformat'):
            tr_end = tr_end.isoformat()

        start = _to_datetime(tr_start)
        end = _to_datetime(pandas.Period(tr_end)
                           .end_time
                           .to_pydatetime())

        tr = Range(start, end)
        if start == end:
            return tr[0]

        return tr


def _convert_to_solar_time(utc, longitude):
    seconds_per_degree = 240
    offset_seconds = int(longitude * seconds_per_degree)
    offset = datetime.timedelta(seconds=offset_seconds)
    return utc + offset


def solar_day(dataset, longitude=None):
    utc = dataset.center_time

    if longitude is None:
        m = dataset.metadata
        if hasattr(m, 'lon'):
            lon = m.lon
            longitude = (lon.begin + lon.end)*0.5
        else:
            raise ValueError('Cannot compute solar_day: dataset is missing spatial info')

    solar_time = _convert_to_solar_time(utc, longitude)
    return np.datetime64(solar_time.date(), 'D')

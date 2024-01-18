# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2024 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
"""
Storage Query and Access API module
"""


import logging
import datetime
import collections
import warnings
from typing import Optional, Union
import pandas

from dateutil import tz
from pandas import to_datetime as pandas_to_datetime
import numpy as np


from ..model import Range, Dataset
from ..utils.dates import normalise_dt, tz_aware

from odc.geo import CRS, Geometry
from odc.geo.geom import (
    lonlat_bounds,
    point,
    line,
    polygon,
    mid_longitude,
)

_LOG = logging.getLogger(__name__)


class GroupBy:
    def __init__(self, group_by_func, dimension, units, sort_key=None, group_key=None):
        """
        GroupBy Object

        :param group_by_func: Dataset -> group identifier
        :param dimension: dimension of the group key
        :param units: units of the group key
        :param sort_key: how to sort datasets in a group internally
        :param group_key: the coordinate value for a group
                          list[Dataset] -> coord value
        """
        self.group_by_func = group_by_func

        self.dimension = dimension
        self.units = units

        if sort_key is None:
            sort_key = group_by_func
        self.sort_key = sort_key

        if group_key is None:
            group_key = lambda datasets: group_by_func(datasets[0])  # noqa: E731
        self.group_key = group_key


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
        Range(begin=datetime.datetime(2001, 1, 1, 0, 0, tzinfo=datetime.timezone.utc), \
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
         * 'extra_dimension_name' (e.g. `z`) - tuples (min, max) bounding extra \
            dimensions specified by name for 3D datasets. E.g. z=(10, 30).
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
            # Retrieve known keys for extra dimensions
            known_dim_keys = set()
            if product is not None:
                datacube_products = index.products.search(product=product)
            else:
                datacube_products = index.products.get_all()

            for datacube_product in datacube_products:
                known_dim_keys.update(datacube_product.extra_dimensions.dims.keys())

            remaining_keys -= known_dim_keys

            unknown_keys = remaining_keys - set(index.products.get_field_names())
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
            geo_bb = lonlat_bounds(self.geopolygon, resolution=100_000)  # TODO: pick resolution better
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
    crs = [v for k, v in kwargs.items() if k in CRS_KEYS]
    if len(crs) == 1:
        spatial_dims['crs'] = crs[0]
    elif len(crs) > 1:
        raise ValueError('CRS is supplied twice')

    if geopolygon is not None and len(spatial_dims) > 0:
        raise ValueError('Cannot specify "geopolygon" and one of %s at the same time' % (SPATIAL_KEYS + CRS_KEYS,))

    if geopolygon is None:
        return _range_to_geopolygon(**spatial_dims)

    return geopolygon


def _extract_time_from_ds(ds: Dataset) -> datetime.datetime:
    return normalise_dt(ds.center_time)


def query_group_by(group_by='time', **kwargs):
    """
    Group by function for loading datasets

    :param group_by: group_by name, supported str are
    ::

        - time (default)
        - solar_day, see :func:`datacube.api.query.solar_day`

    or ::

        - :class:`datacube.api.query.GroupBy` object

    :return: :class:`datacube.api.query.GroupBy`
    :raises LookupError: when group_by string is not a valid dictionary key.
    """
    if isinstance(group_by, GroupBy):
        return group_by

    if not isinstance(group_by, str):
        group_by = None

    time_grouper = GroupBy(group_by_func=_extract_time_from_ds,
                           dimension='time',
                           units='seconds since 1970-01-01 00:00:00')

    solar_day_grouper = GroupBy(group_by_func=solar_day,
                                dimension='time',
                                units='seconds since 1970-01-01 00:00:00',
                                sort_key=_extract_time_from_ds,
                                group_key=lambda datasets: _extract_time_from_ds(datasets[0]))

    group_by_map = {
        None: time_grouper,
        'time': time_grouper,
        'solar_day': solar_day_grouper
    }

    try:
        return group_by_map[group_by]
    except KeyError:
        raise LookupError(
            f'No group by function for {group_by}, valid options are: {group_by_map.keys()}',  # pylint: disable=W1655
        )


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
            input_crs = CRS(value)
    input_crs = input_crs or CRS('EPSG:4326')
    if any(v is not None for v in input_coords.values()):
        if input_coords['left'] == input_coords['right']:
            if input_coords['top'] == input_coords['bottom']:
                return point(input_coords['left'], input_coords['top'], crs=input_crs)
            else:
                points = [(input_coords['left'], input_coords['bottom']),
                          (input_coords['left'], input_coords['top'])]
                return line(points, crs=input_crs)
        else:
            if input_coords['top'] == input_coords['bottom']:
                points = [(input_coords['left'], input_coords['top']),
                          (input_coords['right'], input_coords['top'])]
                return line(points, crs=input_crs)
            else:
                points = [
                    (input_coords['left'], input_coords['top']),
                    (input_coords['right'], input_coords['top']),
                    (input_coords['right'], input_coords['bottom']),
                    (input_coords['left'], input_coords['bottom']),
                    (input_coords['left'], input_coords['top'])
                ]
                return polygon(points, crs=input_crs)
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
            # If it's not a string, but is a sequence of length 2, then it's a Range
            if (
                not isinstance(value, str)
                and isinstance(value, collections.abc.Sequence)
                and len(value) == 2
            ):
                search[key] = Range(*value)
            # All other cases are default
            else:
                search[key] = value
    return search


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
        return tz_aware(t)

    return pandas_to_datetime(t, utc=True, infer_datetime_format=True).to_pydatetime()


def _time_to_search_dims(time_range):
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)

        tr_start, tr_end = time_range, time_range

        if hasattr(time_range, '__iter__') and not isinstance(time_range, str):
            tmp = list(time_range)
            if len(tmp) > 2:
                raise ValueError("Please supply start and end date only for time query")

            tr_start, tr_end = tmp[0], tmp[-1]

        # Attempt conversion to isoformat
        # allows pandas.Period to handle
        # date and datetime objects
        if hasattr(tr_start, 'isoformat'):
            tr_start = tr_start.isoformat()
        if hasattr(tr_end, 'isoformat'):
            tr_end = tr_end.isoformat()

        if tr_start is None:
            tr_start = datetime.datetime.fromtimestamp(0)
        start = _to_datetime(tr_start)
        if tr_end is None:
            tr_end = datetime.datetime.now().strftime("%Y-%m-%d")
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


def _ds_mid_longitude(dataset: Dataset) -> Optional[float]:
    m = dataset.metadata
    if hasattr(m, 'lon'):
        lon = m.lon
        return (lon.begin + lon.end)*0.5
    return None


def solar_day(dataset: Dataset, longitude: Optional[float] = None) -> np.datetime64:
    """
    Adjust Dataset timestamp for "local time" given location and convert to numpy.

    :param dataset: Dataset object from which to read time and location
    :param longitude: If supplied correct timestamp for this longitude,
                      rather than mid-point of the Dataset's footprint
    """
    utc = dataset.center_time.astimezone(datetime.timezone.utc)

    if longitude is None:
        _lon = _ds_mid_longitude(dataset)
        if _lon is None:
            raise ValueError('Cannot compute solar_day: dataset is missing spatial info')
        longitude = _lon

    solar_time = _convert_to_solar_time(utc, longitude)
    return np.datetime64(solar_time.date(), 'D')


def solar_offset(geom: Union[Geometry, Dataset],
                 precision: str = 'h') -> datetime.timedelta:
    """
    Given a geometry or a Dataset compute offset to add to UTC timestamp to get solar day right.

    This only work when geometry is "local enough".

    :param geom: Geometry with defined CRS
    :param precision: one of ``'h'`` or ``'s'``, defaults to hour precision
    """
    if isinstance(geom, Geometry):
        lon = mid_longitude(geom)
    else:
        _lon = _ds_mid_longitude(geom)
        if _lon is None:
            raise ValueError('Cannot compute solar offset, dataset is missing spatial info')
        lon = _lon

    if precision == 'h':
        return datetime.timedelta(hours=int(round(lon*24/360)))

    # 240 == (24*60*60)/360 (seconds of a day per degree of longitude)
    return datetime.timedelta(seconds=int(lon*240))

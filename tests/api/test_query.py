# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2024 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
import datetime
import pandas
import numpy as np
from types import SimpleNamespace

import pytest

from datacube.api.query import Query, query_group_by, solar_day, GroupBy, solar_offset
from datacube.model import Range
from datacube.utils import parse_time
from odc.geo import CRS


@pytest.fixture
def mock_index():
    from unittest.mock import MagicMock
    return MagicMock()


def test_query_kwargs(mock_index):
    mock_index.products.get_field_names = lambda: {u'product', u'lat', u'sat_path', 'type_id', u'time', u'lon',
                                                   u'orbit', u'instrument', u'sat_row', u'platform', 'metadata_type',
                                                   u'gsi', 'type', 'id'}

    query = Query(index=mock_index, product='ls5_nbar_albers')
    assert str(query)
    assert query.product == 'ls5_nbar_albers'
    assert query.search_terms['product'] == 'ls5_nbar_albers'

    query = Query(index=mock_index, latitude=(-35, -36), longitude=(148, 149))
    assert query.geopolygon
    assert 'lat' in query.search_terms
    assert 'lon' in query.search_terms

    query = Query(index=mock_index, latitude=-35, longitude=148)
    assert query.geopolygon
    assert 'lat' in query.search_terms
    assert 'lon' in query.search_terms

    query = Query(index=mock_index, y=(-4174726, -4180011), x=(1515184, 1523263), crs='EPSG:3577')
    assert query.geopolygon
    assert 'lat' in query.search_terms
    assert 'lon' in query.search_terms

    query = Query(index=mock_index, y=-4174726, x=1515184, crs='EPSG:3577')
    assert query.geopolygon
    assert 'lat' in query.search_terms
    assert 'lon' in query.search_terms

    query = Query(index=mock_index, y=-4174726, x=1515184, crs='EPSG:3577')
    assert query.geopolygon
    assert 'lat' in query.search_terms
    assert 'lon' in query.search_terms

    query = Query(index=mock_index, y=-4174726, x=1515184, crs=CRS('EPSG:3577'))
    assert query.geopolygon
    assert 'lat' in query.search_terms
    assert 'lon' in query.search_terms

    query = Query(index=mock_index, time='2001')
    assert 'time' in query.search

    query = Query(index=mock_index, time=('2001', '2002'))
    assert 'time' in query.search

    with pytest.raises(ValueError):
        query = Query(index=mock_index, time=('2001', '2002', '2003'))

    with pytest.raises(ValueError):
        query = Query(index=mock_index, time=['2001', '2002', '2003'])

    with pytest.raises(ValueError):
        Query(index=mock_index,
              y=-4174726, coordinate_reference_system='WGS84',
              x=1515184, crs='EPSG:3577')

    with pytest.raises(LookupError):
        Query(index=mock_index, y=-4174726, x=1515184, crs='EPSG:3577', made_up_key='NotReal')

    with pytest.raises(LookupError):
        query_group_by(group_by='magic')

    gb = query_group_by('time')
    assert isinstance(gb, GroupBy)
    assert query_group_by(group_by=gb) is gb


def format_test(start_out, end_out):
    return Range(pandas.to_datetime(start_out, utc=True).to_pydatetime(),
                 pandas.to_datetime(end_out, utc=True).to_pydatetime())


testdata = [
    ((datetime.datetime(2008, 1, 1), datetime.datetime(2008, 1, 10)),
     format_test('2008-01-01T00:00:00', '2008-01-10T00:00:00.999999')),
    ((datetime.datetime(2008, 1, 1), datetime.datetime(2008, 1, 10, 23, 0, 0)),
     format_test('2008-01-01T00:00:00', '2008-01-10T23:00:00.999999')),
    ((datetime.datetime(2008, 1, 1), datetime.datetime(2008, 1, 10, 23, 59, 40)),
     format_test('2008-01-01T00:00:00', '2008-01-10T23:59:40.999999')),
    (('2008'),
     format_test('2008-01-01T00:00:00', '2008-12-31T23:59:59.999999')),
    (('2008', '2008'),
     format_test('2008-01-01T00:00:00', '2008-12-31T23:59:59.999999')),
    (('2008', '2009'),
     format_test('2008-01-01T00:00:00', '2009-12-31T23:59:59.999999')),
    (('2008-03', '2009'),
     format_test('2008-03-01T00:00', '2009-12-31T23:59:59.999999')),
    (('2008-03', '2009-10'),
     format_test('2008-03-01T00:00', '2009-10-31T23:59:59.999999')),
    (('2008', '2009-10'),
     format_test('2008-01-01T00:00', '2009-10-31T23:59:59.999999')),
    (('2008-03-03', '2008-11'),
     format_test('2008-03-03T00:00:00', '2008-11-30T23:59:59.999999')),
    (('2008-11-14', '2008-11-30'),
     format_test('2008-11-14T00:00:00', '2008-11-30T23:59:59.999999')),
    (('2008-11-14', '2008-11-29'),
     format_test('2008-11-14T00:00:00', '2008-11-29T23:59:59.999999')),
    (('2008-11-14', '2008-11'),
     format_test('2008-11-14T00:00:00', '2008-11-30T23:59:59.999999')),
    (('2008-11-14', '2008'),
     format_test('2008-11-14T00:00:00', '2008-12-31T23:59:59.999999')),
    (('2008-11-14'),
     format_test('2008-11-14T00:00:00', '2008-11-14T23:59:59.999999')),
    (('2008-11-14', '2009-02-02'),
     format_test('2008-11-14T00:00:00', '2009-02-02T23:59:59.999999')),
    (('2008-11-14T23:33:57', '2008-11-14 23:33:57'),
     format_test('2008-11-14T23:33:57', '2008-11-14T23:33:57.999999')),
    (('2008-11-14 23:33', '2008-11-14 23:34'),
     format_test('2008-11-14T23:33:00', '2008-11-14T23:34:59.999999')),
    (('2008-11-14T23:00:00', '2008-11-14 23:35'),
     format_test('2008-11-14T23:00', '2008-11-14T23:35:59.999999')),
    (('2008-11-10T11', '2008-11-16 14:01'),
     format_test('2008-11-10T11:00', '2008-11-16T14:01:59.999999')),
    ((datetime.date(1995, 1, 1), datetime.date(1999, 1, 1)),
     format_test('1995-01-01T00:00:00', '1999-01-01T23:59:59.999999')),
    ((datetime.datetime(2008, 1, 1), datetime.datetime(2008, 1, 10, 23, 59, 40)),
     format_test('2008-01-01T00:00:00', '2008-01-10T23:59:40.999999')),
    ((datetime.date(2008, 1, 1)),
     format_test('2008-01-01T00:00:00', '2008-01-01T23:59:59.999999')),
    ((datetime.date(2008, 1, 1), None),
     format_test('2008-01-01T00:00:00', datetime.datetime.now().strftime("%Y-%m-%dT23:59:59.999999"))),
    ((None, '2008'),
     format_test(datetime.datetime.fromtimestamp(0).strftime("%Y-%m-%dT%H:%M:%S"), '2008-12-31T23:59:59.999999'))
]


@pytest.mark.parametrize('time_param,expected', testdata)
def test_time_handling(time_param, expected):
    query = Query(time=time_param)
    assert 'time' in query.search_terms
    assert query.search_terms['time'] == expected


def test_solar_day():
    _s = SimpleNamespace
    ds = _s(center_time=parse_time('1987-05-22 23:07:44.2270250Z'),
            metadata=_s(lon=Range(begin=150.415,
                                  end=152.975)))

    assert solar_day(ds) == np.datetime64('1987-05-23', 'D')
    assert solar_day(ds, longitude=0) == np.datetime64('1987-05-22', 'D')

    ds.metadata = _s()

    with pytest.raises(ValueError) as e:
        solar_day(ds)

    assert 'Cannot compute solar_day: dataset is missing spatial info' in str(e.value)

    # Test with Non-UTC timestamp in index.
    ds = _s(center_time=parse_time('1997-05-22 22:07:44.2270250-7:00'),
            metadata=_s(lon=Range(begin=-136.615,
                                  end=-134.325)))
    assert solar_day(ds) == np.datetime64('1997-05-22', 'D')
    assert solar_day(ds, longitude=0) == np.datetime64('1997-05-23', 'D')


def test_solar_offset():
    from odc.geo.geom import point
    from datetime import timedelta

    def _hr(t):
        return t.days*24 + t.seconds/3600

    def p(lon):
        return point(lon, 0, 'epsg:4326')

    assert solar_offset(p(0)) == timedelta(seconds=0)
    assert solar_offset(p(0).to_crs('epsg:3857')) == timedelta(seconds=0)

    assert solar_offset(p(179.9)) == timedelta(hours=12)
    assert _hr(solar_offset(p(-179.9))) == -12.0

    assert solar_offset(p(20), 's') != solar_offset(p(20), 'h')
    assert solar_offset(p(20), 's') < solar_offset(p(21), 's')

    _s = SimpleNamespace
    ds = _s(center_time=parse_time('1987-05-22 23:07:44.2270250Z'),
            metadata=_s(lon=Range(begin=150.415,
                                  end=152.975)))
    assert solar_offset(ds) == timedelta(hours=10)
    ds.metadata = _s()

    with pytest.raises(ValueError):
        solar_offset(ds)


def test_dateline_query_building():
    lon = Query(x=(618300, 849000),
                y=(-1876800, -1642500),
                crs='EPSG:32660').search_terms['lon']

    assert lon.begin < 180 < lon.end


def test_query_issue_1146():
    q = Query(k='AB')
    assert q.search['k'] == 'AB'


def test_query_multiple_products(mock_index):
    q = Query(index=mock_index, product=['ls5_nbar_albers', 'ls7_nbar_albers'])
    assert q.product == ['ls5_nbar_albers', 'ls7_nbar_albers']

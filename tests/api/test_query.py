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


from __future__ import absolute_import, division, print_function

import datetime

import pytest
from dateutil import tz

from ..util import isclose

from datacube.api.query import Query, DescriptorQuery, _datetime_to_timestamp, query_group_by
from datacube.model import Range


def test_convert_descriptor_query_to_search_query():
    descriptor_query = {
        'dimensions': {
            'latitude': {
                'range': (-35.5, -36.5),
            },
            'longitude': {
                'range': (148.3, 149.9)
            },
            'time': {
                'range': (datetime.datetime(2001, 5, 7), datetime.datetime(2002, 3, 9))
            }
        }
    }
    descriptor_query_dimensions = descriptor_query['dimensions']
    query = DescriptorQuery(descriptor_query)
    search_query = query.search_terms
    assert min(descriptor_query_dimensions['latitude']['range']) == search_query['lat'].begin
    assert max(descriptor_query_dimensions['latitude']['range']) == search_query['lat'].end
    assert min(descriptor_query_dimensions['longitude']['range']) == search_query['lon'].begin
    assert max(descriptor_query_dimensions['longitude']['range']) == search_query['lon'].end
    assert datetime.datetime(2001, 5, 7, tzinfo=tz.tzutc()) == search_query['time'].begin
    assert datetime.datetime(2002, 3, 9, tzinfo=tz.tzutc()) == search_query['time'].end


def test_convert_descriptor_query_to_search_query_with_slices():
    descriptor_query = {
        'dimensions': {
            'latitude': {
                'range': (-35.5, -36.5),
                'array_range': (100, 200)
            },
            'longitude': {
                'range': (148.3, 149.9),
                'array_range': (100, 200)
            },
            'time': {
                'range': (datetime.datetime(2001, 5, 7), datetime.datetime(2002, 3, 9)),
                'array_range': (5, 10)
            }
        }
    }
    query = DescriptorQuery(descriptor_query)
    assert query.slices
    assert query.slices['latitude'] == slice(100, 200)
    assert query.slices['longitude'] == slice(100, 200)
    assert query.slices['time'] == slice(5, 10)


def test_convert_descriptor_query_to_search_query_with_groupby():
    descriptor_query = {
        'dimensions': {
            'time': {
                'range': (datetime.datetime(2001, 5, 7), datetime.datetime(2002, 3, 9)),
                'group_by': 'solar_day'
            }
        }
    }
    query = DescriptorQuery(descriptor_query)
    assert query.group_by
    assert callable(query.group_by.group_by_func)
    assert query.group_by.dimension == 'time'
    assert query.group_by.units == 'seconds since 1970-01-01 00:00:00'


def test_convert_descriptor_query_to_search_query_with_crs_conversion():
    descriptor_query = {
        'dimensions': {
            'latitude': {
                'range': (-3971790.0737348166, -4101004.3359463234),
                'crs': 'EPSG:3577',
            },
            'longitude': {
                'range': (1458629.8414059384, 1616407.8831088375),
                'crs': 'EPSG:3577',
            }
        }
    }
    expected_result = {
        'lat': Range(-36.6715565808, -35.3276413143),
        'lon': Range(148.145408153, 150.070966341),
    }
    query = DescriptorQuery(descriptor_query)
    search_query = query.search_terms
    assert all(map(isclose, search_query['lat'], expected_result['lat']))
    assert all(map(isclose, search_query['lon'], expected_result['lon']))


def test_convert_descriptor_query_to_search_query_with_single_value():
    descriptor_query = {
        'dimensions': {
            'latitude': {
                'range': -3971790.0737348166,
                'crs': 'EPSG:3577',
            },
            'longitude': {
                'range': 1458629.8414059384,
                'crs': 'EPSG:3577',
            }
        }
    }
    expected_lat = -35.51609212286858
    expected_lon = 148.1454081528769
    query = DescriptorQuery(descriptor_query)
    search_query = query.search_terms
    assert abs(expected_lat - search_query['lat']) <= 1e-8
    assert abs(expected_lon - search_query['lon']) <= 1e-8


def test_descriptor_handles_bad_input():
    with pytest.raises(ValueError):
        descriptor_query = "Not a descriptor"
        DescriptorQuery(descriptor_query)

    with pytest.raises(ValueError):
        descriptor_query = ["Not a descriptor"]
        DescriptorQuery(descriptor_query)

    with pytest.raises(ValueError):
        descriptor_query = {
            'dimensions': {
                'latitude': {
                    'range': -35,
                    'crs': 'EPSG:4326',
                },
                'longitude': {
                    'range': 1458629.8414059384,
                    'crs': 'EPSG:3577',
                }
            }
        }
        DescriptorQuery(descriptor_query)


def test_datetime_to_timestamp():
    assert _datetime_to_timestamp((1990, 1, 7)) == 631670400
    assert _datetime_to_timestamp(datetime.datetime(1990, 1, 7)) == 631670400
    assert _datetime_to_timestamp(631670400) == 631670400
    assert _datetime_to_timestamp('1990-01-07T00:00:00.0Z') == 631670400


def test_query_kwargs():
    from mock import MagicMock

    mock_index = MagicMock()
    mock_index.datasets.get_field_names = lambda: {u'product', u'lat', u'sat_path', 'type_id', u'time', u'lon',
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

    query = Query(index=mock_index, time='2001')
    assert 'time' in query.search

    query = Query(index=mock_index, time=('2001', '2002'))
    assert 'time' in query.search

    with pytest.raises(ValueError):
        Query(index=mock_index,
              y=-4174726, coordinate_reference_system='WGS84',
              x=1515184, crs='EPSG:3577')

    with pytest.raises(LookupError):
        Query(index=mock_index, y=-4174726, x=1515184, crs='EPSG:3577', made_up_key='NotReal')

    with pytest.raises(LookupError):
        query_group_by(group_by='magic')

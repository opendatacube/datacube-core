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


from __future__ import absolute_import, division, print_function

import datetime

from dateutil import tz

from ..util import isclose

from datacube.model import Range
from datacube.api.query import Query, _datetime_to_timestamp

# HACK: to deal with https://github.com/mapbox/rasterio/issues/694
try:
    from rasterio.env import defenv
    defenv()
except ImportError:
    pass


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
    query = Query.from_descriptor_request(descriptor_query)
    search_query = query.search_terms
    assert min(descriptor_query_dimensions['latitude']['range']) == search_query['lat'].begin
    assert max(descriptor_query_dimensions['latitude']['range']) == search_query['lat'].end
    assert min(descriptor_query_dimensions['longitude']['range']) == search_query['lon'].begin
    assert max(descriptor_query_dimensions['longitude']['range']) == search_query['lon'].end
    assert datetime.datetime(2001, 5, 7, tzinfo=tz.tzutc()) == search_query['time'].begin
    assert datetime.datetime(2002, 3, 9, tzinfo=tz.tzutc()) == search_query['time'].end


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
    query = Query.from_descriptor_request(descriptor_query)
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
    expected_lat = -35.5160921229
    expected_lon = 148.145408153
    query = Query.from_descriptor_request(descriptor_query)
    search_query = query.search_terms
    assert min(*search_query['lat']) <= expected_lat <= max(*search_query['lat'])
    assert search_query['lat'].begin != search_query['lat'].end
    assert min(*search_query['lon']) <= expected_lon <= max(*search_query['lon'])
    assert search_query['lon'].begin != search_query['lon'].end


def test_datetime_to_timestamp():
    assert _datetime_to_timestamp((1990, 1, 7)) == 631670400
    assert _datetime_to_timestamp(datetime.datetime(1990, 1, 7)) == 631670400
    assert _datetime_to_timestamp(631670400) == 631670400
    assert _datetime_to_timestamp('1990-01-07T00:00:00.0Z') == 631670400


def test_get_descriptor_no_data():
    from datacube.api import API
    from mock import MagicMock

    mock_index = MagicMock()

    api = API(index=mock_index)

    descriptor = api.get_descriptor({})

    assert descriptor == {}


def test_get_descriptor_some_data():
    from datacube.api import API
    from mock import MagicMock

    band_10 = MagicMock(dtype='int16', )
    my_dict = {'band10': band_10}

    def getitem(name):
        return my_dict[name]

    def setitem(name, val):
        my_dict[name] = val

    mock_measurements = MagicMock()
    mock_measurements.__getitem__.side_effect = getitem
    mock_measurements.__setitem__.side_effect = setitem

    su = MagicMock()
    su.storage_type.dimensions.return_value = ['t', 'x', 'y']
    su.storage_type.measurements = mock_measurements
    su.coordinates.items
    su.storage_type.name
    su.variables.values.return_value = ['t', 'x', 'y']
    mock_index = MagicMock()

    # mock_index.datasets.get_fields.return_value = dict(product=None)
    mock_index.storage.search.return_value = [su]

    api = API(index=mock_index)

    descriptor = api.get_descriptor({})

    assert descriptor == {}

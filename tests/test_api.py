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

from .util import isclose

from datacube.api import convert_descriptor_dims_to_search_dims, convert_descriptor_dims_to_selector_dims
from datacube.model import Range


def test_convert_descriptor_query_to_search_query():
    descriptor_query = {
        'dimensions': {
            'latitude': {
                'range': (-35.5,-36.5),
            },
            'longitude': {
                'range': (148.3, 149.9)
            }
        }
    }
    descriptor_query_dimensions = descriptor_query.get('dimensions', {})
    search_query = convert_descriptor_dims_to_search_dims(descriptor_query_dimensions)
    assert len(search_query) == len(descriptor_query_dimensions)
    assert min(descriptor_query_dimensions['latitude']['range']) == search_query['lat'].begin
    assert max(descriptor_query_dimensions['latitude']['range']) == search_query['lat'].end
    assert min(descriptor_query_dimensions['longitude']['range']) == search_query['lon'].begin
    assert max(descriptor_query_dimensions['longitude']['range']) == search_query['lon'].end


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
        'lat': Range(-36.67155581104,-35.3276406574),
        'lon': Range(148.1454080502, 150.070966205676),
    }
    descriptor_query_dimensions = descriptor_query.get('dimensions', {})
    search_query = convert_descriptor_dims_to_search_dims(descriptor_query_dimensions)
    assert all(map(isclose, search_query['lat'], expected_result['lat']))
    assert all(map(isclose, search_query['lon'], expected_result['lon']))


def test_convert_descriptor_dims_to_selector_dims():
    storage_crs = 'EPSG:3577'
    descriptor_query = {
        'dimensions': {
            'x': {
                'range': (148.3, 149.9),
            },
            'y': {
                'range': (-35.5,-36.5),
            }
        }
    }
    storage_selector = {
        'y': {
            'range': (-3971790.0737348166, -4101004.3359463234)
        },
        'x': {
            'range': (1458629.8414059384, 1616407.8831088375)
        }
    }
    descriptor_query_dimensions = descriptor_query.get('dimensions', {})
    selector_dims = convert_descriptor_dims_to_selector_dims(descriptor_query_dimensions, storage_crs)
    assert len(selector_dims) == len(descriptor_query_dimensions)
    assert isclose(selector_dims['x']['range'][0], storage_selector['x']['range'][0])
    assert isclose(selector_dims['x']['range'][1], storage_selector['x']['range'][1])
    assert isclose(selector_dims['y']['range'][0], storage_selector['y']['range'][0])
    assert isclose(selector_dims['y']['range'][1], storage_selector['y']['range'][1])


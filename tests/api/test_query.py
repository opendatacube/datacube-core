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

from datacube.api.query import Query, _datetime_to_timestamp, query_group_by
from datacube.model import Range


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

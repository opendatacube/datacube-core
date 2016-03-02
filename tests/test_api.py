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

import numpy
from dateutil import tz
from affine import Affine

from .util import isclose

from datacube.model import Range, Coordinate, Variable, GeoBox
from datacube.storage.access.backends.geobox import GeoBoxStorageUnit
from datacube.api._api import _get_dimension_properties
from datacube.api._storage import MemoryStorageUnit
from datacube.api._conversion import convert_descriptor_dims_to_search_dims, convert_descriptor_dims_to_selector_dims
from datacube.api._conversion import datetime_to_timestamp
from datacube.api._stratify import _stratify_irregular_dimension
from datacube.api._dask import get_dask_array


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
    descriptor_query_dimensions = descriptor_query.get('dimensions', {})
    search_query = convert_descriptor_dims_to_search_dims(descriptor_query_dimensions)
    assert len(search_query) == len(descriptor_query_dimensions)
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
        'lat': Range(-36.67155581104, -35.3276406574),
        'lon': Range(148.1454080502, 150.070966205676),
    }
    descriptor_query_dimensions = descriptor_query.get('dimensions', {})
    search_query = convert_descriptor_dims_to_search_dims(descriptor_query_dimensions)
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
    expected_lat = -35.5160917746369
    expected_lon = 148.145408285529885
    descriptor_query_dimensions = descriptor_query.get('dimensions', {})
    search_query = convert_descriptor_dims_to_search_dims(descriptor_query_dimensions)
    assert min(*search_query['lat']) <= expected_lat <= max(*search_query['lat'])
    assert search_query['lat'].begin != search_query['lat'].end
    assert min(*search_query['lon']) <= expected_lon <= max(*search_query['lon'])
    assert search_query['lon'].begin != search_query['lon'].end


def test_convert_descriptor_dims_to_selector_dims():
    storage_crs = 'EPSG:3577'
    descriptor_query = {
        'dimensions': {
            'x': {
                'range': (148.3, 149.9),
            },
            'y': {
                'range': (-35.5, -36.5),
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


def test_convert_descriptor_dims_to_selector_dims_with_single_value():
    storage_crs = 'EPSG:3577'
    descriptor_query = {
        'dimensions': {
            'x': {
                'range': 148.3,
            },
            'y': {
                'range': -35.5,
            }
        }
    }
    descriptor_query_dimensions = descriptor_query.get('dimensions', {})
    selector_dims = convert_descriptor_dims_to_selector_dims(descriptor_query_dimensions, storage_crs)
    assert isclose(selector_dims['x']['range'], 1472748.1820625546)
    assert isclose(selector_dims['y']['range'], -3971790.0737348166)


def test_convert_descriptor_dims_to_selector_dims_with_time():
    storage_crs = 'EPSG:3577'
    descriptor_query = {
        'dimensions': {
            'time': {
                'range': ((1990, 1, 7), datetime.datetime(1995, 5, 8)),
            },
        }
    }
    expected_result = {
        'time': {
            'range': (631670400, 799891200),
        },
    }
    descriptor_query_dimensions = descriptor_query.get('dimensions', {})
    actual_result = convert_descriptor_dims_to_selector_dims(descriptor_query_dimensions, storage_crs)
    assert len(actual_result) == len(descriptor_query_dimensions)
    print(actual_result['time']['range'][0])
    assert actual_result['time']['range'][0] == expected_result['time']['range'][0]
    assert actual_result['time']['range'][1] == expected_result['time']['range'][1]


def test_datetime_to_timestamp():
    assert datetime_to_timestamp((1990, 1, 7)) == 631670400
    assert datetime_to_timestamp(datetime.datetime(1990, 1, 7)) == 631670400
    assert datetime_to_timestamp(631670400) == 631670400
    assert datetime_to_timestamp('1990-01-07T00:00:00.0Z') == 631670400


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


def test_stratify_irregular_dimension():
    coordinates_1 = {'time': Coordinate(dtype='datetime64[D]',
                                        begin=numpy.datetime64("1990-01-01"), end=numpy.datetime64("2000-01-01"),
                                        length=5, units='D')}
    coodinate_values_1 = {'time': numpy.asarray(["1990-01-01", "1991-01-01", "1992-01-01", "1999-01-01", "2000-01-01"],
                                                dtype='datetime64')}
    variables_1 = {'test': Variable(dimensions='time', dtype=int, nodata=0, units='dummy')}
    storage_unit_1 = MemoryStorageUnit(coordinates_1, variables_1, coodinate_values=coodinate_values_1)

    coordinates_2 = {'time': Coordinate(dtype='datetime64[D]',
                                        begin=numpy.datetime64("1990-01-01"), end=numpy.datetime64("2000-01-01"),
                                        length=5, units='ns')}
    coodinate_values_2 = {'time': numpy.asarray(["1991-01-01", "1992-01-01", "1993-01-01", "1999-01-01", "2001-01-01"],
                                                dtype='datetime64')}
    variables_2 = {'test': Variable(dimensions='time', dtype=int, nodata=0, units='dummy')}
    storage_unit_2 = MemoryStorageUnit(coordinates_2, variables_2, coodinate_values=coodinate_values_2)

    input_sus = [storage_unit_1, storage_unit_2]
    output_sus = _stratify_irregular_dimension(input_sus, 'time')

    all_coords = set(numpy.concatenate((coodinate_values_1['time'], coodinate_values_2['time'])))
    # for each coordinate across all storage units
    for coord in all_coords:
        # for each storage unit
        for su in output_sus:
            # if the coordinate is in the range of the storage unit
            if su.coordinates['time'].begin <= coord <= su.coordinates['time'].end:
                # assert that the coordinate is in the storage unit
                assert su and numpy.any(su.get_coord('time')[0] == coord)


def test_dask():
    GEO_PROJ = 'GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563,AUTHORITY["EPSG","7030"]],' \
               'AUTHORITY["EPSG","6326"]],PRIMEM["Greenwich",0],UNIT["degree",0.0174532925199433],' \
               'AUTHORITY["EPSG","4326"]]'

    affine1 = Affine.scale(0.1, 0.1)*Affine.translation(20, 30)
    ds1 = GeoBoxStorageUnit(GeoBox(100, 100, affine1, GEO_PROJ),
                            {'time': Coordinate(numpy.dtype(numpy.int), begin=100, end=400, length=4, units='seconds')},
                            {
                                'B10': Variable(numpy.dtype(numpy.float32), nodata=numpy.nan,
                                                dimensions=('time', 'latitude', 'longitude'), units='1')
                            })
    ds1.get_crs = lambda: {'time': None, 'latitude': None, 'longitude': None}
    affine2 = Affine.scale(0.1, 0.1)*Affine.translation(120, 130)
    ds2 = GeoBoxStorageUnit(GeoBox(100, 100, affine2, GEO_PROJ),
                            {'time': Coordinate(numpy.dtype(numpy.int), begin=100, end=400, length=4, units='seconds')},
                            {
                                'B10': Variable(numpy.dtype(numpy.float32), nodata=numpy.nan,
                                                dimensions=('time', 'latitude', 'longitude'), units='1')
                            })
    ds2.get_crs = lambda: {'time': None, 'latitude': None, 'longitude': None}

    storage_units = [ds1, ds2]
    dim_props = _get_dimension_properties(storage_units, ('time', 'latitude', 'longitude'), [])
    da = get_dask_array(storage_units, 'B10', ['time', 'latitude', 'longitude'], dim_props)
    da_computed = da.compute()
    assert da.shape == (4, 200, 200)
    assert da_computed[0, 0, 0] == 0
    assert da_computed[0, 199, 199] == 9999
    assert numpy.isnan(da_computed[0, 0, 150])
    assert numpy.isnan(da_computed[0, 150, 0])

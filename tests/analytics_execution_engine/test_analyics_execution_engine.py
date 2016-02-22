from __future__ import absolute_import

from .mock_api_response import mock_get_data, mock_get_descriptor


from datetime import datetime

from datacube.api import API
from mock import MagicMock

import pytest

from datacube.analytics.analytics_engine import AnalyticsEngine
from datacube.execution.execution_engine import ExecutionEngine

#
# Test cases for NDexpr class
#
# Tested with democube database + data provided for Milestone 2 dev branch.
#


skip = pytest.mark.skipif(True, reason="Until completed")


@pytest.fixture
def mock_api():
    mock_api = MagicMock(type=API)
    mock_api.get_descriptor.side_effect = mock_get_descriptor
    mock_api.get_data.side_effect = mock_get_data
    return mock_api


def test_get_data(mock_api):
    # Test get data

    # mock_api.get_data.return_value = mock_get_data(('band_30', 'band_40'))

    a = AnalyticsEngine(api=mock_api)
    e = ExecutionEngine(api=mock_api)

    # Lake Burley Griffin
    dimensions = {'longitude': {'range': (149.07, 149.18)},
                  'latitude':  {'range': (-35.32, -35.28)},
                  'time':      {'range': (datetime(1990, 1, 1), datetime(1990, 12, 31))}}

    arrays = a.create_array(('LANDSAT 5', 'NBAR'), ['band_30', 'band_40'], dimensions, 'get_data')

    e.execute_plan(a.plan)

    result = e.cache['get_data']
    assert 'array_result' in result
    assert 'band_30' in result['array_result']
    assert 'band_40' in result['array_result']

    assert result['array_result']['band_30'].shape == (2, 400, 400)


def test_perform_ndvi(mock_api):

    # Test perform ndvi

    a = AnalyticsEngine(api=mock_api)
    e = ExecutionEngine(api=mock_api)

    mock_api.get_descriptor.side_effect = mock_get_descriptor

    # Lake Burley Griffin
    dimensions = {'longitude': {'range': (149.07, 149.18)},
                  'latitude':  {'range': (-35.32, -35.28)},
                  'time':      {'range': (datetime(1990, 1, 1), datetime(1990, 12, 31))}}

    b40 = a.create_array(('LANDSAT 5', 'NBAR'), ['band_40'], dimensions, 'b40')
    b30 = a.create_array(('LANDSAT 5', 'NBAR'), ['band_30'], dimensions, 'b30')

    ndvi = a.apply_expression([b40, b30], '((array1 - array2) / (array1 + array2))', 'ndvi')

    res = e.execute_plan(a.plan)

    print(res)


def test_perform_old_ndvi_version(mock_api):

    # Test perform ndvi - old version for backwards compatibility

    a = AnalyticsEngine(api=mock_api)
    e = ExecutionEngine(api=mock_api)

    # Lake Burley Griffin
    dimensions = {'longitude': {'range': (149.07, 149.18)},
                  'latitude':  {'range': (-35.32, -35.28)},
                  'time':      {'range': (datetime(1990, 1, 1), datetime(1990, 12, 31))}}

    arrays = a.create_array(('LANDSAT 5', 'NBAR'), ['band_40', 'band_30'], dimensions, 'get_data')
    ndvi = a.apply_bandmath(arrays, '((array1 - array2) / (array1 + array2))', 'ndvi')

    e.execute_plan(a.plan)


def test_median_reduction_over_time(mock_api):

    # Test median reduction over time

    a = AnalyticsEngine(api=mock_api)
    e = ExecutionEngine(api=mock_api)

    # Lake Burley Griffin
    dimensions = {'longitude': {'range': (149.07, 149.18)},
                  'latitude':  {'range': (-35.32, -35.28)},
                  'time':      {'range': (datetime(1990, 1, 1), datetime(1990, 12, 31))}}

    arrays = a.create_array(('LANDSAT 5', 'NBAR'), ['band_40'], dimensions, 'get_data')

    median = a.apply_expression(arrays, 'median(array1, 0)', 'medianT')

    e.execute_plan(a.plan)


def test_old_version_median_reduction_over_time(mock_api):

    # Test median reduction over time - old version for backwards compatibility

    a = AnalyticsEngine(api=mock_api)
    e = ExecutionEngine(api=mock_api)

    # Lake Burley Griffin
    dimensions = {'longitude': {'range': (149.07, 149.18)},
                  'latitude':  {'range': (-35.32, -35.28)},
                  'time':      {'range': (datetime(1990, 1, 1), datetime(1990, 12, 31))}}

    arrays = a.create_array(('LANDSAT 5', 'NBAR'), ['band_40'], dimensions, 'get_data')

    median_t = a.apply_generic_reduction(arrays, ['time'], 'median(array1)', 'medianT')

    result = e.execute_plan(a.plan)


def test_median_reduction_over_lat_long(mock_api):

    # Test median reduction over lat/long

    a = AnalyticsEngine(api=mock_api)
    e = ExecutionEngine(api=mock_api)

    # Lake Burley Griffin
    dimensions = {'longitude': {'range': (149.07, 149.18)},
                  'latitude':  {'range': (-35.32, -35.28)},
                  'time':      {'range': (datetime(1990, 1, 1), datetime(1990, 12, 31))}}

    arrays = a.create_array(('LANDSAT 5', 'NBAR'), ['band_40'], dimensions, 'get_data')

    median = a.apply_expression(arrays, 'median(array1, 1, 2)', 'medianXY')

    e.execute_plan(a.plan)


def test_median_reduction_over_latlong_old_version(mock_api):

    # Test median reduction over lat/long - old version for backwards compatibility

    a = AnalyticsEngine(api=mock_api)
    e = ExecutionEngine(api=mock_api)

    # Lake Burley Griffin
    dimensions = {'x': {'range': (149.07, 149.18)},
                  'y':  {'range': (-35.32, -35.28)},
                  'time':      {'range': (datetime(1990, 1, 1), datetime(1990, 12, 31))}}

    arrays = a.create_array(('LANDSAT 5', 'NBAR'), ['band_40'], dimensions, 'get_data')

    median_xy = a.apply_generic_reduction(arrays, ['x', 'y'], 'median(array1)', 'medianXY')

    result = e.execute_plan(a.plan)


@skip
def test_perform_ndvi_mask_old_version(mock_api):

    # Test perform ndvi + mask - old version for backwards compatibility

    a = AnalyticsEngine(api=mock_api)
    e = ExecutionEngine(api=mock_api)

    # Lake Burley Griffin
    dimensions = {'longitude': {'range': (149.07, 149.18)},
                  'latitude':  {'range': (-35.32, -35.28)},
                  'time':      {'range': (datetime(1990, 1, 1), datetime(1990, 12, 31))}}

    arrays = a.create_array(('LANDSAT 5', 'NBAR'), ['band_40', 'band_30'], dimensions, 'get_data')
    ndvi = a.apply_bandmath(arrays, '((array1 - array2) / (array1 + array2))', 'ndvi')
    pq = a.create_array(('LANDSAT 5', 'PQ'), ['band_pixelquality'], dimensions, 'pq')
    mask = a.apply_cloud_mask(ndvi, pq, 'mask')

    e.execute_plan(a.plan)


def test_perform_ndvi_mask(mock_api):

    # Test perform ndvi + mask

    a = AnalyticsEngine(api=mock_api)
    e = ExecutionEngine(api=mock_api)

    # Lake Burley Griffin
    dimensions = {'longitude': {'range': (149.07, 149.18)},
                  'latitude':  {'range': (-35.32, -35.28)},
                  'time':      {'range': (datetime(1990, 1, 1), datetime(1990, 12, 31))}}

    b40 = a.create_array(('LANDSAT 5', 'NBAR'), ['band_40'], dimensions, 'b40')
    b30 = a.create_array(('LANDSAT 5', 'NBAR'), ['band_30'], dimensions, 'b30')
    pq = a.create_array(('LANDSAT 5', 'PQ'), ['band_pixelquality'], dimensions, 'pq')

    ndvi = a.apply_expression([b40, b30], '((array1 - array2) / (array1 + array2))', 'ndvi')
    mask = a.apply_expression([ndvi, pq], 'array1{array2}', 'mask')

    e.execute_plan(a.plan)


def test_sensor_specific_bandmath_old_version(mock_api):

    # Test sensor specific bandmath - old version for backwards compatibility

    a = AnalyticsEngine(api=mock_api)
    e = ExecutionEngine(api=mock_api)

    # Lake Burley Griffin
    dimensions = {'longitude': {'range': (149.07, 149.18)},
                  'latitude':  {'range': (-35.32, -35.28)},
                  'time':      {'range': (datetime(1990, 1, 1), datetime(1990, 12, 31))}}

    ndvi = a.apply_sensor_specific_bandmath('LANDSAT 5', 'NBAR', 'ndvi', dimensions, 'get_data', 'ndvi')

    result = e.execute_plan(a.plan)


def test_bit_of_everything(mock_api):

    # Test bit of everything

    a = AnalyticsEngine(api=mock_api)
    e = ExecutionEngine(api=mock_api)

    # Lake Burley Griffin
    dimensions = {'longitude': {'range': (149.07, 149.18)},
                  'latitude':  {'range': (-35.32, -35.28)},
                  'time':      {'range': (datetime(1990, 1, 1), datetime(1990, 12, 31))}}

    b40 = a.create_array(('LANDSAT 5', 'NBAR'), ['band_40'], dimensions, 'b40')
    b30 = a.create_array(('LANDSAT 5', 'NBAR'), ['band_30'], dimensions, 'b30')
    pq = a.create_array(('LANDSAT 5', 'PQ'), ['band_pixelquality'], dimensions, 'pq')

    ndvi = a.apply_expression([b40, b30], '((array1 - array2) / (array1 + array2))', 'ndvi')
    adjusted_ndvi = a.apply_expression(ndvi, '(ndvi*0.5)', 'adjusted_ndvi')
    mask = a.apply_expression([adjusted_ndvi, pq], 'array1{array2}', 'mask')
    median_t = a.apply_expression(mask, 'median(array1, 0)', 'medianT')

    result = e.execute_plan(a.plan)


def test_median_reduction_over_time_old_version(mock_api):

    # Test median reduction over time - old version for backwards compatibility

    a = AnalyticsEngine(api=mock_api)
    e = ExecutionEngine(api=mock_api)

    # Lake Burley Griffin
    dimensions = {'longitude': {'range': (149.07, 149.18)},
                  'latitude':  {'range': (-35.32, -35.28)},
                  'time':      {'range': (datetime(1990, 1, 1), datetime(1990, 12, 31))}}

    arrays = a.create_array(('LANDSAT 5', 'NBAR'), ['band_40'], dimensions, 'get_data')

    median_t = a.apply_reduction(arrays, ['time'], 'median', 'medianT')

    result = e.execute_plan(a.plan)

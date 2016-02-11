from __future__ import absolute_import

from datetime import datetime

import pytest

from datacube.analytics.analytics_engine import AnalyticsEngine
from datacube.execution.execution_engine import ExecutionEngine

#
# Test cases for NDexpr class
#
# Tested with democube database + data provided for Milestone 2 dev branch.
#


skip = pytest.mark.skipif(True, reason="Until completed")


@skip
def test_1(dict_api, default_collection):

    # Test get data

    a = AnalyticsEngine(api=dict_api)
    e = ExecutionEngine(api=dict_api)

    # Lake Burley Griffin
    dimensions = {'longitude': {'range': (149.07, 149.18)},
                  'latitude':  {'range': (-35.32, -35.28)},
                  'time':      {'range': (datetime(1990, 1, 1), datetime(1990, 12, 31))}}

    arrays = a.create_array(('LANDSAT 5', 'NBAR'), ['band_30', 'band_40'], dimensions, 'get_data')

    e.execute_plan(a.plan)


@skip
def test_2(dict_api, default_collection):

    # Test perform ndvi

    a = AnalyticsEngine(api=dict_api)
    e = ExecutionEngine(api=dict_api)

    # Lake Burley Griffin
    dimensions = {'longitude': {'range': (149.07, 149.18)},
                  'latitude':  {'range': (-35.32, -35.28)},
                  'time':      {'range': (datetime(1990, 1, 1), datetime(1990, 12, 31))}}

    b40 = a.create_array(('LANDSAT 5', 'NBAR'), ['band_40'], dimensions, 'b40')
    b30 = a.create_array(('LANDSAT 5', 'NBAR'), ['band_30'], dimensions, 'b30')

    ndvi = a.apply_expression([b40, b30], '((array1 - array2) / (array1 + array2))', 'ndvi')

    e.execute_plan(a.plan)


@skip
def test_3(dict_api, default_collection):

    # Test perform ndvi - old version for backwards compatibility

    a = AnalyticsEngine(api=dict_api)
    e = ExecutionEngine(api=dict_api)

    # Lake Burley Griffin
    dimensions = {'longitude': {'range': (149.07, 149.18)},
                  'latitude':  {'range': (-35.32, -35.28)},
                  'time':      {'range': (datetime(1990, 1, 1), datetime(1990, 12, 31))}}

    arrays = a.create_array(('LANDSAT 5', 'NBAR'), ['band_40', 'band_30'], dimensions, 'get_data')
    ndvi = a.apply_bandmath(arrays, '((array1 - array2) / (array1 + array2))', 'ndvi')

    e.execute_plan(a.plan)


@skip
def test_4(dict_api, default_collection):

    # Test median reduction over time

    a = AnalyticsEngine(api=dict_api)
    e = ExecutionEngine(api=dict_api)

    # Lake Burley Griffin
    dimensions = {'longitude': {'range': (149.07, 149.18)},
                  'latitude':  {'range': (-35.32, -35.28)},
                  'time':      {'range': (datetime(1990, 1, 1), datetime(1990, 12, 31))}}

    arrays = a.create_array(('LANDSAT 5', 'NBAR'), ['band_40'], dimensions, 'get_data')

    median = a.apply_expression(arrays, 'median(array1, 0)', 'medianT')

    e.execute_plan(a.plan)


@skip
def test_5(dict_api, default_collection):

    # Test median reduction over time - old version for backwards compatibility

    a = AnalyticsEngine(api=dict_api)
    e = ExecutionEngine(api=dict_api)

    # Lake Burley Griffin
    dimensions = {'longitude': {'range': (149.07, 149.18)},
                  'latitude':  {'range': (-35.32, -35.28)},
                  'time':      {'range': (datetime(1990, 1, 1), datetime(1990, 12, 31))}}

    arrays = a.create_array(('LANDSAT 5', 'NBAR'), ['band_40'], dimensions, 'get_data')

    median_t = a.apply_generic_reduction(arrays, ['time'], 'median(array1)', 'medianT')

    result = e.execute_plan(a.plan)


@skip
def test_6(dict_api, default_collection):

    # Test median reduction over lat/long

    a = AnalyticsEngine(api=dict_api)
    e = ExecutionEngine(api=dict_api)

    # Lake Burley Griffin
    dimensions = {'longitude': {'range': (149.07, 149.18)},
                  'latitude':  {'range': (-35.32, -35.28)},
                  'time':      {'range': (datetime(1990, 1, 1), datetime(1990, 12, 31))}}

    arrays = a.create_array(('LANDSAT 5', 'NBAR'), ['band_40'], dimensions, 'get_data')

    median = a.apply_expression(arrays, 'median(array1, 1, 2)', 'medianXY')

    e.execute_plan(a.plan)


@skip
def test_7(dict_api, default_collection):

    # Test median reduction over lat/long - old version for backwards compatibility

    a = AnalyticsEngine(api=dict_api)
    e = ExecutionEngine(api=dict_api)

    # Lake Burley Griffin
    dimensions = {'longitude': {'range': (149.07, 149.18)},
                  'latitude':  {'range': (-35.32, -35.28)},
                  'time':      {'range': (datetime(1990, 1, 1), datetime(1990, 12, 31))}}

    arrays = a.create_array(('LANDSAT 5', 'NBAR'), ['band_40'], dimensions, 'get_data')

    median_xy = a.apply_generic_reduction(arrays, ['latitude', 'longitude'], 'median(array1)', 'medianXY')

    result = e.execute_plan(a.plan)


@skip
def test_8(dict_api, default_collection):

    # Test perform ndvi + mask - old version for backwards compatibility

    a = AnalyticsEngine(api=dict_api)
    e = ExecutionEngine(api=dict_api)

    # Lake Burley Griffin
    dimensions = {'longitude': {'range': (149.07, 149.18)},
                  'latitude':  {'range': (-35.32, -35.28)},
                  'time':      {'range': (datetime(1990, 1, 1), datetime(1990, 12, 31))}}

    arrays = a.create_array(('LANDSAT 5', 'NBAR'), ['band_40', 'band_30'], dimensions, 'get_data')
    ndvi = a.apply_bandmath(arrays, '((array1 - array2) / (array1 + array2))', 'ndvi')
    pq = a.create_array(('LANDSAT 5', 'PQ'), ['band_pixelquality'], dimensions, 'pq')
    mask = a.apply_cloud_mask(ndvi, pq, 'mask')

    e.execute_plan(a.plan)


@skip
def test_9(dict_api, default_collection):

    # Test perform ndvi + mask

    a = AnalyticsEngine(api=dict_api)
    e = ExecutionEngine(api=dict_api)

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


@skip
def test_10(dict_api, default_collection):

    # Test sensor specific bandmath - old version for backwards compatibility

    a = AnalyticsEngine(api=dict_api)
    e = ExecutionEngine(api=dict_api)

    # Lake Burley Griffin
    dimensions = {'longitude': {'range': (149.07, 149.18)},
                  'latitude':  {'range': (-35.32, -35.28)},
                  'time':      {'range': (datetime(1990, 1, 1), datetime(1990, 12, 31))}}

    ndvi = a.apply_sensor_specific_bandmath('LANDSAT 5', 'NBAR', 'ndvi', dimensions, 'get_data', 'ndvi')

    result = e.execute_plan(a.plan)


@skip
def test_11(dict_api, default_collection):

    # Test bit of everything

    a = AnalyticsEngine(api=dict_api)
    e = ExecutionEngine(api=dict_api)

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


@skip
def test_12(dict_api, default_collection):

    # Test median reduction over time - old version for backwards compatibility

    a = AnalyticsEngine(api=dict_api)
    e = ExecutionEngine(api=dict_api)

    # Lake Burley Griffin
    dimensions = {'longitude': {'range': (149.07, 149.18)},
                  'latitude':  {'range': (-35.32, -35.28)},
                  'time':      {'range': (datetime(1990, 1, 1), datetime(1990, 12, 31))}}

    arrays = a.create_array(('LANDSAT 5', 'NBAR'), ['band_40'], dimensions, 'get_data')

    median_t = a.apply_reduction(arrays, ['time'], 'median', 'medianT')

    result = e.execute_plan(a.plan)

# coding=utf-8
"""
Module
"""
from __future__ import absolute_import

from psycopg2.extras import NumericRange

from datacube.index.fields import to_expressions
from datacube.index.postgres._fields import SimpleDocField, RangeBetweenExpression, EqualsExpression, \
    NumericRangeDocField
from datacube.model import Range
from datacube.ui import parse_expressions

_sat_field = SimpleDocField('platform', None, None, None)
_sens_field = SimpleDocField('instrument', None, None, None)
_lat_field = NumericRangeDocField('lat', None, None, None)
_fields = {
    'platform': _sat_field,
    'instrument': _sens_field,
    'lat': _lat_field
}


def test_parse_simple_expression():
    assert {'platform': 4} == parse_expressions('platform = 4')
    assert {'platform': 'LANDSAT_8'} == parse_expressions('platform = "LANDSAT_8"')
    assert {'platform': 'LANDSAT_8'} == parse_expressions('platform = LANDSAT_8')
    assert {'platform': 'LAND SAT_8'} == parse_expressions('platform = "LAND SAT_8"')

    between_exp = {'lat': Range(4, 6)}
    assert between_exp == parse_expressions('4<lat<6')
    assert between_exp == parse_expressions('6 > lat > 4')


def test_parse_uri_expression():
    assert {'uri': 'file:///f/data/test.nc'} == parse_expressions('uri = file:///f/data/test.nc')
    assert {'uri': 'file:///f/data/test.nc'} == parse_expressions('uri = "file:///f/data/test.nc"')
    assert {'uri': 'file:///f/data/test me.nc'} == parse_expressions('uri = "file:///f/data/test me.nc"')
    assert {'uri': 'file:///C:/f/data/test.nc'} == parse_expressions('uri = file:///C:/f/data/test.nc')
    assert {'uri': 'file:///C:/f/data/test.nc'} == parse_expressions('uri = "file:///C:/f/data/test.nc"')
    assert {'uri': 'file:///C:/f/data/test me.nc'} == parse_expressions('uri = "file:///C:/f/data/test me.nc"')


def test_parse_multiple_simple_expressions():
    # Multiple expressions in one command-line statement.
    # Mixed whitespace:
    between_exp = parse_expressions('platform=LS8 -4<lat<23.5 instrument="OTHER"')
    assert between_exp == {'platform': 'LS8', 'lat': Range(-4, 23.5), 'instrument': 'OTHER'}
    # Range(x,y) is "equal" to (x, y). Check explicitly that it's a range:
    assert between_exp['lat'].begin == -4


def test_build_query_expressions():
    assert [EqualsExpression(_sat_field, "LANDSAT_8")] == to_expressions(_fields.get, platform="LANDSAT_8")
    assert [
               RangeBetweenExpression(_lat_field, 4, 23.0, _range_class=NumericRange)
           ] == to_expressions(_fields.get, lat=Range(4, 23))

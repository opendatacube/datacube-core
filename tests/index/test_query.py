# coding=utf-8
"""
Module
"""

from datetime import datetime

from dateutil.tz.tz import tzutc
from psycopg2.extras import NumericRange

from datacube.index.fields import to_expressions
from datacube.drivers.postgres._fields import SimpleDocField, RangeBetweenExpression, EqualsExpression, \
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
    assert between_exp == parse_expressions('lat in range (4, 6)')


def test_parse_uri_expression():
    assert {'uri': 'file:///f/data/test.nc'} == parse_expressions('uri = file:///f/data/test.nc')
    assert {'uri': 'file:///f/data/test.nc'} == parse_expressions('uri = "file:///f/data/test.nc"')
    assert {'uri': 'file:///f/data/test me.nc'} == parse_expressions('uri = "file:///f/data/test me.nc"')
    assert {'uri': 'file:///C:/f/data/test.nc'} == parse_expressions('uri = file:///C:/f/data/test.nc')
    assert {'uri': 'file:///C:/f/data/test.nc'} == parse_expressions('uri = "file:///C:/f/data/test.nc"')
    assert {'uri': 'file:///C:/f/data/test me.nc'} == parse_expressions('uri = "file:///C:/f/data/test me.nc"')


def test_parse_dates():
    assert {'time': datetime(2014, 3, 2, tzinfo=tzutc())} == parse_expressions('time = 2014-03-02')
    assert {'time': datetime(2014, 3, 2, tzinfo=tzutc())} == parse_expressions('time = 2014-3-2')

    # A missing day defaults to the first of the month.
    # They are probably better off using in-expessions in these cases (eg. "time in 2013-01"), but it's here
    # for backwards compatibility.
    march_2014 = {
        'time': datetime(2014, 3, 1, tzinfo=tzutc())
    }
    assert march_2014 == parse_expressions('time = 2014-03')
    assert march_2014 == parse_expressions('time = 2014-3')

    implied_feb_2014 = {
        'time': Range(datetime(2014, 2, 1, tzinfo=tzutc()), datetime(2014, 3, 1, tzinfo=tzutc()))
    }
    assert implied_feb_2014 == parse_expressions('2014-02 < time < 2014-03')
    assert implied_feb_2014 == parse_expressions('time in range (2014-02, 2014-03)')


def test_parse_date_ranges():
    eighth_march_2014 = {
        'time': Range(datetime(2014, 3, 8, tzinfo=tzutc()), datetime(2014, 3, 8, 23, 59, 59, tzinfo=tzutc()))
    }
    assert eighth_march_2014 == parse_expressions('time in 2014-03-08')
    assert eighth_march_2014 == parse_expressions('time in 2014-03-8')

    march_2014 = {
        'time': Range(datetime(2014, 3, 1, tzinfo=tzutc()), datetime(2014, 3, 31, 23, 59, 59, tzinfo=tzutc()))
    }
    assert march_2014 == parse_expressions('time in 2014-03')
    assert march_2014 == parse_expressions('time in 2014-3')
    # Leap year, 28 days
    feb_2014 = {
        'time': Range(datetime(2014, 2, 1, tzinfo=tzutc()), datetime(2014, 2, 28, 23, 59, 59, tzinfo=tzutc()))
    }
    assert feb_2014 == parse_expressions('time in 2014-02')
    assert feb_2014 == parse_expressions('time in 2014-2')

    # Entire year
    year_2014 = {
        'time': Range(datetime(2014, 1, 1, tzinfo=tzutc()), datetime(2014, 12, 31, 23, 59, 59, tzinfo=tzutc()))
    }
    assert year_2014 == parse_expressions('time in 2014')


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

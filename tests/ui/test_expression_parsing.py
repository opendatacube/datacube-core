from datetime import datetime
from dateutil.tz import tzutc

from datacube.model import Range
from datacube.ui import parse_expressions


def test_parse_empty_str():
    q = parse_expressions('')
    assert q == {}


def test_between_expression():
    q = parse_expressions('time in [2014, 2015]')
    assert 'time' in q
    r = q['time']
    assert isinstance(r, Range)
    assert isinstance(r.begin, datetime)
    assert isinstance(r.end, datetime)

    for k in ('lon', 'lat', 'x', 'y'):
        q = parse_expressions('{} in [10, 11.3]'.format(k))
        assert k in q
        r = q[k]
        assert isinstance(r, Range)
        assert isinstance(r.begin, (int, float))
        assert isinstance(r.end, (int, float))
        assert r == Range(10, 11.3)


def test_parse_simple_expression():
    assert parse_expressions('platform = 4') == {'platform': 4}
    assert parse_expressions('platform = "LANDSAT_8"') == {'platform': 'LANDSAT_8'}
    assert parse_expressions('platform = LANDSAT_8') == {'platform': 'LANDSAT_8'}
    assert parse_expressions('platform = "LAND SAT_8"') == {'platform': 'LAND SAT_8'}

    assert parse_expressions('lat in [4, 6]') == {'lat': Range(4, 6)}


def test_parse_uri_expression():
    assert parse_expressions('uri = file:///f/data/test.nc') == {'uri': 'file:///f/data/test.nc'}
    assert parse_expressions('uri = "file:///f/data/test.nc"') == {'uri': 'file:///f/data/test.nc'}
    assert parse_expressions('uri = "file:///f/data/test me.nc"') == {'uri': 'file:///f/data/test me.nc'}
    assert parse_expressions('uri = file:///C:/f/data/test.nc') == {'uri': 'file:///C:/f/data/test.nc'}
    assert parse_expressions('uri = "file:///C:/f/data/test.nc"') == {'uri': 'file:///C:/f/data/test.nc'}
    assert parse_expressions('uri = "file:///C:/f/data/test me.nc"') == {'uri': 'file:///C:/f/data/test me.nc'}


def test_parse_dates():
    assert parse_expressions('time in 2014-03-02') == {
        'time': Range(begin=datetime(2014, 3, 2, 0, 0, tzinfo=tzutc()),
                      end=datetime(2014, 3, 2, 23, 59, 59, 999999, tzinfo=tzutc()))
    }

    assert parse_expressions('time in 2014-3-2') == {
        'time': Range(begin=datetime(2014, 3, 2, 0, 0, tzinfo=tzutc()),
                      end=datetime(2014, 3, 2, 23, 59, 59, 999999, tzinfo=tzutc()))
    }

    # A missing day defaults to the first of the month.
    # They are probably better off using in-expessions in these cases (eg. "time in 2013-01"), but it's here
    # for backwards compatibility.
    march_2014 = {
        'time': Range(begin=datetime(2014, 3, 1, 0, 0, tzinfo=tzutc()),
                      end=datetime(2014, 3, 31, 23, 59, 59, 999999, tzinfo=tzutc()))
    }
    assert parse_expressions('time in 2014-03') == march_2014
    assert parse_expressions('time in 2014-3') == march_2014

    implied_feb_march_2014 = {
        'time': Range(begin=datetime(2014, 2, 1, 0, 0, tzinfo=tzutc()),
                      end=datetime(2014, 3, 31, 23, 59, 59, 999999, tzinfo=tzutc()))
    }
    assert parse_expressions('time in [2014-02, 2014-03]') == implied_feb_march_2014


def test_parse_date_ranges():
    eighth_march_2014 = {
        'time': Range(datetime(2014, 3, 8, tzinfo=tzutc()), datetime(2014, 3, 8, 23, 59, 59, 999999, tzinfo=tzutc()))
    }
    assert parse_expressions('time in 2014-03-08') == eighth_march_2014
    assert parse_expressions('time in 2014-03-8') == eighth_march_2014

    march_2014 = {
        'time': Range(datetime(2014, 3, 1, tzinfo=tzutc()), datetime(2014, 3, 31, 23, 59, 59, 999999, tzinfo=tzutc()))
    }
    assert parse_expressions('time in 2014-03') == march_2014
    assert parse_expressions('time in 2014-3') == march_2014
    # Leap year, 28 days
    feb_2014 = {
        'time': Range(datetime(2014, 2, 1, tzinfo=tzutc()), datetime(2014, 2, 28, 23, 59, 59, 999999, tzinfo=tzutc()))
    }
    assert parse_expressions('time in 2014-02') == feb_2014
    assert parse_expressions('time in 2014-2') == feb_2014

    # Entire year
    year_2014 = {
        'time': Range(datetime(2014, 1, 1, tzinfo=tzutc()), datetime(2014, 12, 31, 23, 59, 59, 999999, tzinfo=tzutc()))
    }
    assert parse_expressions('time in 2014') == year_2014


def test_parse_multiple_simple_expressions():
    # Multiple expressions in one command-line statement.
    # Mixed whitespace:
    between_exp = parse_expressions('platform=LS8 lat in [-4, 23.5] instrument="OTHER"')
    assert between_exp == {'platform': 'LS8', 'lat': Range(-4, 23.5), 'instrument': 'OTHER'}
    # Range(x,y) is "equal" to (x, y). Check explicitly that it's a range:
    assert between_exp['lat'].begin == -4

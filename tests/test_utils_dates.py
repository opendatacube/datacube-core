import numpy as np
import pytest
from datetime import datetime
from datacube.utils.dates import (
    parse_duration,
    parse_interval,
    parse_time,
    _parse_time_generic,
    mk_time_coord,
    normalise_dt,
)

from dateutil.rrule import YEARLY, MONTHLY, DAILY
from dateutil.relativedelta import relativedelta


def test_parse():
    assert parse_duration('1y') == relativedelta(years=1)
    assert parse_duration('3m') == relativedelta(months=3)
    assert parse_duration('13d') == relativedelta(days=13)

    assert parse_interval('1y') == (1, YEARLY)
    assert parse_interval('3m') == (3, MONTHLY)
    assert parse_interval('13d') == (13, DAILY)

    with pytest.raises(ValueError):
        parse_duration('1p')

    with pytest.raises(ValueError):
        parse_interval('1g')

    assert _parse_time_generic('2020-01-01') == parse_time('2020-01-01')
    date = datetime(2020, 2, 3)
    assert _parse_time_generic(date) is date

    # test fallback to python parser
    assert parse_time("3 Jan 2020") == datetime(2020, 1, 3)


def test_normalise_dt():
    dt_notz = datetime(2020, 2, 14, 10, 33, 11, tzinfo=None)
    assert normalise_dt(dt_notz) is dt_notz

    assert normalise_dt("2020-01-20") == datetime(2020, 1, 20)
    assert normalise_dt('2020-03-26T10:15:32.556793+1:00').tzinfo is None
    assert normalise_dt('2020-03-26T10:15:32.556793+1:00') == datetime(2020, 3, 26, 9, 15, 32, 556793)
    assert normalise_dt('2020-03-26T10:15:32.556793+9:00') == datetime(2020, 3, 26, 1, 15, 32, 556793)


def test_mk_time_coord():
    t = mk_time_coord(['2020-01-20'])
    assert t.shape == (1,)
    assert isinstance(t.units, str)
    assert t.name == 'time'
    assert list(t.coords) == ['time']
    assert t.dtype == np.dtype('datetime64[ns]')

    some_dates = ['2020-01-20', datetime(2020, 2, 23)]
    t = mk_time_coord(some_dates)
    assert t.shape == (2,)
    assert t.dtype == np.dtype('datetime64[ns]')

    t = mk_time_coord(some_dates, name='T', units='ns')
    assert t.name == 'T'
    assert list(t.coords) == ['T']
    assert t.units == 'ns'
    assert t.attrs['units'] == 'ns'

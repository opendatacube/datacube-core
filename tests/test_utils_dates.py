import pytest
from datetime import datetime
from datacube.utils.dates import (
    parse_duration,
    parse_interval,
    parse_time,
    _parse_time_generic,
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

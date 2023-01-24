# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2020 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
"""
Date and time utility functions

Includes sequence generation functions to be used by statistics apps

"""
from typing import Union
from datetime import datetime, tzinfo

import dateutil
import dateutil.parser
from dateutil.relativedelta import relativedelta
from dateutil.rrule import YEARLY, MONTHLY, DAILY, rrule
from dateutil.tz import tzutc
import numpy as np
import xarray as xr


FREQS = {'y': YEARLY, 'm': MONTHLY, 'd': DAILY}
DURATIONS = {'y': 'years', 'm': 'months', 'd': 'days'}


def date_sequence(start, end, stats_duration, step_size):
    """
    Generate a sequence of time span tuples

    :seealso:
        Refer to `dateutil.parser.parse` for details on date parsing.

    :param str start: Start date of first interval
    :param str end: End date. The end of the last time span may extend past this date.
    :param str stats_duration: What period of time should be grouped
    :param str step_size: How far apart should the start dates be
    :return: sequence of (start_date, end_date) tuples
    """
    step_size, freq = parse_interval(step_size)
    stats_duration = parse_duration(stats_duration)
    for start_date in rrule(freq, interval=step_size, dtstart=start, until=end):
        end_date = start_date + stats_duration
        if end_date <= end:
            yield start_date, start_date + stats_duration


def parse_interval(interval):
    count, units = _split_duration(interval)
    try:
        return count, FREQS[units]
    except KeyError:
        raise ValueError('Invalid interval "{}", units not in of: {}'.format(interval, FREQS.keys))


def parse_duration(duration):
    count, units = _split_duration(duration)
    try:
        delta = {DURATIONS[units]: count}
    except KeyError:
        raise ValueError('Duration "{}" not in months or years'.format(duration))

    return relativedelta(**delta)


def _split_duration(duration):
    return int(duration[:-1]), duration[-1:]


def normalise_dt(dt: Union[str, datetime]) -> datetime:
    """ Turn strings into dates, turn timestamps with timezone info into UTC and remove timezone info.
    """
    if isinstance(dt, str):
        dt = parse_time(dt)
    if dt.tzinfo is not None:
        dt = dt.astimezone(tzutc()).replace(tzinfo=None)
    return dt


def tz_aware(dt: datetime, default: tzinfo = tzutc()) -> datetime:
    """ Ensure a datetime is timezone aware, defaulting to UTC or a user-selected default
    """
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=default)
    return dt


def tz_as_utc(dt: datetime) -> datetime:
    """ Ensure a datetime has a UTC timezone
    """
    if dt.tzinfo is None:
        return dt.replace(tzinfo=tzutc())
    return dt.astimezone(tzutc())


def mk_time_coord(dts, name='time', units=None):
    """ List[datetime] -> time coordinate for xarray
    """
    attrs = {'units': units} if units is not None else {}

    dts = [normalise_dt(dt) for dt in dts]
    data = np.asarray(dts, dtype='datetime64')
    return xr.DataArray(data,
                        name=name,
                        coords={name: data},
                        dims=(name,),
                        attrs=attrs)


def parse_time(time: Union[str, datetime]) -> datetime:
    """Convert string to datetime object

    This function deals with ISO8601 dates fast, and fallbacks to python for
    other formats.

    Calling this on datetime object is a no-op.
    """
    if isinstance(time, str):
        try:
            from ciso8601 import parse_datetime  # pylint: disable=wrong-import-position # noqa: F401
            return parse_datetime(time)
        except (ImportError, ValueError):        # pragma: no cover
            return dateutil.parser.parse(time)

    return time

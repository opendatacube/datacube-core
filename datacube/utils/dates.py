"""
Date and time utility functions

Includes sequence generation functions to be used by statistics apps

"""
from datetime import datetime

import dateutil
from dateutil.relativedelta import relativedelta
from dateutil.rrule import YEARLY, MONTHLY, DAILY, rrule
from dateutil.tz import tzutc


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
    count, units = split_duration(interval)
    try:
        return count, FREQS[units]
    except KeyError:
        raise ValueError('Invalid interval "{}", units not in of: {}'.format(interval, FREQS.keys))


def parse_duration(duration):
    count, units = split_duration(duration)
    try:
        delta = {DURATIONS[units]: count}
    except KeyError:
        raise ValueError('Duration "{}" not in months or years'.format(duration))

    return relativedelta(**delta)


def split_duration(duration):
    return int(duration[:-1]), duration[-1:]


def datetime_to_seconds_since_1970(dt):
    epoch = datetime(1970, 1, 1, 0, 0, 0, tzinfo=tzutc() if dt.tzinfo else None)
    return (dt - epoch).total_seconds()


def _parse_time_generic(time):
    if isinstance(time, str):
        return dateutil.parser.parse(time)
    return time


try:
    import ciso8601  # pylint: disable=wrong-import-position

    def parse_time(time):
        try:
            result = ciso8601.parse_datetime(time)
        except TypeError:
            return time

        if result is not None:
            return result

        return _parse_time_generic(time)
except ImportError:
    def parse_time(time):
        return _parse_time_generic(time)

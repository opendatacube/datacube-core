"""
Date sequence generation functions to be used by statistics apps


"""
from __future__ import absolute_import

from dateutil.rrule import YEARLY, MONTHLY, rrule
from dateutil.relativedelta import relativedelta
from dateutil.parser import parse

FREQS = {'y': YEARLY, 'm': MONTHLY}
DURATIONS = {'y': 'years', 'm': 'months'}


def date_sequence(start, end, stats_duration, step_size):
    """
    Generate a sequence of time span tuples

    :seealso:
        Refer to `dateutil.parser.parse` for details on date parsing.

    :param str start: Start date of first interval
    :param str end: End date. The end of the last time span may extend past this date.
    :param str stats_duration: What period of time shouuld be grouped
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

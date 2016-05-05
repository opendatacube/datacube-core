# coding=utf-8
"""
Utility functions used in storage modules
"""
from __future__ import absolute_import, division, print_function

import logging
from datetime import datetime
from dateutil.tz import tzutc

_LOG = logging.getLogger(__name__)


def namedtuples2dicts(namedtuples):
    """
    Convert a dict of namedtuples to a dict of dicts

    :param namedtuples: dict of namedtuples
    :return: dict of dicts
    """
    return {k: dict(v._asdict()) for k, v in namedtuples.items()}


def datetime_to_seconds_since_1970(dt):
    epoch = datetime(1970, 1, 1, 0, 0, 0, tzinfo=tzutc() if dt.tzinfo else None)
    return (dt - epoch).total_seconds()


def attrs_all_equal(iterable, attr_name):
    """
    Return true if everything in the iterable has the same value for `attr_name`

    :rtype: bool
    """
    return len({getattr(item, attr_name, float('nan')) for item in iterable}) <= 1


def clamp(x, l, u):
    """
    clamp x to be l <= x <= u

    >>> clamp(5, 1, 10)
    5
    >>> clamp(-1, 1, 10)
    1
    >>> clamp(12, 1, 10)
    10
    """
    return l if x < l else u if x > u else x

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

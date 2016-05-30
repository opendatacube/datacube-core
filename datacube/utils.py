# coding=utf-8
"""
Utility functions used in storage modules
"""
from __future__ import absolute_import, division, print_function

import logging
import math
from datetime import datetime

import dateutil.parser
from dateutil.tz import tzutc
from osgeo import ogr

from datacube import compat

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


def get_doc_offset(offset, document):
    """
    :type offset: list[str]
    :type document: dict

    >>> get_doc_offset(['a'], {'a': 4})
    4
    >>> get_doc_offset(['a', 'b'], {'a': {'b': 4}})
    4
    >>> get_doc_offset(['a'], {})
    Traceback (most recent call last):
    ...
    KeyError: 'a'
    """
    value = document
    for key in offset:
        value = value[key]
    return value


def parse_time(time):
    if isinstance(time, compat.string_types):
        return dateutil.parser.parse(time)
    return time


def grid_range(lower, upper, step):
    """
    Return indexes of a grid.

    >>> list(grid_range(-4.0, -1.0, 3.0))
    [-2, -1]
    >>> list(grid_range(-3.0, 0.0, 3.0))
    [-1]
    >>> list(grid_range(-2.0, 1.0, 3.0))
    [-1, 0]
    >>> list(grid_range(-1.0, 2.0, 3.0))
    [-1, 0]
    >>> list(grid_range(0.0, 3.0, 3.0))
    [0]
    >>> list(grid_range(1.0, 4.0, 3.0))
    [0, 1]
    """
    assert step > 0.0
    return range(int(math.floor(lower/step)), int(math.ceil(upper/step)))


def check_intersect(a, b):
    assert a.crs == b.crs

    def ogr_poly(poly):
        ring = ogr.Geometry(ogr.wkbLinearRing)
        for point in poly.points:
            ring.AddPoint_2D(*point)
        ring.AddPoint_2D(*poly.points[0])
        poly = ogr.Geometry(ogr.wkbPolygon)
        poly.AddGeometry(ring)
        return poly

    a = ogr_poly(a)
    b = ogr_poly(b)
    return a.Intersects(b) and not a.Touches(b)

# coding=utf-8
"""
Utility functions used in storage modules
"""
from __future__ import absolute_import, division, print_function

import gzip
import json
import logging
import math
import pathlib
from datetime import datetime
from dateutil.tz import tzutc

import dateutil.parser
import jsonschema
import numpy
from osgeo import ogr

import yaml
try:
    from yaml import CSafeLoader as SafeLoader
except ImportError:
    from yaml import SafeLoader

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
    return range(int(math.floor(lower / step)), int(math.ceil(upper / step)))


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


def data_resolution_and_offset(data):
    """
    >>> data_resolution_and_offset(numpy.array([1.5, 2.5, 3.5]))
    (1.0, 1.0)
    >>> data_resolution_and_offset(numpy.array([5, 3, 1]))
    (-2.0, 6.0)
    """
    res = (data[data.size - 1] - data[0]) / (data.size - 1.0)
    off = data[0] - 0.5 * res
    return numpy.asscalar(res), numpy.asscalar(off)


_DOCUMENT_EXTENSIONS = ('.yaml', '.yml', '.json')
_COMPRESSION_EXTENSIONS = ('', '.gz')
_ALL_SUPPORTED_EXTENSIONS = tuple(doc_type + compression_type
                                  for doc_type in _DOCUMENT_EXTENSIONS
                                  for compression_type in _COMPRESSION_EXTENSIONS)


def is_supported_document_type(path):
    """
    Does a document path look like a supported type?
    :type path: pathlib.Path
    :rtype: bool
    >>> from pathlib import Path
    >>> is_supported_document_type(Path('/tmp/something.yaml'))
    True
    >>> is_supported_document_type(Path('/tmp/something.YML'))
    True
    >>> is_supported_document_type(Path('/tmp/something.yaml.gz'))
    True
    >>> is_supported_document_type(Path('/tmp/something.tif'))
    False
    >>> is_supported_document_type(Path('/tmp/something.tif.gz'))
    False
    """
    return any([str(path).lower().endswith(suffix) for suffix in _ALL_SUPPORTED_EXTENSIONS])


def read_documents(*paths):
    """
    Read & parse documents from the filesystem (yaml or json).

    Note that a single yaml file can contain multiple documents.

    :type paths: list[pathlib.Path]
    :rtype: tuple[(pathlib.Path, dict)]
    """
    for path in paths:
        suffix = path.suffix.lower()

        # If compressed, open as gzip stream.
        opener = open
        if suffix == '.gz':
            suffix = path.suffixes[-2].lower()
            opener = gzip.open

        if suffix in ('.yaml', '.yml'):
            for parsed_doc in yaml.load_all(opener(str(path), 'r'), Loader=SafeLoader):
                yield path, parsed_doc
        elif suffix == '.json':
            yield path, json.load(opener(str(path), 'r'))
        else:
            raise ValueError('Unknown document type for {}; expected one of {!r}.'
                             .format(path.name, _ALL_SUPPORTED_EXTENSIONS))


def validate_document(document, schema):
    try:
        jsonschema.validate(document, schema)
    except jsonschema.ValidationError as e:
        raise InvalidDocException(e.message)


def generate_table(rows):
    """
    Yields strings to print a table using the data in `rows`.

    TODO: Maybe replace with Pandas

    :param rows: A sequence of sequences with the 0th element being the table
                 header
    """

    # - figure out column widths
    widths = [len(max(columns, key=len)) for columns in zip(*rows)]

    # - print the header
    header, data = rows[0], rows[1:]
    yield (
        ' | '.join(format(title, "%ds" % width) for width, title in zip(widths, header))
    )

    # Print the separator
    first_col = ''
    # - print the data
    for row in data:
        if first_col == '' and row[0] != '':
            # - print the separator
            yield '-+-'.join('-' * width for width in widths)
        first_col = row[0]

        yield (
            " | ".join(format(cdata, "%ds" % width) for width, cdata in zip(widths, row))
        )


class DatacubeException(Exception):
    """Your Data Cube has malfunctioned"""
    pass


class InvalidDocException(Exception):
    pass

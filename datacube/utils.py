# coding=utf-8
"""
Utility functions used in storage modules
"""
from __future__ import absolute_import, division, print_function

import gzip
import json
import logging
import pathlib
from datetime import datetime
from dateutil.tz import tzutc

import dateutil.parser
import jsonschema
import numpy
from osgeo import ogr
import xarray

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


def unsqueeze_data_array(da, dim, pos, coord=None):
    """
    Adds a 1-length dimension to a data array
    :param xarray.DataArray da: array to add a 1-length dimension
    :param str dim: name of new dimension
    :param int pos: position of dim
    :param dict coord:
    :return: A new xarray with a dimension added
    :rtype: xarray.DataArray
    """
    new_dims = list(da.dims)
    new_dims.insert(pos, dim)
    new_shape = da.data.shape[:pos] + (1,) + da.data.shape[pos:]
    new_data = da.data.reshape(new_shape)
    new_coords = {k: v for k, v in da.coords.items()}
    if coord:
        new_coords[dim] = coord
    return xarray.DataArray(new_data, dims=new_dims, coords=new_coords, attrs=da.attrs)


def unsqueeze_dataset(ds, dim, coord=None, pos=0):
    if coord is None:
        coord = [0]
    ds = ds.apply(unsqueeze_data_array, dim=dim, pos=pos, keep_attrs=True, coord=coord)
    return ds


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


def _points_to_ogr(points):
    ring = ogr.Geometry(ogr.wkbLinearRing)
    for point in points:
        ring.AddPoint_2D(*point)
    ring.AddPoint_2D(*points[0])
    poly = ogr.Geometry(ogr.wkbPolygon)
    poly.AddGeometry(ring)
    return poly


def _ogr_to_points(geom):
    assert geom.GetGeometryType() == ogr.wkbPolygon
    return geom.GetGeometryRef(0).GetPoints()[:-1]


def check_intersect(a, b):
    assert a.crs == b.crs

    a = _points_to_ogr(a.points)
    b = _points_to_ogr(b.points)
    return a.Intersects(b) and not a.Touches(b)


def intersect_points(a, b):
    a = _points_to_ogr(a)
    b = _points_to_ogr(b)
    return _ogr_to_points(a.Intersection(b))


def union_points(a, b):
    a = _points_to_ogr(a)
    b = _points_to_ogr(b)
    return _ogr_to_points(a.Union(b))


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


class NoDatesSafeLoader(SafeLoader):  # pylint: disable=too-many-ancestors
    @classmethod
    def remove_implicit_resolver(cls, tag_to_remove):
        """
        Removes implicit resolvers for a particular tag

        Takes care not to modify resolvers in super classes.

        We want to load datetimes as strings, not dates. We go on to
        serialise as json which doesn't have the advanced types of
        yaml, and leads to slightly different objects down the track.
        """
        if 'yaml_implicit_resolvers' not in cls.__dict__:
            cls.yaml_implicit_resolvers = cls.yaml_implicit_resolvers.copy()

        for first_letter, mappings in cls.yaml_implicit_resolvers.items():
            cls.yaml_implicit_resolvers[first_letter] = [(tag, regexp)
                                                         for tag, regexp in mappings
                                                         if tag != tag_to_remove]

NoDatesSafeLoader.remove_implicit_resolver('tag:yaml.org,2002:timestamp')


def read_documents(*paths):
    """
    Read & parse documents from the filesystem (yaml or json).

    Note that a single yaml file can contain multiple documents.

    This function will load any dates in the documents as strings. In
    the datacube we use JSON in PostgreSQL and it will turn our dates
    to strings anyway.

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
            for parsed_doc in yaml.load_all(opener(str(path), 'r'), Loader=NoDatesSafeLoader):
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


class cached_property(object):  # pylint: disable=invalid-name
    """ A property that is only computed once per instance and then replaces
        itself with an ordinary attribute. Deleting the attribute resets the
        property.

        Source: https://github.com/bottlepy/bottle/commit/fa7733e075da0d790d809aa3d2f53071897e6f76
        """

    def __init__(self, func):
        self.__doc__ = getattr(func, '__doc__')
        self.func = func

    def __get__(self, obj, cls):
        if obj is None:
            return self
        value = obj.__dict__[self.func.__name__] = self.func(obj)
        return value

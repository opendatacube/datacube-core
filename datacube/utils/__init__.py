# coding=utf-8
"""
Utility functions used in storage modules
"""
from __future__ import absolute_import, division, print_function

import gzip
import importlib
import itertools
import json
import logging
import re
import pathlib
from datetime import datetime, date

from dateutil.tz import tzutc
from math import ceil

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

URL_RE = re.compile(r'\A\s*\w+://')


def namedtuples2dicts(namedtuples):
    """
    Convert a dict of namedtuples to a dict of dicts.

    :param namedtuples: dict of namedtuples
    :return: dict of dicts
    """
    return {k: dict(v._asdict()) for k, v in namedtuples.items()}


def datetime_to_seconds_since_1970(dt):
    epoch = datetime(1970, 1, 1, 0, 0, 0, tzinfo=tzutc() if dt.tzinfo else None)
    return (dt - epoch).total_seconds()


def attrs_all_equal(iterable, attr_name):
    """
    Return true if everything in the iterable has the same value for `attr_name`.

    :rtype: bool
    """
    return len({getattr(item, attr_name, float('nan')) for item in iterable}) <= 1


def unsqueeze_data_array(da, dim, pos, coord=0, attrs=None):
    """
    Add a 1-length dimension to a data array.

    :param xarray.DataArray da: array to add a 1-length dimension
    :param str dim: name of new dimension
    :param int pos: position of dim
    :param coord: label of the coordinate on the unsqueezed dimension
    :param attrs: attributes for the coordinate dimension
    :return: A new xarray with a dimension added
    :rtype: xarray.DataArray
    """
    new_dims = list(da.dims)
    new_dims.insert(pos, dim)
    new_shape = da.data.shape[:pos] + (1,) + da.data.shape[pos:]
    new_data = da.data.reshape(new_shape)
    new_coords = {k: v for k, v in da.coords.items()}
    new_coords[dim] = xarray.DataArray([coord], dims=[dim], attrs=attrs)
    return xarray.DataArray(new_data, dims=new_dims, coords=new_coords, attrs=da.attrs)


def unsqueeze_dataset(ds, dim, coord=0, pos=0):
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
    assert l <= u
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


def _parse_time_generic(time):
    if isinstance(time, compat.string_types):
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


def intersects(a, b):
    return a.intersects(b) and not a.touches(b)


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


###
# Functions for working with YAML documents and configurations
###

_DOCUMENT_EXTENSIONS = ('.yaml', '.yml', '.json')
_COMPRESSION_EXTENSIONS = ('', '.gz')
_ALL_SUPPORTED_EXTENSIONS = tuple(doc_type + compression_type
                                  for doc_type in _DOCUMENT_EXTENSIONS
                                  for compression_type in _COMPRESSION_EXTENSIONS)


def is_supported_document_type(path):
    """
    Does a document path look like a supported type?

    :type path: Union[pathlib.Path, str]
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
        path = pathlib.Path(path)
        suffix = path.suffix.lower()

        # If compressed, open as gzip stream.
        opener = open
        if suffix == '.gz':
            suffix = path.suffixes[-2].lower()
            opener = gzip.open

        if suffix in ('.yaml', '.yml'):
            try:
                for parsed_doc in yaml.load_all(opener(str(path), 'r'), Loader=NoDatesSafeLoader):
                    yield path, parsed_doc
            except yaml.YAMLError as e:
                raise InvalidDocException('Failed to load %s: %s' % (path, e))
        elif suffix == '.json':
            try:
                yield path, json.load(opener(str(path), 'r'))
            except ValueError as e:
                raise InvalidDocException('Failed to load %s: %s' % (path, e))
        else:
            raise ValueError('Unknown document type for {}; expected one of {!r}.'
                             .format(path.name, _ALL_SUPPORTED_EXTENSIONS))


def validate_document(document, schema, schema_folder=None):
    try:
        # Allow schemas to reference other schemas in the given folder.
        def doc_reference(path):
            path = pathlib.Path(schema_folder).joinpath(path)
            if not path.exists():
                raise ValueError("Reference not found: %s" % path)
            referenced_schema = next(iter(read_documents(path)))[1]
            return referenced_schema

        jsonschema.Draft4Validator.check_schema(schema)
        ref_resolver = jsonschema.RefResolver.from_schema(
            schema,
            handlers={'': doc_reference} if schema_folder else ()
        )
        validator = jsonschema.Draft4Validator(schema, resolver=ref_resolver)
        validator.validate(document)
    except jsonschema.ValidationError as e:
        raise InvalidDocException(e.message)


# TODO: Replace with Pandas
def generate_table(rows):
    """
    Yield strings to print a table using the data in `rows`.

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


def transform_object_tree(f, o, key_transform=lambda k: k):
    """
    Apply a function (f) on all the values in the given document tree, returning a new document of
    the results.

    Recurses through container types (dicts, lists, tuples).

    Returns a new instance (deep copy) without modifying the original.

    :param f: Function to apply on values.
    :param o: document/object
    :param key_transform: Optional function to apply on any dictionary keys.

    >>> transform_object_tree(lambda a: a+1, [1, 2, 3])
    [2, 3, 4]
    >>> transform_object_tree(lambda a: a+1, {'a': 1, 'b': 2, 'c': 3}) == {'a': 2, 'b': 3, 'c': 4}
    True
    >>> transform_object_tree(lambda a: a+1, {'a': 1, 'b': (2, 3), 'c': [4, 5]}) == {'a': 2, 'b': (3, 4), 'c': [5, 6]}
    True
    >>> transform_object_tree(lambda a: a+1, {1: 1, '2': 2, 3.0: 3}, key_transform=float) == {1.0: 2, 2.0: 3, 3.0: 4}
    True
    """
    def recur(o_):
        return transform_object_tree(f, o_, key_transform=key_transform)

    if isinstance(o, dict):
        return {key_transform(k): recur(v) for k, v in o.items()}
    if isinstance(o, list):
        return [recur(v) for v in o]
    if isinstance(o, tuple):
        return tuple(recur(v) for v in o)
    return f(o)


def jsonify_document(doc):
    """
    Make a document ready for serialisation as JSON.

    Returns the new document, leaving the original unmodified.

    >>> sorted(jsonify_document({'a': (1.0, 2.0, 3.0), 'b': float("inf"), 'c': datetime(2016, 3, 11)}).items())
    [('a', (1.0, 2.0, 3.0)), ('b', 'Infinity'), ('c', '2016-03-11T00:00:00')]
    >>> # Converts keys to strings:
    >>> sorted(jsonify_document({1: 'a', '2': 'b'}).items())
    [('1', 'a'), ('2', 'b')]
    """

    def fixup_value(v):
        if isinstance(v, float):
            if v != v:
                return "NaN"
            if v == float("inf"):
                return "Infinity"
            if v == float("-inf"):
                return "-Infinity"
        if isinstance(v, (datetime, date)):
            return v.isoformat()
        if isinstance(v, numpy.dtype):
            return v.name
        return v

    return transform_object_tree(fixup_value, doc, key_transform=str)


def iter_slices(shape, chunk_size):
    """
    Generate slices for a given shape.

    E.g. ``shape=(4000, 4000), chunk_size=(500, 500)``
    Would yield 64 tuples of slices, each indexing 500x500.

    If the shape is not divisible by the chunk_size, the last chunk in each dimension will be smaller.

    :param tuple(int) shape: Shape of an array
    :param tuple(int) chunk_size: length of each slice for each dimension
    :return: Yields slices that can be used on an array of the given shape

    >>> list(iter_slices((5,), (2,)))
    [(slice(0, 2, None),), (slice(2, 4, None),), (slice(4, 5, None),)]
    """
    assert len(shape) == len(chunk_size)
    num_grid_chunks = [int(ceil(s / float(c))) for s, c in zip(shape, chunk_size)]
    for grid_index in numpy.ndindex(*num_grid_chunks):
        yield tuple(
            slice(min(d * c, stop), min((d + 1) * c, stop)) for d, c, stop in zip(grid_index, chunk_size, shape))


def is_url(url_str):
    """
    Check if url_str tastes like url (starts with blah://)

    >>> is_url('file:///etc/blah')
    True
    >>> is_url('http://greg.com/greg.txt')
    True
    >>> is_url('/etc/blah')
    False
    >>> is_url('C:/etc/blah')
    False
    """
    return URL_RE.match(url_str) is not None


def uri_to_local_path(local_uri):
    """
    Transform a URI to a platform dependent Path.

    :type local_uri: str
    :rtype: pathlib.Path

    For example on Unix:
    'file:///tmp/something.txt' -> '/tmp/something.txt'

    On Windows:
    'file:///C:/tmp/something.txt' -> 'C:\\tmp\\test.tmp'

    .. note:
        Only supports file:// schema URIs
    """
    if not local_uri:
        return None

    components = compat.urlparse(local_uri)
    if components.scheme != 'file':
        raise ValueError('Only file URIs currently supported. Tried %r.' % components.scheme)

    path = compat.url2pathname(components.path)

    return pathlib.Path(path)


def schema_validated(schema):
    """
    Decorate a class to enable validating its definition against a JSON Schema file.

    Adds a self.validate() method which takes a dict used to populate the instantiated class.

    :param pathlib.Path schema: filename of the json schema, relative to `SCHEMA_PATH`
    :return: wrapped class
    """

    def validate(cls, document):
        return validate_document(document, cls.schema, schema.parent)

    def decorate(cls):
        cls.schema = next(iter(read_documents(schema)))[1]
        cls.validate = classmethod(validate)
        return cls

    return decorate


def _set_doc_offset(offset, document, value):
    """
    :type offset: list[str]
    :type document: dict

    >>> doc = {'a': 4}
    >>> _set_doc_offset(['a'], doc, 5)
    >>> doc
    {'a': 5}
    >>> doc = {'a': {'b': 4}}
    >>> _set_doc_offset(['a', 'b'], doc, 'c')
    >>> doc
    {'a': {'b': 'c'}}
    """
    read_offset = offset[:-1]
    sub_doc = get_doc_offset(read_offset, document)
    sub_doc[offset[-1]] = value


class DocReader(object):
    def __init__(self, field_offsets, search_fields, doc):
        """
        :type field_offsets: dict[str,list[str]]
        :type doc: dict
        >>> d = DocReader({'lat': ['extent', 'lat']}, {}, doc={'extent': {'lat': 4}})
        >>> d.lat
        4
        >>> d.lat = 5
        >>> d._doc
        {'extent': {'lat': 5}}
        >>> hasattr(d, 'lat')
        True
        >>> hasattr(d, 'lon')
        False
        >>> d.lon
        Traceback (most recent call last):
        ...
        AttributeError: Unknown field 'lon'. Expected one of ['lat']
        """
        self.__dict__['_doc'] = doc
        self.__dict__['_fields'] = {name: field for name, field in search_fields.items() if hasattr(field, 'extract')}
        self._fields.update(field_offsets)

    def __getattr__(self, name):
        field = self._fields.get(name)
        if field is None:
            raise AttributeError(
                'Unknown field %r. Expected one of %r' % (
                    name, list(self._fields.keys())
                )
            )
        return self._unsafe_get_field(field)

    def __setattr__(self, name, val):
        offset = self._fields.get(name)
        if offset is None:
            raise AttributeError(
                'Unknown field %r. Expected one of %r' % (
                    name, list(self._fields.keys())
                )
            )
        return _set_doc_offset(offset, self._doc, val)

    def _unsafe_get_field(self, field):
        if isinstance(field, list):
            return get_doc_offset(field, self._doc)
        else:
            return field.extract(self._doc)

    @property
    def fields(self):
        fields = {}
        for name, field in self._fields.items():
            try:
                fields[name] = self._unsafe_get_field(field)
            except (AttributeError, KeyError, ValueError):
                continue
        return fields


def import_function(func_ref):
    """
    Import a function available in the python path.

    Expects at least one '.' in the `func_ref`,
    eg:
        `module.function_name`
        `package.module.function_name`

    :param func_ref:
    :return: function
    """
    module_name, _, func_name = func_ref.rpartition('.')
    module = importlib.import_module(module_name)
    return getattr(module, func_name)


def _tuplify(keys, values, defaults):
    assert not set(values.keys()) - set(keys), 'bad keys'
    return tuple(values.get(key, default) for key, default in zip(keys, defaults))


def _slicify(step, size):
    return (slice(i, min(i + step, size)) for i in range(0, size, step))


def _block_iter(steps, shape):
    return itertools.product(*(_slicify(step, size) for step, size in zip(steps, shape)))


def tile_iter(tile, chunk_size):
    """
    Return the sequence of chunks to split a tile into computable regions.

    :param tile: a tile of `.shape` size containing `.dim` dimensions
    :param chunk_size: dict of dimension sizes
    :return: Sequence of chunks to iterate across the entire tile
    """
    steps = _tuplify(tile.dims, chunk_size, tile.shape)
    return _block_iter(steps, tile.shape)

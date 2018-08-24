# coding=utf-8
"""
Utility functions used in storage modules
"""
from __future__ import absolute_import, division, print_function

import os
import gzip
import collections
import importlib
import itertools
import json
import logging
import math
import pathlib
import re
import toolz
from copy import deepcopy
from collections import OrderedDict
from contextlib import contextmanager
from datetime import datetime, date
from itertools import chain
from math import ceil
from uuid import UUID
from urllib.parse import urlparse, parse_qsl
from urllib.request import url2pathname

import dateutil.parser
import jsonschema
import netCDF4
import numpy
import xarray
import yaml
from dateutil.tz import tzutc
from decimal import Decimal

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


def sorted_items(d, key=None, reverse=False):
    """Given a dictionary `d` return items: (k1, v1), (k2, v2)... sorted in
    ascending order according to key.

    :param dict d: dictionary
    :param key: optional function remapping key
    :param bool reverse: If True return in descending order instead of default ascending

    """
    key = toolz.first if key is None else toolz.comp(key, toolz.first)
    return sorted(d.items(), key=key, reverse=reverse)


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
    return toolz.get_in(offset, document, no_default=True)


def get_doc_offset_safe(offset, document, value_if_missing=None):
    """
    :type offset: list[str]
    :type document: dict

    >>> get_doc_offset_safe(['a'], {'a': 4})
    4
    >>> get_doc_offset_safe(['a', 'b'], {'a': {'b': 4}})
    4
    >>> get_doc_offset_safe(['a'], {}) is None
    True
    >>> get_doc_offset_safe(['a', 'b', 'c'], {'a':{'b':{}}}, 10)
    10
    >>> get_doc_offset_safe(['a', 'b', 'c'], {'a':{'b':[]}}, 11)
    11
    """
    return toolz.get_in(offset, document, default=value_if_missing)


def _parse_time_generic(time):
    if isinstance(time, compat.string_types):
        return dateutil.parser.parse(time)
    return time


def mk_part_uri(uri, idx):
    """ Appends fragment part to the uri recording index of the part
    """
    return '{}#part={:d}'.format(uri, idx)


def get_part_from_uri(uri):
    """ Reverse of mk_part_uri

    returns None|int|string
    """
    def maybe_int(v):
        if v is None:
            return None
        try:
            return int(v)
        except ValueError:
            return v

    opts = dict(parse_qsl(urlparse(uri).fragment))
    return maybe_int(opts.get('part', None))


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


def map_with_lookahead(it, if_one=None, if_many=None):
    """It's like normal map: creates new generator by applying a function to every
    element of the original generator, but it applies `if_one` transform for
    single element sequences and `if_many` transform for multi-element sequences.

    If iterators supported `len` it would be equivalent to the code below:

    ```
    proc = if_many if len(it) > 1 else if_one
    return map(proc, it)
    ```

    :param it: Sequence to iterate over
    :param if_one: Function to apply for single element sequences
    :param if_many: Function to apply for multi-element sequences

    """
    if_one = if_one or (lambda x: x)
    if_many = if_many or (lambda x: x)

    it = iter(it)
    p1 = list(itertools.islice(it, 2))
    proc = if_many if len(p1) > 1 else if_one

    for v in itertools.chain(iter(p1), it):
        yield proc(v)


###
# Functions for working with YAML documents and configurations
###

_DOCUMENT_EXTENSIONS = ('.yaml', '.yml', '.json', '.nc')
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


def without_lineage_sources(doc, spec, inplace=False):
    """ Replace lineage.source_datasets with {}

    :param dict doc: parsed yaml/json document describing dataset
    :param spec: Product or MetadataType according to which `doc` to be interpreted
    :param bool inplace: If True modify `doc` in place
    """

    if not inplace:
        doc = deepcopy(doc)

    doc_view = spec.dataset_reader(doc)

    if 'sources' in doc_view.fields:
        doc_view.sources = {}

    return doc


def read_documents(*paths, uri=False):
    """
    Read & parse documents from the filesystem (yaml or json).

    Note that a single yaml file can contain multiple documents.

    This function will load any dates in the documents as strings. In
    the datacube we use JSON in PostgreSQL and it will turn our dates
    to strings anyway.

    :param uri: When True yield uri instead pathlib.Path

    :type paths: pathlib.Path
    :type uri: Bool
    :rtype: tuple[(pathlib.Path, dict)]
    """
    def process_yaml(path, compressed):
        opener = gzip.open if compressed else open
        with opener(str(path), 'r') as handle:
            for parsed_doc in yaml.load_all(handle, Loader=NoDatesSafeLoader):
                yield parsed_doc

    def process_json(path, compressed):
        opener = gzip.open if compressed else open
        with opener(str(path), 'r') as handle:
            yield json.load(handle)

    def process_netcdf(path, compressed):
        if compressed:
            raise InvalidDocException("Can't process gziped netcdf files")

        for doc in read_strings_from_netcdf(path, variable='dataset'):
            yield yaml.load(doc, Loader=NoDatesSafeLoader)

    procs = {
        '.yaml': process_yaml,
        '.yml': process_yaml,
        '.json': process_json,
        '.nc': process_netcdf,
    }

    def process_file(path):
        path = pathlib.Path(path)
        suffix = path.suffix.lower()

        compressed = suffix == '.gz'

        if compressed:
            suffix = path.suffixes[-2].lower()

        proc = procs.get(suffix)

        if proc is None:
            raise ValueError('Unknown document type for {}; expected one of {!r}.'
                             .format(path.name, _ALL_SUPPORTED_EXTENSIONS))

        if not uri:
            for doc in proc(path, compressed):
                yield path, doc
        else:
            def add_uri_no_part(x):
                idx, doc = x
                return path.absolute().as_uri(), doc

            def add_uri_with_part(x):
                idx, doc = x
                return mk_part_uri(path.absolute().as_uri(), idx), doc

            yield from map_with_lookahead(enumerate(proc(path, compressed)),
                                          if_one=add_uri_no_part,
                                          if_many=add_uri_with_part)

    for path in paths:
        try:
            yield from process_file(path)
        except InvalidDocException as e:
            raise e
        except (yaml.YAMLError, ValueError) as e:
            raise InvalidDocException('Failed to load %s: %s' % (path, e))
        except Exception as e:
            raise InvalidDocException('Failed to load %s: %s' % (path, e))


def netcdf_extract_string(chars):
    """
    Convert netcdf S|U chars to Unicode string.
    """
    if isinstance(chars, str):
        return chars

    chars = netCDF4.chartostring(chars)
    if chars.dtype.kind == 'U':
        return str(chars)
    else:
        return str(numpy.char.decode(chars))


def read_strings_from_netcdf(path, variable):
    """Load all of the string encoded data from a variable in a NetCDF file.

    By 'string', the CF conventions mean ascii.

    Useful for loading dataset metadata information.
    """
    with netCDF4.Dataset(str(path)) as ds:
        for chars in ds[variable]:
            yield netcdf_extract_string(chars)


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
        raise InvalidDocException(e)


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

    >>> add_one = lambda a: a + 1
    >>> transform_object_tree(add_one, [1, 2, 3])
    [2, 3, 4]
    >>> transform_object_tree(add_one, {'a': 1, 'b': 2, 'c': 3}) == {'a': 2, 'b': 3, 'c': 4}
    True
    >>> transform_object_tree(add_one, {'a': 1, 'b': (2, 3), 'c': [4, 5]}) == {'a': 2, 'b': (3, 4), 'c': [5, 6]}
    True
    >>> transform_object_tree(add_one, {1: 1, '2': 2, 3.0: 3}, key_transform=float) == {1.0: 2, 2.0: 3, 3.0: 4}
    True
    >>> # Order must be maintained
    >>> transform_object_tree(add_one, OrderedDict([('z', 1), ('w', 2), ('y', 3), ('s', 7)]))
    OrderedDict([('z', 2), ('w', 3), ('y', 4), ('s', 8)])
    """

    def recur(o_):
        return transform_object_tree(f, o_, key_transform=key_transform)

    if isinstance(o, OrderedDict):
        return OrderedDict((key_transform(k), recur(v)) for k, v in o.items())
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
    >>> sorted(jsonify_document({1: 'a', '2': Decimal('2')}).items())
    [('1', 'a'), ('2', '2')]
    >>> jsonify_document({'k': UUID("1f231570-e777-11e6-820f-185e0f80a5c0")})
    {'k': '1f231570-e777-11e6-820f-185e0f80a5c0'}
    """

    def fixup_value(v):
        if isinstance(v, float):
            if math.isfinite(v):
                return v
            if math.isnan(v):
                return "NaN"
            return "-Infinity" if v < 0 else "Infinity"
        if isinstance(v, (datetime, date)):
            return v.isoformat()
        if isinstance(v, numpy.dtype):
            return v.name
        if isinstance(v, UUID):
            return str(v)
        if isinstance(v, Decimal):
            return str(v)
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

    components = urlparse(local_uri)
    if components.scheme != 'file':
        raise ValueError('Only file URIs currently supported. Tried %r.' % components.scheme)

    path = url2pathname(components.path)

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
    def __init__(self, type_definition, search_fields, doc):
        """
        :type system_offsets: dict[str,list[str]]
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
        >>> # If that section of doc doesn't exist, treat the value not specified (None)
        >>> d = DocReader({'platform': ['platform', 'code']}, {}, doc={})
        >>> d.platform
        """
        self.__dict__['_doc'] = doc

        # The user-configurable search fields for this dataset type.
        self.__dict__['_search_fields'] = {name: field
                                           for name, field in search_fields.items()
                                           if hasattr(field, 'extract')}

        # The field offsets that the datacube itself understands: id, format, sources etc.
        # (See the metadata-type-schema.yaml or the comments in default-metadata-types.yaml)
        self.__dict__['_system_offsets'] = {name: field
                                            for name, field in type_definition.items()
                                            if name != 'search_fields'}

    def __getattr__(self, name):
        offset = self._system_offsets.get(name)
        field = self._search_fields.get(name)
        if offset:
            return get_doc_offset_safe(offset, self._doc)
        elif field:
            return field.extract(self._doc)
        else:
            raise AttributeError(
                'Unknown field %r. Expected one of %r' % (
                    name, list(chain(self._system_offsets.keys(), self._search_fields.keys()))
                )
            )

    def __setattr__(self, name, val):
        offset = self._system_offsets.get(name)
        if offset is None:
            raise AttributeError(
                'Unknown field offset %r. Expected one of %r' % (
                    name, list(self._fields.keys())
                )
            )
        return _set_doc_offset(offset, self._doc, val)

    @property
    def fields(self):
        fields = {}
        fields.update(self.search_fields)
        fields.update(self.system_fields)
        return fields

    @property
    def search_fields(self):
        fields = {}
        for name, field in self._search_fields.items():
            try:
                fields[name] = field.extract(self._doc)
            except (AttributeError, KeyError, ValueError):
                continue
        return fields

    @property
    def system_fields(self):
        fields = {}
        for name, offset in self._system_offsets.items():
            try:
                fields[name] = get_doc_offset(offset, self._doc)
            except (AttributeError, KeyError, ValueError):
                continue
        return fields

    def __dir__(self):
        return list(self.fields)


class SimpleDocNav(object):
    """Allows navigation of Dataset metadata document lineage tree without
    creating Dataset objects.

    """

    def __init__(self, doc):
        if not isinstance(doc, collections.Mapping):
            raise ValueError("")

        self._doc = doc
        self._doc_without = None
        self._sources_path = ('lineage', 'source_datasets')
        self._sources = None

    @property
    def doc(self):
        return self._doc

    @property
    def doc_without_lineage_sources(self):
        if self._doc_without is None:
            self._doc_without = toolz.assoc_in(self._doc, self._sources_path, {})

        return self._doc_without

    @property
    def id(self):
        return self._doc.get('id', None)

    @property
    def sources(self):
        if self._sources is None:
            self._sources = {k: SimpleDocNav(v)
                             for k, v in get_doc_offset_safe(self._sources_path, self._doc, {}).items()}
        return self._sources

    @property
    def sources_path(self):
        return self._sources_path


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


def write_user_secret_file(text, fname, in_home_dir=False, mode='w'):
    """Write file only readable/writeable by the user"""

    if in_home_dir:
        fname = os.path.join(os.environ['HOME'], fname)

    open_flags = os.O_WRONLY | os.O_CREAT | os.O_TRUNC
    access = 0o600  # Make sure file is readable by current user only
    with os.fdopen(os.open(fname, open_flags, access), mode) as handle:
        handle.write(text)
        handle.close()


def slurp(fname, in_home_dir=False, mode='r'):
    """
    Read the entire file into a string
    :param fname: file path
    :param in_home_dir: if True treat fname as a path relative to $HOME folder
    :return: Content of a file or None if file doesn't exist or can not be read for any other reason
    """
    if in_home_dir:
        fname = os.path.join(os.environ['HOME'], fname)
    try:
        with open(fname, mode) as handle:
            return handle.read()
    except IOError:
        return None


def gen_password(num_random_bytes=12):
    """ Generate random password
    """
    import base64
    return base64.urlsafe_b64encode(os.urandom(num_random_bytes)).decode('utf-8')


@contextmanager
def ignore_exceptions_if(ignore_errors):
    """Ignore Exceptions raised within this block if ignore_errors is True"""
    if ignore_errors:
        try:
            yield
        except OSError as e:
            _LOG.warning('Ignoring Exception: %s', e)
    else:
        yield


def _readable_offset(offset):
    return '.'.join(map(str, offset))

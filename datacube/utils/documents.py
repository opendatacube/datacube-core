# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2020 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
"""
Functions for working with YAML documents and configurations
"""
import gzip
import json
import logging
import sys
import collections.abc
from collections import OrderedDict
from contextlib import contextmanager
from itertools import chain
from pathlib import Path
from urllib.parse import urlparse
from urllib.request import urlopen
from typing import Dict, Any, Mapping
from copy import deepcopy
from uuid import UUID

import numpy
import toolz  # type: ignore[import]
import yaml

try:
    from yaml import CSafeLoader as SafeLoader  # type: ignore
except ImportError:
    from yaml import SafeLoader  # type: ignore

from datacube.utils.generic import map_with_lookahead
from datacube.utils.uris import mk_part_uri, as_url, uri_to_local_path

PY35 = sys.version_info <= (3, 6)
_LOG = logging.getLogger(__name__)


@contextmanager
def _open_from_s3(url):
    o = urlparse(url)
    if o.scheme != 's3':
        raise RuntimeError("Abort abort I don't know how to open non s3 urls")

    from .aws import s3_open
    yield s3_open(url)


def _open_with_urllib(url):
    return urlopen(url)


_PROTOCOL_OPENERS = {
    's3': _open_from_s3,
    'ftp': _open_with_urllib,
    'http': _open_with_urllib,
    'https': _open_with_urllib,
    'file': _open_with_urllib
}


def load_from_yaml(handle, parse_dates=False):
    loader = SafeLoader if parse_dates else NoDatesSafeLoader
    yield from yaml.load_all(handle, Loader=loader)


def parse_yaml(doc: str) -> Mapping[str, Any]:
    """ Convert a single document yaml string into a parsed document
    """
    return yaml.load(doc, Loader=SafeLoader)


def load_from_json(handle):
    yield json.load(handle)


def load_from_netcdf(path):
    for doc in read_strings_from_netcdf(path, variable='dataset'):
        yield yaml.load(doc, Loader=NoDatesSafeLoader)


_PARSERS = {
    '.yaml': load_from_yaml,
    '.yml': load_from_yaml,
    '.json': load_from_json,
}


def load_documents(path):
    """
    Load document/s from the specified path.

    At the moment can handle:

     - JSON and YAML locally and remotely.
     - Compressed JSON and YAML locally
     - Data Cube Dataset Documents inside local NetCDF files.

    :param path: path or URI to load documents from
    :return: generator of dicts
    """
    path = str(path)
    url = as_url(path)
    scheme = urlparse(url).scheme
    compressed = url[-3:] == '.gz'

    if scheme == 'file' and path[-3:] == '.nc':
        path = uri_to_local_path(url)
        yield from load_from_netcdf(path)
    else:
        with _PROTOCOL_OPENERS[scheme](url) as fh:
            if compressed:
                fh = gzip.open(fh)
                path = path[:-3]

            suffix = Path(path).suffix

            parser = _PARSERS[suffix]

            yield from parser(fh)


def read_documents(*paths, uri=False):
    """
    Read and parse documents from the filesystem or remote URLs (yaml or json).

    Note that a single yaml file can contain multiple documents.

    This function will load any dates in the documents as strings. In
    Data Cube we store JSONB in PostgreSQL and it will turn our dates
    into strings anyway.

    :param uri: When True yield URIs instead of Paths
    :param paths: input Paths or URIs
    :type uri: Bool
    :rtype: tuple[(str, dict)]
    """

    def process_file(path):
        docs = load_documents(path)

        if not uri:
            for doc in docs:
                yield path, doc
        else:
            url = as_url(path)

            def add_uri_no_part(x):
                idx, doc = x
                return url, doc

            def add_uri_with_part(x):
                idx, doc = x
                return mk_part_uri(url, idx), doc

            yield from map_with_lookahead(enumerate(docs),
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
    import netCDF4  # type: ignore[import]

    if isinstance(chars, str):
        return chars

    chars = netCDF4.chartostring(chars)
    if chars.dtype.kind == 'U':
        return str(chars)
    else:
        return str(numpy.char.decode(chars))


def read_strings_from_netcdf(path, variable):
    """
    Load all of the string encoded data from a variable in a NetCDF file.

    By 'string', the CF conventions mean ascii.

    Useful for loading dataset metadata information.
    """
    import netCDF4

    with netCDF4.Dataset(str(path)) as ds:
        for chars in ds[variable]:
            yield netcdf_extract_string(chars)


def validate_document(document, schema, schema_folder=None):
    import jsonschema

    try:
        # Allow schemas to reference other schemas in the given folder.
        def doc_reference(path):
            path = Path(schema_folder).joinpath(path)
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


_DOCUMENT_EXTENSIONS = ('.yaml', '.yml', '.json', '.nc')
_COMPRESSION_EXTENSIONS = ('', '.gz')
_ALL_SUPPORTED_EXTENSIONS = tuple(doc_type + compression_type
                                  for doc_type in _DOCUMENT_EXTENSIONS
                                  for compression_type in _COMPRESSION_EXTENSIONS)


def is_supported_document_type(path):
    """
    Does a document path look like a supported type?

    :type path: Union[Path, str]
    :rtype: bool
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


class InvalidDocException(Exception):  # noqa: N818
    pass


def get_doc_offset(offset, document):
    """
    :type offset: list[str]
    :type document: dict

    """
    return toolz.get_in(offset, document, no_default=True)


def get_doc_offset_safe(offset, document, value_if_missing=None):
    """
    :type offset: list[str]
    :type document: dict

    """
    return toolz.get_in(offset, document, default=value_if_missing)


def transform_object_tree(f, o, key_transform=lambda k: k):
    """
    Apply a function (f) on all the values in the given document tree (o), returning a new document of
    the results.

    Recurses through container types (dicts, lists, tuples).

    Returns a new instance (deep copy) without modifying the original.

    :param f: Function to apply on values.
    :param o: document/object
    :param key_transform: Optional function to apply on any dictionary keys.

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


def metadata_subset(element, document) -> bool:
    """
    Recursively check if one metadata document/object is a subset of another

    :param element: The document/object to search for
    :param document: The document/object to search in
    :return: True if element is a subset of document
    """
    if isinstance(element, dict) and isinstance(document, dict):
        matches = True
        for k in element.keys():
            if k not in document or not metadata_subset(element[k], document[k]):
                matches = False
                break
        if matches:
            return True
        for k in document.keys():
            if metadata_subset(element, document[k]):
                return True
    elif isinstance(document, dict):
        for k in document.keys():
            if metadata_subset(element, document[k]):
                return True
    elif isinstance(element, list) or isinstance(element, tuple):
        matches = True
        for i in element:
            if not metadata_subset(i, document):
                matches = False
                break
        if matches:
            return True
    elif isinstance(document, list) or isinstance(document, tuple):
        for i in document:
            if metadata_subset(element, i):
                return True
    else:
        return element == document
    return False


class SimpleDocNav(object):
    """
    Allows navigation of Dataset metadata document lineage tree without
    creating full Dataset objects.

    This has the assumption that a dictionary of source datasets is
    found at the offset ``lineage -> source_datasets`` inside each
    dataset dictionary.

    """

    def __init__(self, doc):
        if not isinstance(doc, collections.abc.Mapping):
            raise ValueError("")

        self._doc = doc
        self._doc_without = None
        self._sources_path = ('lineage', 'source_datasets')
        self._sources = None
        self._doc_uuid = None

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
        if not self._doc_uuid:
            doc_id = self._doc.get('id', None)
            if doc_id:
                self._doc_uuid = doc_id if isinstance(doc_id, UUID) else UUID(doc_id)
        return self._doc_uuid

    @property
    def sources(self):
        if self._sources is None:
            self._sources = {k: SimpleDocNav(v)
                             for k, v in get_doc_offset_safe(self._sources_path, self._doc, {}).items()}
        return self._sources

    @property
    def sources_path(self):
        return self._sources_path

    @property
    def location(self):
        return self._doc.get('location', None)

    def without_location(self):
        if self.location is None:
            return self
        return SimpleDocNav(toolz.dissoc(self._doc, 'location'))


def _set_doc_offset(offset, document, value):
    """
    :type offset: list[str]
    :type document: dict

    """
    read_offset = offset[:-1]
    sub_doc = get_doc_offset(read_offset, document)
    sub_doc[offset[-1]] = value


class DocReader(object):
    def __init__(self, type_definition, search_fields, doc):
        """
        :type system_offsets: dict[str,list[str]]
        :type doc: dict
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


def without_lineage_sources(doc: Dict[str, Any],
                            spec,
                            inplace: bool = False) -> Dict[str, Any]:
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


def _readable_offset(offset):
    return '.'.join(map(str, offset))

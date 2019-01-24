"""
Utility functions
"""

import logging
import os
import pathlib

from datacube.utils.dates import parse_time
from .dates import datetime_to_seconds_since_1970
from .documents import InvalidDocException, SimpleDocNav, DocReader, is_supported_document_type, \
    read_strings_from_netcdf, read_documents, validate_document, NoDatesSafeLoader, get_doc_offset, \
    get_doc_offset_safe, netcdf_extract_string, without_lineage_sources
from .math import unsqueeze_data_array, iter_slices, unsqueeze_dataset, data_resolution_and_offset
from .py import cached_property, ignore_exceptions_if, import_function
from .serialise import jsonify_document
from .uris import is_url, uri_to_local_path, get_part_from_uri, mk_part_uri

_LOG = logging.getLogger(__name__)


class DatacubeException(Exception):
    """Your Data Cube has malfunctioned"""
    pass


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
    Read an entire file into a string

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
    """
    Generate random password
    """
    import base64
    return base64.urlsafe_b64encode(os.urandom(num_random_bytes)).decode('utf-8')


def _readable_offset(offset):
    return '.'.join(map(str, offset))

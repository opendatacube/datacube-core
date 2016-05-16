# coding=utf-8
"""
API for storage indexing, access and search.
"""
from __future__ import absolute_import, division

import jsonschema
import pathlib
import yaml

try:
    from yaml import CSafeLoader as SafeLoader
except ImportError:
    from yaml import SafeLoader

from datacube.index.fields import InvalidDocException

STORAGE_TYPE_SCHEMA_PATH = pathlib.Path(__file__).parent.joinpath('storage-type-schema.yaml')


def _ensure_valid(descriptor):
    try:
        jsonschema.validate(
            descriptor,
            yaml.load(STORAGE_TYPE_SCHEMA_PATH.open('r'), Loader=SafeLoader)
        )
    except jsonschema.ValidationError as e:
        raise InvalidDocException(e.message)

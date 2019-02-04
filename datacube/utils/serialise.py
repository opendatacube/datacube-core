# coding=utf-8
"""
Serialise function used in YAML output
"""

import math
from collections import OrderedDict
from datetime import datetime, date
from decimal import Decimal
from uuid import UUID

import numpy
import yaml

from datacube.utils.documents import transform_object_tree
from datacube.model._base import Range


class SafeDatacubeDumper(yaml.SafeDumper):  # pylint: disable=too-many-ancestors
    pass


def _dict_representer(dumper, data):
    return dumper.represent_mapping(yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, data.items())


def _reduced_accuracy_decimal_representer(dumper: yaml.Dumper, data: Decimal) -> yaml.Node:
    return dumper.represent_float(float(data))


def _range_representer(dumper: yaml.Dumper, data: Range) -> yaml.Node:
    begin, end = data

    # pyyaml doesn't output timestamps in flow style as timestamps(?)
    if isinstance(begin, datetime):
        begin = begin.isoformat()
    if isinstance(end, datetime):
        end = end.isoformat()

    return dumper.represent_mapping(
        yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG,
        (('begin', begin), ('end', end)),
        flow_style=True
    )


SafeDatacubeDumper.add_representer(OrderedDict, _dict_representer)
SafeDatacubeDumper.add_representer(Decimal, _reduced_accuracy_decimal_representer)
SafeDatacubeDumper.add_representer(Range, _range_representer)


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

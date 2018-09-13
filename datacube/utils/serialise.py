# coding=utf-8
"""
Serialise function used in YAML output
"""
from __future__ import absolute_import, division, print_function

from collections import OrderedDict
from datetime import datetime
from decimal import Decimal

import yaml
from datacube.model import Range


class SafeDatacubeDumper(yaml.SafeDumper):  # pylint: disable=too-many-ancestors
    pass


def _dict_representer(dumper, data):
    return dumper.represent_mapping(yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, data.items())


def _range_representer(dumper, data):
    # type: (yaml.Dumper, Range) -> Node
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


def _reduced_accuracy_decimal_representer(dumper, data):
    # type: (yaml.Dumper, Decimal) -> Node
    return dumper.represent_float(
        float(data)
    )


SafeDatacubeDumper.add_representer(OrderedDict, _dict_representer)
SafeDatacubeDumper.add_representer(Range, _range_representer)
SafeDatacubeDumper.add_representer(Decimal, _reduced_accuracy_decimal_representer)

# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2025 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
import datetime
import sys
from collections import namedtuple
from collections.abc import Iterable
from typing import Generic, NamedTuple, Protocol, TypeAlias, TypeVar

from odc.geo import Geometry

T = TypeVar("T")
_T_contra = TypeVar("_T_contra", contravariant=True)


class Orderable(Protocol[_T_contra]):
    def __lt__(self, other: _T_contra) -> bool: ...
    def __gt__(self, other: _T_contra) -> bool: ...


OrderedT = TypeVar("OrderedT", bound=Orderable)

if sys.version_info < (3, 11):
    # NamedTuples with multiple inheritance are not supported in Python 3.10.
    Range = namedtuple("Range", ("begin", "end"))
    Not = namedtuple("Not", "value")
else:

    class Range(NamedTuple, Generic[OrderedT]):
        """
        A named tuple representing a range.

        :param begin: start of the range.
        :param end: end of the range.
        """

        begin: OrderedT
        end: OrderedT

    class Not(NamedTuple, Generic[T]):
        """
        A named tuple representing negated value.

        :param value: The value to be negated.
        """

        value: T


def ranges_overlap(ra: Range, rb: Range) -> bool:
    """
    Check whether two ranges overlap.

    (Assumes the start of the range is included in the range and the end of the range is not.)

    :return: True if the ranges overlap.
    """
    if ra.begin <= rb.begin:
        return ra.end > rb.begin
    return rb.end > ra.begin


QueryField: TypeAlias = (
    str | float | int | Range | datetime.datetime | Iterable[str | Geometry] | Not
)
QueryDict: TypeAlias = dict[str, QueryField]

# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2026 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
import datetime
from collections.abc import Iterable
from typing import Generic, NamedTuple, Protocol, TypeAlias, TypeVar
from uuid import UUID

from odc.geo import Geometry

_T_contra = TypeVar("_T_contra", contravariant=True)


class Orderable(Protocol[_T_contra]):
    def __lt__(self, other: _T_contra) -> bool: ...
    def __gt__(self, other: _T_contra) -> bool: ...


OrderedT = TypeVar("OrderedT", bound=Orderable)


class Range(NamedTuple, Generic[OrderedT]):  # noqa: UP046
    """
    A named tuple representing a range.

    :param begin: start of the range.
    :param end: end of the range.
    """

    begin: OrderedT
    end: OrderedT


class Not[T](NamedTuple):
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


def unnest_nots(n: Not) -> Not | object:
    """
    Handle an arbitrary amount of nested Nots, such that:
    - Not(Not(x)) == x;
    - Not(Not(Not(x))) == Not(x);
    - and so on.
    """
    if isinstance(n.value, Not):
        if isinstance(n.value.value, Not):
            nested = Not(unnest_nots(n.value))
            return unnest_nots(nested)
        return n.value.value
    return n


QueryField: TypeAlias = (
    str | float | int | Range | datetime.datetime | Iterable[str | Geometry] | Not
)
QueryDict: TypeAlias = dict[str, QueryField]

# Non-strict Dataset ID representation

DSID: TypeAlias = str | UUID


def dsid_to_uuid(dsid: DSID) -> UUID:
    """
    Convert non-strict dataset ID representation to strict UUID
    """
    if isinstance(dsid, UUID):
        return dsid
    return UUID(dsid)

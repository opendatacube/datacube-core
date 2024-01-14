# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2024 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
"""
Validation of document/dictionary changes.
"""
import numpy

from itertools import zip_longest
from typing import cast, Any, Callable, List, Mapping, Sequence, Tuple, Union

# Type that can be checked for changes.
# (MyPy approximation without recursive references)
Changable = Union[str, int, None, Sequence[Any], Mapping[str, Any]]
# More accurate recursive definition:
# Changable = Union[str, int, None, Sequence["Changable"], Mapping[str, "Changable"]]


def contains(v1: Changable, v2: Changable, case_sensitive: bool = False) -> bool:
    """
    Check that v1 is a superset of v2.

    For dicts contains(v1[k], v2[k]) for all k in v2
    For other types v1 == v2
    v2 None is interpreted as {}

    """
    if not case_sensitive:
        if isinstance(v1, str):
            return isinstance(v2, str) and v1.lower() == v2.lower()

    if isinstance(v1, dict):
        return v2 is None or (isinstance(v2, dict) and
                              all(contains(v1.get(k, object()), v, case_sensitive=case_sensitive)
                                  for k, v in v2.items()))

    return v1 == v2


class MissingSentinel:
    def __str__(self):
        return "missing"

    def __repr__(self):
        return "missing"


MISSING = MissingSentinel()

# Representation of an offset in a dict structure
OffsetElem = Union[str, int]
Offset = Tuple[OffsetElem, ...]

# Representation of a changed value
ChangedValue = Union[MissingSentinel, Changable]

# Representation of a change
Change = Tuple[Offset, ChangedValue, ChangedValue]


def get_doc_changes(original: Changable,
                    new: Changable,
                    base_prefix: Offset = ()
                    ) -> List[Change]:
    """
    Return a list of `changed fields` between two dict structures.

    A `changed field` is represented by a 3-tuple made up of:

    1. `offset` to the change - a tuple of `item` accessors on the document.
    2. What is in `original` - Either a single value, a dict or list, or :data:`MISSING`.
    3. What is in `new`

    If the documents are identical, an empty list is returned.

    :type original: Union[dict, list, int]
    :rtype: list[(tuple, object, object)]


    """
    changed_fields: List[Change] = []
    if original == new:
        return changed_fields

    if isinstance(original, dict) and isinstance(new, dict):
        all_keys = set(original.keys()).union(new.keys())
        for key in all_keys:
            changed_fields.extend(get_doc_changes(original.get(key, MISSING),
                                                  new.get(key, MISSING),
                                                  base_prefix + (key,)))
    elif isinstance(original, list) and isinstance(new, list):
        for idx, (orig_item, new_item) in enumerate(zip_longest(original, new)):
            changed_fields.extend(get_doc_changes(orig_item, new_item, base_prefix + (idx, )))
    elif isinstance(original, tuple) or isinstance(new, tuple):
        if not numpy.array_equal(cast(Sequence[Any], original), cast(Sequence[Any], new)):
            changed_fields.append((base_prefix, original, new))
    else:
        changed_fields.append((base_prefix, original, new))

    return sorted(changed_fields, key=lambda a: a[0])


class DocumentMismatchError(Exception):
    pass


def check_doc_unchanged(original: Changable, new: Changable, doc_name: str) -> None:
    """
    Raise an error if any fields have been modified on a document.

    :param original: original document
    :param new: new document to compare against the original
    :param doc_name: Label used to name the document
    """
    changes = get_doc_changes(original, new)

    if changes:
        raise DocumentMismatchError(
            '{} differs from stored ({})'.format(
                doc_name,
                ', '.join(['{}: {!r}!={!r}'.format('.'.join(map(str, offset)), v1, v2) for offset, v1, v2 in changes])
            )
        )


AllowPolicy = Callable[[Offset, Offset, ChangedValue, ChangedValue], bool]


def allow_truncation(key: Offset, offset: Offset,
                     old_value: ChangedValue, new_value: ChangedValue) -> bool:
    return bool(offset) and key == offset[:-1] and new_value == MISSING


def allow_extension(key: Offset, offset: Offset,
                    old_value: ChangedValue, new_value: ChangedValue) -> bool:
    return bool(offset) and key == offset[:-1] and old_value == MISSING


def allow_addition(key: Offset, offset: Offset,
                   old_value: ChangedValue, new_value: ChangedValue) -> bool:
    return key == offset and old_value == MISSING


def allow_removal(key: Offset, offset: Offset,
                  old_value: ChangedValue, new_value: ChangedValue) -> bool:
    return key == offset and new_value == MISSING


def allow_any(key: Offset, offset: Offset,
              old: ChangedValue, new: ChangedValue) -> bool:
    return True


def classify_changes(changes: List[Change], allowed_changes: Mapping[Offset, AllowPolicy]
                     ) -> Tuple[List[Change], List[Change]]:
    """
    Classify list of changes into good(allowed) and bad(not allowed) based on allowed changes.

    :param list[(tuple,object,object)] changes: result of get_doc_changes
    :param allowed_changes: mapping from key to change policy (subset, superset, any)
    :return: good_changes, bad_chages
    """
    allowed_changes_index = dict(allowed_changes)

    good_changes: List[Change] = []
    bad_changes: List[Change] = []

    for offset, old_val, new_val in changes:
        allowance = allowed_changes_index.get(offset)
        allowance_offset = offset
        # If no allowance on this leaf, find if any parents have allowances.
        while allowance is None:
            if not allowance_offset:
                break

            allowance_offset = allowance_offset[:-1]
            allowance = allowed_changes_index.get(allowance_offset)

        if allowance is None:
            bad_changes.append((offset, old_val, new_val))
        elif hasattr(allowance, '__call__'):
            if allowance(allowance_offset, offset, old_val, new_val):
                good_changes.append((offset, old_val, new_val))
            else:
                bad_changes.append((offset, old_val, new_val))
        else:
            raise RuntimeError('Unknown change type: expecting validation function at %r' % offset)

    return good_changes, bad_changes

# -*- coding: utf-8 -*-
"""
Validation of document/dictionary changes.
"""

import logging

from datacube.utils import contains, get_doc_changes

_LOG = logging.getLogger(__name__)


def allow_subset(offset, old_value, new_value):
    valid = contains(old_value, new_value, case_sensitive=True)
    return (
        valid,
        None if valid else 'not a superset'
    )


def allow_any(offset, old, new):
    return True, None


def default_failure(offset, msg):
    raise ValueError(msg)


def validate_dict_changes(old, new, allowed_changes,
                          on_failure=default_failure,
                          on_change=lambda offset, old, new: None,
                          offset_context=()):
    """
    Validate the changes of a dictionary. Takes the old version, the new version,
    and a dictionary (mirroring their structure) of validation functions

    >>> validate_dict_changes({}, {}, {})
    ()
    >>> validate_dict_changes({'a': 1}, {'a': 1}, {})
    ()
    >>> validate_dict_changes({'a': 1}, {'a': 2}, {('a',): allow_any})
    ((('a',), 1, 2),)
    >>> validate_dict_changes({'a': 1}, {'a': 2}, {})
    Traceback (most recent call last):
    ...
    ValueError: Potentially unsafe update: changing 'a'
    >>> validate_dict_changes({'a1': 1, 'a2': {'b1': 1}}, {'a1': 1}, {})
    Traceback (most recent call last):
    ...
    ValueError: Potentially unsafe update: changing 'a2'
    >>> # A change in a nested dict
    >>> validate_dict_changes({'a1': 1, 'a2': {'b1': 1}}, {'a1': 1, 'a2': {'b1': 2}}, {('a2', 'b1'): allow_any})
    ((('a2', 'b1'), 1, 2),)
    >>> # A disallowed change in a nested dict
    >>> validate_dict_changes({'a1': 1, 'a2': {'b1': 1}}, {'a1': 1}, {('a2', 'b1'): allow_any})
    Traceback (most recent call last):
    ...
    ValueError: Potentially unsafe update: changing 'a2'
    >>> # Removal of a value
    >>> validate_dict_changes({'a1': 1, 'a2': {'b1': 1}}, {'a1': 1}, {('a2',): allow_any})
    ((('a2',), {'b1': 1}, None),)
    >>> # There's no allowance for the specific leaf change, but a parent allows all changes.
    >>> validate_dict_changes({'a1': 1, 'a2': {'b1': 1}}, {'a1': 1, 'a2': {'b1': 2}}, {('a2',): allow_any})
    ((('a2', 'b1'), 1, 2),)
    >>>

    :param dict old: Old value
    :param dict new: New value
    :param allowed_changes: Offsets that are allowed to change.
        Keys are tuples (offset in dictionary), values are functions to validate.
    :type allowed_changes: dict[tuple[str], (tuple[str], dict, dict) -> bool]
    :param tuple offset_context: Prefix to append to all key offsets
    :type on_failure: (tuple[str], dict, dict) -> None
    :type on_change: (tuple[str], dict, dict) -> None
    :rtype: tuple[(tuple, object, object)]
    """
    if old == new:
        return ()

    allowed_changes_index = dict(allowed_changes)

    changes = get_doc_changes(old, new)
    for offset, old_val, new_val in changes:
        on_change(offset_context, old_val, new_val)

        allowance = allowed_changes_index.get(offset)
        allowance_offset = offset
        # If no allowance on this leaf, find if any parents have allowances.
        while allowance is None:
            if not allowance_offset:
                break

            allowance_offset = allowance_offset[:-1]
            allowance = allowed_changes_index.get(allowance_offset)

        if allowance is None:
            on_failure(offset_context, 'Potentially unsafe update: changing %r' % ('.'.join(offset),))
        elif hasattr(allowance, '__call__'):
            is_allowed, message = allowance(offset, old_val, new_val)
            if not is_allowed:
                on_failure(offset_context, message)
        else:
            raise RuntimeError('Unknown change type: expecting validation function at %r' % offset)

    return tuple(changes)

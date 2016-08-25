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
    and a mirroring dictionary of functions to validate changes.

    >>> validate_dict_changes({}, {}, {})
    ()
    >>> validate_dict_changes({'a': 1}, {'a': 1}, {})
    ()
    >>> validate_dict_changes({'a': 1}, {'a': 2}, {'a': allow_any})
    ((('a',), 1, 2),)
    >>> validate_dict_changes({'a': 1}, {'a': 2}, {})
    Traceback (most recent call last):
    ...
    ValueError: Potentially unsafe update: changing 'a'
    >>> validate_dict_changes({'a1': 1, 'a2': {'b1': 1}}, {'a1': 1}, {})
    Traceback (most recent call last):
    ...
    ValueError: Potentially unsafe update: changing 'a2'
    >>> validate_dict_changes({'a1': 1, 'a2': {'b1': 1}}, {'a1': 1, 'a2': {'b1': 2}}, {'a2': {'b1': allow_any}})
    ((('a2', 'b1'), 1, 2),)
    >>> validate_dict_changes({'a1': 1, 'a2': {'b1': 1}}, {'a1': 1}, {'a2': {'b1': allow_any}})
    ((('a2', 'b1'), 1, None),)
    >>>

    :param dict old: Old value
    :param dict new: New value
    :param dict allowed_changes:
        Dict of offsets that are allowed to change: leaves are functions that validate the value.
    :param tuple offset_context: Prefix to append to all key offsets
    :type on_failure: (tuple[str], dict, dict) -> None
    :type on_change: (tuple[str], dict, dict) -> None
    :rtype: tuple[(tuple, object, object)]
    """
    doc_changes = get_doc_changes(old, new)
    for offset, old_value, new_value in doc_changes:
        key_name = offset[0]
        global_offset = offset_context + (key_name,)

        on_change(global_offset, old_value, new_value)

        if key_name not in allowed_changes:
            on_failure(offset, 'Potentially unsafe update: changing %r' % key_name)

        allowed_change = allowed_changes[key_name]
        if hasattr(allowed_change, '__call__'):
            is_allowed, message = allowed_change(global_offset, old_value, new_value)
            if not is_allowed:
                on_failure(offset, message)
        elif isinstance(allowed_change, dict):
            validate_dict_changes(old[key_name], new[key_name], allowed_change,
                                  on_failure=on_failure,
                                  on_change=on_change,
                                  offset_context=global_offset)
        else:
            raise RuntimeError('Unknown change type: expecting dict or valiadation function at %r' % global_offset)

    return tuple(doc_changes)

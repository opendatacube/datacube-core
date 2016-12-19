# -*- coding: utf-8 -*-
"""
Validation of document/dictionary changes.
"""
from __future__ import absolute_import, division, print_function

from datacube import compat


def contains(v1, v2, case_sensitive=False):
    """
    Check that v1 is a superset of v2.

    For dicts contains(v1[k], v2[k]) for all k in v2
    For other types v1 == v2
    v2 None is interpreted as {}

    >>> contains("bob", "BOB")
    True
    >>> contains("bob", "BOB", case_sensitive=True)
    False
    >>> contains(1, 1)
    True
    >>> contains(1, {})
    False
    >>> # same as above, but with None interpreted as {}
    >>> contains(1, None)
    False
    >>> contains({}, 1)
    False
    >>> contains(None, 1)
    False
    >>> contains({}, {})
    True
    >>> contains({}, None)
    True
    >>> # this one is arguable...
    >>> contains(None, {})
    False
    >>> contains(None, None)
    True
    >>> contains({'a':1, 'b': 2}, {'a':1})
    True
    >>> contains({'a':{'b': 'BOB'}}, {'a':{'b': 'bob'}})
    True
    >>> contains({'a':{'b': 'BOB'}}, {'a':{'b': 'bob'}}, case_sensitive=True)
    False
    >>> contains("bob", "alice")
    False
    >>> contains({'a':1}, {'a':1, 'b': 2})
    False
    >>> contains({'a': {'b': 1}}, {'a': {}})
    True
    >>> contains({'a': {'b': 1}}, {'a': None})
    True
    """
    if not case_sensitive:
        if isinstance(v1, compat.string_types):
            return isinstance(v2, compat.string_types) and v1.lower() == v2.lower()

    if isinstance(v1, dict):
        return v2 is None or (isinstance(v2, dict) and
                              all(contains(v1.get(k, object()), v, case_sensitive=case_sensitive)
                                  for k, v in v2.items()))

    return v1 == v2


class MissingSentinel(object):
    def __str__(self):
        return "missing"
    def __repr__(self):
        return "missing"
MISSING = MissingSentinel()


def get_doc_changes(original, new, base_prefix=()):
    """
    Return a list of changed fields between two dict structures.

    :type original: Union[dict, list, int]
    :rtype: list[(tuple, object, object)]


    >>> get_doc_changes(1, 1)
    []
    >>> get_doc_changes({}, {})
    []
    >>> get_doc_changes({'a': 1}, {'a': 1})
    []
    >>> get_doc_changes({'a': {'b': 1}}, {'a': {'b': 1}})
    []
    >>> get_doc_changes([1, 2, 3], [1, 2, 3])
    []
    >>> get_doc_changes([1, 2, [3, 4, 5]], [1, 2, [3, 4, 5]])
    []
    >>> get_doc_changes(1, 2)
    [((), 1, 2)]
    >>> get_doc_changes([1, 2, 3], [2, 1, 3])
    [((0,), 1, 2), ((1,), 2, 1)]
    >>> get_doc_changes([1, 2, [3, 4, 5]], [1, 2, [3, 6, 7]])
    [((2, 1), 4, 6), ((2, 2), 5, 7)]
    >>> get_doc_changes({'a': 1}, {'a': 2})
    [(('a',), 1, 2)]
    >>> get_doc_changes({'a': 1}, {'a': 2})
    [(('a',), 1, 2)]
    >>> get_doc_changes({'a': 1}, {'b': 1})
    [(('a',), 1, missing), (('b',), missing, 1)]
    >>> get_doc_changes({'a': {'b': 1}}, {'a': {'b': 2}})
    [(('a', 'b'), 1, 2)]
    >>> get_doc_changes({}, {'b': 1})
    [(('b',), missing, 1)]
    >>> get_doc_changes({'a': {'c': 1}}, {'a': {'b': 1}})
    [(('a', 'b'), missing, 1), (('a', 'c'), 1, missing)]
    >>> get_doc_changes({}, None, base_prefix=('a',))
    [(('a',), {}, None)]
    """
    changed_fields = []
    if original == new:
        return changed_fields

    if isinstance(original, dict) and isinstance(new, dict):
        all_keys = set(original.keys()).union(new.keys())
        for key in all_keys:
            changed_fields.extend(get_doc_changes(original.get(key, MISSING),
                                                  new.get(key, MISSING),
                                                  base_prefix + (key,)))
    elif isinstance(original, list) and isinstance(new, list):
        for idx, (orig_item, new_item) in enumerate(compat.zip_longest(original, new)):
            changed_fields.extend(get_doc_changes(orig_item, new_item, base_prefix + (idx, )))
    else:
        changed_fields.append((base_prefix, original, new))

    return sorted(changed_fields, key=lambda a: a[0])


def check_doc_unchanged(original, new, doc_name):
    """
    Complain if any fields have been modified on a document.

    :param original:
    :param new:
    :param doc_name:
    :return:


    >>> check_doc_unchanged({'a': 1}, {'a': 1}, 'Letters')
    >>> check_doc_unchanged({'a': 1}, {'a': 2}, 'Letters')
    Traceback (most recent call last):
    ...
    ValueError: Letters differs from stored (a: 1!=2)
    >>> check_doc_unchanged({'a': {'b': 1}}, {'a': {'b': 2}}, 'Letters')
    Traceback (most recent call last):
    ...
    ValueError: Letters differs from stored (a.b: 1!=2)
    """
    changes = get_doc_changes(original, new)

    if changes:
        raise ValueError(
            '{} differs from stored ({})'.format(
                doc_name,
                ', '.join(['{}: {!r}!={!r}'.format('.'.join(offset), v1, v2) for offset, v1, v2 in changes])
            )
        )


def allow_truncation(key, offset, old_value, new_value):
    return offset and key == offset[:-1] and new_value == MISSING


def allow_extension(key, offset, old_value, new_value):
    return offset and key == offset[:-1] and old_value == MISSING


def allow_addition(key, offset, old_value, new_value):
    return key == offset and old_value == MISSING


def allow_removal(key, offset, old_value, new_value):
    return key == offset and new_value == MISSING


def allow_any(key, offset, old, new):
    return True, None


def default_failure(offset, msg):
    raise ValueError("Change to {!r}: {}".format(".".join(offset), msg))


def classify_changes(changes, allowed_changes):
    """
    Classify list of changes into good(allowed) and bad(not allowed) based on allowed changes.

    :param list[(tuple,object,object)] changes: result of get_doc_changes
    :param allowed_changes: mapping from key to change policy (subset, superset, any)
    :return: good_changes, bad_chages


    >>> classify_changes([], {})
    ([], [])
    >>> classify_changes([(('a',), 1, 2)], {})
    ([], [(('a',), 1, 2)])
    >>> classify_changes([(('a',), 1, 2)], {('a',): allow_any})
    ([(('a',), 1, 2)], [])

    >>> changes = [(('a2',), {'b1': 1}, MISSING)]  # {'a1': 1, 'a2': {'b1': 1}} → {'a1': 1}
    >>> good_change = (changes, [])
    >>> bad_change = ([], changes)
    >>> classify_changes(changes, {}) == bad_change
    True
    >>> classify_changes(changes, {tuple(): allow_any}) == good_change
    True
    >>> classify_changes(changes, {tuple(): allow_removal}) == bad_change
    True
    >>> classify_changes(changes, {tuple(): allow_addition}) == bad_change
    True
    >>> classify_changes(changes, {tuple(): allow_truncation}) == good_change
    True
    >>> classify_changes(changes, {tuple(): allow_extension}) == bad_change
    True
    >>> classify_changes(changes, {('a1', ): allow_any}) == bad_change
    True
    >>> classify_changes(changes, {('a1', ): allow_removal}) == bad_change
    True
    >>> classify_changes(changes, {('a1', ): allow_addition}) == bad_change
    True
    >>> classify_changes(changes, {('a1', ): allow_truncation}) == bad_change
    True
    >>> classify_changes(changes, {('a1', ): allow_extension}) == bad_change
    True
    >>> classify_changes(changes, {('a2', ): allow_any}) == good_change
    True
    >>> classify_changes(changes, {('a2', ): allow_removal}) == good_change
    True
    >>> classify_changes(changes, {('a2', ): allow_addition}) == bad_change
    True
    >>> classify_changes(changes, {('a2', ): allow_truncation}) == bad_change
    True
    >>> classify_changes(changes, {('a2', ): allow_extension}) == bad_change
    True

    """
    allowed_changes_index = dict(allowed_changes)

    good_changes = []
    bad_changes = []

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


def get_failure_message(allowance, old_val, new_val):
    messages = {
        None: 'value differs ({!r} → {!r})',
        allow_addition: 'value differs ({!r} → {!r})',
        allow_removal: 'value differs ({!r} → {!r})',
        allow_truncation: 'not a subset ({!r} → {!r})',
        allow_extension: 'not a superset ({!r} → {!r})'
    }
    return messages[allowance].format(old_val, new_val)


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
    ValueError: Change to 'a': value differs (1 → 2)
    >>> validate_dict_changes({'a1': 1, 'a2': {'b1': 1}}, {'a1': 1}, {})
    Traceback (most recent call last):
    ...
    ValueError: Change to 'a2': value differs ({'b1': 1} → missing)
    >>> # A change in a nested dict
    >>> validate_dict_changes({'a1': 1, 'a2': {'b1': 1}}, {'a1': 1, 'a2': {'b1': 2}}, {('a2', 'b1'): allow_any})
    ((('a2', 'b1'), 1, 2),)
    >>> # A disallowed change in a nested dict
    >>> validate_dict_changes({'a1': 1, 'a2': {'b1': 1}}, {'a1': 1}, {('a2', 'b1'): allow_any})
    Traceback (most recent call last):
    ...
    ValueError: Change to 'a2': value differs ({'b1': 1} → missing)
    >>> # Removal of a value
    >>> validate_dict_changes({'a1': 1, 'a2': {'b1': 1}}, {'a1': 1}, {('a2',): allow_any})
    ((('a2',), {'b1': 1}, missing),)
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

    changes = get_doc_changes(old, new)
    good_changes, bad_changes = classify_changes(changes, allowed_changes)

    allowed_changes_index = dict(allowed_changes)

    for offset, old_val, new_val in good_changes:
        on_change(offset_context, old_val, new_val)

    for offset, old_val, new_val in bad_changes:
        on_change(offset_context, old_val, new_val)
        message = get_failure_message(allowed_changes_index.get(offset), old_val, new_val)
        on_failure(offset, message)

    return tuple(changes)

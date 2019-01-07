# -*- coding: utf-8 -*-
"""
Validation of document/dictionary changes.
"""
from itertools import zip_longest


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
        if isinstance(v1, str):
            return isinstance(v2, str) and v1.lower() == v2.lower()

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
    Return a list of `changed fields` between two dict structures.

    A `changed field` is represented by a 3-tuple made up of:

    1. `offset` to the change - a tuple of `item` accessors on the document.
    2. What is in `original` - Either a single value, a dict or list, or :data:`MISSING`.
    3. What is in `new`

    If the documents are identical, an empty list is returned.

    :type original: Union[dict, list, int]
    :rtype: list[(tuple, object, object)]


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
        for idx, (orig_item, new_item) in enumerate(zip_longest(original, new)):
            changed_fields.extend(get_doc_changes(orig_item, new_item, base_prefix + (idx, )))
    else:
        changed_fields.append((base_prefix, original, new))

    return sorted(changed_fields, key=lambda a: a[0])


class DocumentMismatchError(Exception):
    pass


def check_doc_unchanged(original, new, doc_name):
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

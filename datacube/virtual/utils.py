""" Utilities to facilitate virtual product implementation. """

import warnings

from datacube.model import Range


def select_unique(things):
    """ Checks that all the members of `things` are equal, and then returns it. """
    first, *rest = things
    for other in rest:
        if first != other:
            warnings.warn("select_unique may have failed: {} is not the same as {}"
                          .format(first, other))
            break

    return first


def select_keys(settings, keys):
    return {key: value
            for key, value in settings.items() if key in keys}


def reject_keys(settings, keys):
    return {key: value
            for key, value in settings.items() if key not in keys}


def merge_dicts(dicts):
    """
    Merge a list of dictionaries into one.
    Later entries override the earlier ones.
    """
    if len(dicts) == 0:
        return {}
    if len(dicts) == 1:
        return dicts[0]

    first, *rest = dicts
    result = dict(first)
    for other in rest:
        result.update(other)
    return result


def merge_search_terms(original, override, keys=None):
    def pick(key, a, b):
        if b is None:
            return a

        # trust the override
        return b

    return {key: pick(key, original.get(key), override.get(key))
            for key in list(original.keys()) + list(override.keys())
            if keys is None or key in keys}


def qualified_name(func):
    return func.__module__ + '.' + func.__qualname__

""" Utilities to facilitate virtual product implementation. """

import warnings
import math


def subset_geobox_slices(dataset_geobox, extent):
    """ Returns the slices to subset a geobox that encompasses a given extent. """
    subset_bb = (extent.to_crs(dataset_geobox.crs).boundingbox
                 .transform(~dataset_geobox.affine).buffered(1, 1))

    y_start, y_end = sorted([subset_bb.top, subset_bb.bottom])
    x_start, x_end = sorted([subset_bb.left, subset_bb.right])

    y_start = max(math.floor(y_start), 0)
    y_end = min(math.ceil(y_end), dataset_geobox.shape[0])
    x_start = max(math.floor(x_start), 0)
    x_end = min(math.ceil(x_end), dataset_geobox.shape[1])

    return (slice(y_start, y_end), slice(x_start, x_end))


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

import importlib
import logging
from contextlib import contextmanager

import toolz

_LOG = logging.getLogger(__name__)


def import_function(func_ref):
    """
    Import a function available in the python path.

    Expects at least one '.' in the `func_ref`,
    eg:
        `module.function_name`
        `package.module.function_name`

    :param func_ref:
    :return: function
    """
    module_name, _, func_name = func_ref.rpartition('.')
    module = importlib.import_module(module_name)
    return getattr(module, func_name)


@contextmanager
def ignore_exceptions_if(ignore_errors, errors=None):
    """Ignore Exceptions raised within this block if ignore_errors is True"""
    if errors is None:
        errors = (Exception,)

    if ignore_errors:
        try:
            yield
        except errors as e:
            _LOG.warning('Ignoring Exception: %s', e)
    else:
        yield


class cached_property(object):  # pylint: disable=invalid-name
    """
    A property that is only computed once per instance and then replaces
    itself with an ordinary attribute. Deleting the attribute resets the
    property.

    Source: https://github.com/bottlepy/bottle/commit/fa7733e075da0d790d809aa3d2f53071897e6f76
    """

    def __init__(self, func):
        self.__doc__ = getattr(func, '__doc__')
        self.func = func

    def __get__(self, obj, cls):
        if obj is None:
            return self
        value = obj.__dict__[self.func.__name__] = self.func(obj)
        return value


def namedtuples2dicts(namedtuples):
    """
    Convert a dict of namedtuples to a dict of dicts.

    :param namedtuples: dict of namedtuples
    :return: dict of dicts
    """
    return {k: dict(v._asdict()) for k, v in namedtuples.items()}


def sorted_items(d, key=None, reverse=False):
    """Given a dictionary `d` return items: (k1, v1), (k2, v2)... sorted in
    ascending order according to key.

    :param dict d: dictionary
    :param key: optional function remapping key
    :param bool reverse: If True return in descending order instead of default ascending

    """
    key = toolz.first if key is None else toolz.comp(key, toolz.first)
    return sorted(d.items(), key=key, reverse=reverse)


def attrs_all_equal(iterable, attr_name):
    """
    Return true if everything in the iterable has the same value for `attr_name`.

    :rtype: bool
    """
    return len({getattr(item, attr_name, float('nan')) for item in iterable}) <= 1

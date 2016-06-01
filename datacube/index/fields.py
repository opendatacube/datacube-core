# coding=utf-8
"""
Common datatypes for DB drivers.
"""
from __future__ import absolute_import
# For the search API.
from datacube.model import Range


class Field(object):
    """
    A searchable field within a dataset/storage metadata document.
    """

    def __init__(self, name, description):
        self.name = name
        self.description = description

    def __eq__(self, value):
        """
        Is this field equal to a value?
        :rtype: Expression
        """
        raise NotImplementedError('equals expression')

    def between(self, low, high):
        """
        Is this field in a range?
        :rtype: Expression
        """
        raise NotImplementedError('between expression')


class Expression(object):
    # No properties at the moment. These are built and returned by the
    # DB driver (from Field methods), so they're mostly an opaque token.

    # A simple equals implementation for comparison in test code.
    def __eq__(self, other):
        if self.__class__ != other.__class__:
            return False
        return self.__dict__ == other.__dict__


class OrExpression(Expression):
    def __init__(self, *exprs):
        super(OrExpression, self).__init__()
        self.exprs = exprs


def _to_expression(get_field, name, value):
    field = get_field(name)
    if field is None:
        raise RuntimeError('Unknown field %r' % name)

    if isinstance(value, Range):
        return field.between(value.begin, value.end)
    if isinstance(value, list):
        return OrExpression(*[_to_expression(get_field, name, val) for val in value])
    else:
        return field == value


def to_expressions(get_field, **query):
    """
    Convert a simple query (dict of param names and values) to expression objects.
    :type get_field: (str) -> Field
    :type query: dict[str,str|float|datacube.model.Range]
    :rtype: list[Expression]
    """
    return [_to_expression(get_field, name, value) for name, value in query.items()]


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


def get_doc_changes(original, new, base_prefix=()):
    """
    Return a list of changed fields between
    two dict structures.

    :type original: dict
    :rtype: list[(tuple, object, object)]


    >>> get_doc_changes({}, {})
    []
    >>> get_doc_changes({'a': 1}, {'a': 1})
    []
    >>> get_doc_changes({'a': {'b': 1}}, {'a': {'b': 1}})
    []
    >>> get_doc_changes({'a': 1}, {'a': 2})
    [(('a',), 1, 2)]
    >>> get_doc_changes({'a': 1}, {'a': 2})
    [(('a',), 1, 2)]
    >>> get_doc_changes({'a': 1}, {'b': 1})
    [(('a',), 1, None), (('b',), None, 1)]
    >>> get_doc_changes({'a': {'b': 1}}, {'a': {'b': 2}})
    [(('a', 'b'), 1, 2)]
    >>> get_doc_changes({}, {'b': 1})
    [(('b',), None, 1)]
    >>> get_doc_changes({}, None, base_prefix=('a',))
    [(('a',), {}, None)]
    """
    changed_fields = []
    if original == new:
        return changed_fields

    if not isinstance(new, dict):
        changed_fields.append((base_prefix, original, new))
        return changed_fields

    all_keys = set(original.keys()).union(new.keys())

    for key in all_keys:
        key_prefix = base_prefix + (key,)

        original_val = original.get(key)
        new_val = new.get(key)

        if original_val == new_val:
            continue

        if isinstance(original_val, dict):
            changed_fields.extend(get_doc_changes(original_val, new_val, key_prefix))
        else:
            changed_fields.append(
                (key_prefix, original_val, new_val)
            )

    return sorted(changed_fields, key=lambda a: a[0])

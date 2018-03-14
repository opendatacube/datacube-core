# coding=utf-8
"""
Common datatypes for DB drivers.
"""
from __future__ import absolute_import

from datetime import date, datetime, time

from dateutil.tz import tz

from datacube.model import Range


class UnknownFieldError(Exception):
    pass


# Allowed values for field 'type' (specified in a metadata type docuemnt)
_AVAILABLE_TYPE_NAMES = (
    'numeric-range',
    'double-range',
    'integer-range',
    'datetime-range',

    'string',
    'numeric',
    'double',
    'integer',
    'datetime',

    # For backwards compatibility (alias for numeric-range)
    'float-range',
)


class Field(object):
    """
    A searchable field within a dataset/storage metadata document.
    """
    # type of field.
    # If type is not specified, the field is a string
    # This should always be one of _AVAILABLE_TYPE_NAMES
    type_name = 'string'

    def __init__(self, name: str, description: str):
        self.name = name

        self.description = description

        # Does selecting this affect the output rows?
        # (eg. Does this join other tables that aren't 1:1 with datasets.)
        self.affects_row_selection = False

        assert self.type_name in _AVAILABLE_TYPE_NAMES, "Invalid type name %r" % (self.type_name,)

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

    def evaluate(self, ctx):
        return any(expr.evaluate(ctx) for expr in self.exprs)


def as_expression(field, value):
    """
    Convert a single field/value to expression, following the "simple" convensions.
    """
    if isinstance(value, Range):
        return field.between(value.begin, value.end)
    elif isinstance(value, list):
        return OrExpression(*(as_expression(field, val) for val in value))
    # Treat a date (day) as a time range.
    elif isinstance(value, date) and not isinstance(value, datetime):
        return as_expression(
            field,
            Range(
                datetime.combine(value, time.min.replace(tzinfo=tz.tzutc())),
                datetime.combine(value, time.max.replace(tzinfo=tz.tzutc()))
            )
        )
    return field == value


def _to_expression(get_field, name, value):
    field = get_field(name)
    if field is None:
        raise UnknownFieldError('Unknown field %r' % name)

    return as_expression(field, value)


def to_expressions(get_field, **query):
    """
    Convert a simple query (dict of param names and values) to expression objects.
    :type get_field: (str) -> Field
    :type query: dict[str,str|float|datacube.model.Range]
    :rtype: list[Expression]
    """
    return [_to_expression(get_field, name, value) for name, value in query.items()]

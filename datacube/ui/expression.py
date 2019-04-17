"""
This module defines a simple query language for searching Data Cube indexes

Literal Values
--------------

- Strings
- Numerics
- Dates
- Vague Dates

ie. Specify a year, meaning a range covering the entire year

Comparisons
-----------

- Equals

    eg.  field=<value>

- Between

    eg.

      - low < field < high
      - high > field > low
      - field in range(low, high)

API
---

This module exposes a single function :func:`parse_expression` which takes
a list of string expressions, and returns a dictionary of expressions to
pass to the index search API.

"""

import calendar
import re
from datetime import datetime
import warnings

from dateutil import tz
from pypeg2 import word, attr, List, maybe_some, parse as peg_parse

from datacube.model import Range
from datacube.api.query import _time_to_search_dims

FIELD_NAME = attr('field_name', word)

NUMBER = re.compile(r"[-+]?(\d*\.\d+|\d+\.\d*|\d+)")
# A limited string can be used without quotation marks.
LIMITED_STRING = re.compile(r"[a-zA-Z][\w._-]*")
# Inside string quotation marks. Kept simple. We're not supporting escapes or much else yet...
STRING_CONTENTS = re.compile(r"[\w\s._-]*")
# URI
URI_CONTENTS = re.compile(r"[a-z0-9+.-]+://([:/\w._-])*")
URI_CONTENTS_WITH_SPACE = re.compile(r"[a-z0-9+.-]+://([:/\s\w._-])*")

# Either a day '2016-02-20' or a month '2016-02'
DATE = re.compile(r"\d{4}-\d{1,2}(-\d{1,2})?")

# Either a whole day '2016-02-20' a whole month '2016-02' or a whole year '2014'
VAGUE_DATE = re.compile(r"\d{4}(-\d{1,2}(-\d{1,2})?)?")


class Expr(object):
    def query_repr(self, get_field):
        """
        Return this as a database expression.

        :type get_field: (str) -> datacube.index.fields.Field
        :rtype: datacube.index.fields.Expression
        """
        raise NotImplementedError('to_expr')


class StringValue(Expr):
    def __init__(self, value=None):
        self.value = value

    grammar = [
        attr('value', URI_CONTENTS),
        attr('value', LIMITED_STRING),
        ('"', attr('value', URI_CONTENTS_WITH_SPACE), '"'),
        ('"', attr('value', STRING_CONTENTS), '"'),
    ]

    def __str__(self):
        return self.value

    def __repr__(self):
        return repr(self.value)

    def query_repr(self, get_field):
        return self.value

    def as_value(self):
        return self.value


class NumericValue(Expr):
    def __init__(self, value=None):
        self.value = value

    grammar = attr('value', NUMBER)

    def __str__(self):
        return self.value

    def __repr__(self):
        return self.value

    def query_repr(self, get_field):
        return float(self.value)

    def as_value(self):
        return float(self.value)


class DateValue(Expr):
    def __init__(self, value=None):
        self.value = value

    grammar = attr('value', DATE)

    def __str__(self):
        return self.value

    def __repr__(self):
        return self.value

    def query_repr(self, get_field):
        return self.as_value()

    def as_value(self):
        """
        >>> DateValue(value='2017-03-03').as_value()
        datetime.datetime(2017, 3, 3, 0, 0, tzinfo=tzutc())
        >>> # A missing day implies the first.
        >>> DateValue(value='2017-03').as_value()
        datetime.datetime(2017, 3, 1, 0, 0, tzinfo=tzutc())
        """
        parts = self.value.split('-')
        parts.reverse()

        year = int(parts.pop())
        month = int(parts.pop())
        day = int(parts.pop()) if parts else 1
        return datetime(year, month, day, tzinfo=tz.tzutc())


def last_day_of_month(year, month):
    first_weekday, last_day = calendar.monthrange(year, month)
    return last_day


class VagueDateValue(Expr):
    def __init__(self, value=None):
        self.value = value

    grammar = attr('value', VAGUE_DATE)

    def __str__(self):
        return self.value

    def __repr__(self):
        return self.value

    def query_repr(self, get_field):
        return self.as_value()

    def as_value(self):
        """
        >>> VagueDateValue(value='2017-03-03').as_value()
        Range(begin=datetime.datetime(2017, 3, 3, 0, 0, tzinfo=tzutc()), \
end=datetime.datetime(2017, 3, 3, 23, 59, 59, tzinfo=tzutc()))
        >>> VagueDateValue(value='2017-03').as_value()
        Range(begin=datetime.datetime(2017, 3, 1, 0, 0, tzinfo=tzutc()), \
end=datetime.datetime(2017, 3, 31, 23, 59, 59, tzinfo=tzutc()))
        >>> VagueDateValue(value='2017').as_value()
        Range(begin=datetime.datetime(2017, 1, 1, 0, 0, tzinfo=tzutc()), \
end=datetime.datetime(2017, 12, 31, 23, 59, 59, tzinfo=tzutc()))
        """
        parts = self.value.split('-')
        parts.reverse()

        year = int(parts.pop())
        month = int(parts.pop()) if parts else None
        day = int(parts.pop()) if parts else None

        if parts:
            raise RuntimeError("More than three components in date expression? %r" % self.value)

        month_range = (month, month) if month else (1, 12)
        day_range = (day, day) if day else (1, last_day_of_month(year, month_range[1]))

        return Range(
            datetime(year, month_range[0], day_range[0], 0, 0, tzinfo=tz.tzutc()),
            datetime(year, month_range[1], day_range[1], 23, 59, 59, tzinfo=tz.tzutc())
        )


class InExpression(Expr):
    def __init__(self, field_name=None, value=None, year=None):
        self.field_name = field_name
        self.value = value
        self.year = year

    grammar = [
        (FIELD_NAME, 'in', attr('value', [VagueDateValue]))
    ]

    def __str__(self):
        return '{} = {!r}'.format(self.field_name, self.value)

    def query_repr(self, get_field):
        return get_field(self.field_name) == self.value.query_repr(get_field)

    def as_query(self):
        return {self.field_name: self.value.as_value()}


class EqualsExpression(Expr):
    def __init__(self, field_name=None, value=None):
        self.field_name = field_name
        self.value = value

    grammar = FIELD_NAME, '=', attr('value', [DateValue, NumericValue, StringValue])

    def __str__(self):
        return '{} = {!r}'.format(self.field_name, self.value)

    def query_repr(self, get_field):
        return get_field(self.field_name) == self.value.query_repr(get_field)

    def as_query(self):
        return {self.field_name: self.value.as_value()}


class OldBetweenExpression(Expr):
    def __init__(self, field_name=None, low_value=None, high_value=None):
        self.field_name = field_name
        self.low_value = low_value
        self.high_value = high_value

    range_values = [DateValue, NumericValue]
    grammar = [
        # low < field < high
        (attr('low_value', range_values), '<', FIELD_NAME, '<', attr('high_value', range_values)),
        # high > field > low
        (attr('high_value', range_values), '>', FIELD_NAME, '>', attr('low_value', range_values)),
        # field in range(low, high)
        (FIELD_NAME, 'in', 'range',
         '(',
         attr('low_value', range_values), ',', attr('high_value', range_values),
         ')'),
    ]

    def __str__(self):
        return '{!r} < {} < {!r}'.format(self.low_value, self.field_name, self.high_value)

    def query_repr(self, get_field):
        return get_field(self.field_name).between(
            self.low_value.query_repr(get_field),
            self.high_value.query_repr(get_field)
        )

    def as_query(self):
        warnings.warn("old-style between expressions are deprecated, use 'field in [start, end]' syntax instead",
                      DeprecationWarning)
        return {self.field_name: Range(self.low_value.as_value(), self.high_value.as_value())}


class BetweenExpression(Expr):
    def __init__(self, field_name=None, low_value=None, high_value=None):
        self.field_name = field_name
        self.low_value = low_value
        self.high_value = high_value

    range_values = [DateValue, NumericValue]
    grammar = [
        # field in [low, high]
        (FIELD_NAME, 'in',
         '[',
         attr('low_value', range_values), ',', attr('high_value', range_values),
         ']'),
    ]

    def __str__(self):
        return '{} in [{!r}, {!r}]'.format(self.field_name, self.low_value, self.high_value)

    def query_repr(self, get_field):
        search_range = self.as_query()[self.field_name]
        return get_field(self.field_name).between(search_range.begin, search_range.end)

    def as_query(self):
        return {self.field_name: _time_to_search_dims([self.low_value.value, self.high_value.value])}


class ExpressionList(List):
    grammar = maybe_some([EqualsExpression, BetweenExpression, OldBetweenExpression, InExpression])

    def __str__(self):
        return ' and '.join(map(str, self))


def _parse_raw_expressions(*expression_text):
    """
    :rtype: ExpressionList
    :type expression_text: str
    """
    return peg_parse(' '.join(expression_text), ExpressionList)


def parse_expressions(*expression_text):
    """
    Parse an expression string into a dictionary suitable for .search() methods.

    :type expression_text: str
    :rtype: dict[str, object]
    """
    raw_expr = _parse_raw_expressions(' '.join(expression_text))
    out = {}
    for expr in raw_expr:
        out.update(expr.as_query())

    return out

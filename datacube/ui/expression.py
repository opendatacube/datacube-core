#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.

"""
Data Access Module
"""
from __future__ import absolute_import, print_function, division

import calendar
import re
from datetime import datetime

from dateutil import tz
from pypeg2 import word, attr, List, maybe_some, parse as peg_parse

from datacube.model import Range

FIELD_NAME = attr(u'field_name', word)

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
        attr(u'value', URI_CONTENTS),
        attr(u'value', LIMITED_STRING),
        (u'"', attr(u'value', URI_CONTENTS_WITH_SPACE), u'"'),
        (u'"', attr(u'value', STRING_CONTENTS), u'"'),
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

    grammar = attr(u'value', NUMBER)

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

    grammar = attr(u'value', DATE)

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

    grammar = attr(u'value', VAGUE_DATE)

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
        (FIELD_NAME, u'in', attr(u'value', [VagueDateValue]))
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

    grammar = FIELD_NAME, u'=', attr(u'value', [DateValue, NumericValue, StringValue])

    def __str__(self):
        return '{} = {!r}'.format(self.field_name, self.value)

    def query_repr(self, get_field):
        return get_field(self.field_name) == self.value.query_repr(get_field)

    def as_query(self):
        return {self.field_name: self.value.as_value()}


class BetweenExpression(Expr):
    def __init__(self, field_name=None, low_value=None, high_value=None):
        self.field_name = field_name
        self.low_value = low_value
        self.high_value = high_value

    range_values = [DateValue, NumericValue]
    grammar = [
        # low < field < high
        (attr(u'low_value', range_values), u'<', FIELD_NAME, u'<', attr(u'high_value', range_values)),
        # high > field > low
        (attr(u'high_value', range_values), u'>', FIELD_NAME, u'>', attr(u'low_value', range_values)),
        # field in range(low, high)
        (FIELD_NAME, u'in', u'range',
         u'(',
         attr(u'low_value', range_values), u',', attr(u'high_value', range_values),
         u')'),
    ]

    def __str__(self):
        return '{!r} < {} < {!r}'.format(self.low_value, self.field_name, self.high_value)

    def query_repr(self, get_field):
        return get_field(self.field_name).between(
            self.low_value.query_repr(get_field),
            self.high_value.query_repr(get_field)
        )

    def as_query(self):
        return {self.field_name: Range(self.low_value.as_value(), self.high_value.as_value())}


class ExpressionList(List):
    grammar = maybe_some([EqualsExpression, BetweenExpression, InExpression])

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

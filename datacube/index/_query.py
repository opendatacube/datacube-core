# coding=utf-8
"""
Query datasets and storage units.
"""
from __future__ import absolute_import
from __future__ import unicode_literals, print_function

import re

from pypeg2 import word, attr, List, some

FIELD_NAME = attr('field_name', word)

NUMBER = re.compile(r"\d+")
# A limited string can be used without quotation marks.
LIMITED_STRING = re.compile(r"[a-zA-Z][\w\._-]*")
# Inside string quotation marks. Kept simple. We're not supporting escapes or much else yet...
STRING_CONTENTS = re.compile(r"[\w\s\._-]*")


class Expr(object):
    def query_repr(self, field):
        raise NotImplementedError('to_expr')


class StringValue(Expr):
    def __init__(self):
        self.value = None

    grammar = [
        attr('value', LIMITED_STRING),
        ('"', attr('value', STRING_CONTENTS), '"')
    ]

    def __str__(self):
        return self.value

    def __repr__(self):
        return repr(self.value)

    def query_repr(self, field):
        return self.value


class NumericValue(Expr):
    def __init__(self):
        self.value = None

    grammar = attr('value', NUMBER)

    def __str__(self):
        return self.value

    def __repr__(self):
        return self.value

    def query_repr(self, field):
        return float(self.value)


class EqualsExpression(Expr):
    def __init__(self):
        self.field_name = None
        self.value = None

    grammar = FIELD_NAME, '=', attr('value', [NumericValue, StringValue])

    def __str__(self):
        return '{} = {!r}'.format(self.field_name, self.value)

    def query_repr(self, field):
        return field(self.field_name) == self.value.query_repr(field)


class BetweenExpression(Expr):
    def __init__(self):
        self.field_name = None
        self.low_val = None
        self.high_val = None

    grammar = [
        (attr('low_val', NumericValue), '<', FIELD_NAME, '<', attr('high_val', NumericValue)),
        (attr('high_val', NumericValue), '>', FIELD_NAME, '>', attr('low_val', NumericValue))
    ]

    def __str__(self):
        return '{!r} < {} < {!r}'.format(self.low_val, self.field_name, self.high_val)

    def query_repr(self, field):
        return field(self.field_name).between(
            self.low_val.query_repr(),
            self.high_val.query_repr()
        )


class ExpressionList(List):
    grammar = some([EqualsExpression, BetweenExpression])

    def __str__(self):
        return ' and '.join(map(str, self))


class DataQuery(object):
    def __init__(self, db):
        """
        :type db: datacube.index._core_db.Db
        """
        self.db = db

    def search_datasets(self, *expressions):
        """
        :type expressions: list[Expression]
        """

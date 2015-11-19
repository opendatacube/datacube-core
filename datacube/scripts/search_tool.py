# coding=utf-8
"""
Query datasets and storage units.
"""
from __future__ import absolute_import
from __future__ import unicode_literals, print_function

import re

import click
from pypeg2 import word, attr, List, some, parse as peg_parse

from datacube import config, index

FIELD_NAME = attr('field_name', word)

NUMBER = re.compile(r"\d+")
# A limited string can be used without quotation marks.
LIMITED_STRING = re.compile(r"[a-zA-Z][\w\._-]*")
# Inside string quotation marks. Kept simple. We're not supporting escapes or much else yet...
STRING_CONTENTS = re.compile(r"[\w\s\._-]*")


class Expr(object):
    def query_repr(self, get_field):
        """
        Return this as a database expression.

        :type get_field: (str) -> datacube.index.db._fields.Field
        :rtype: datacube.index.db._fields.Expression
        """
        raise NotImplementedError('to_expr')


class StringValue(Expr):
    def __init__(self, value=None):
        self.value = value

    grammar = [
        attr('value', LIMITED_STRING),
        ('"', attr('value', STRING_CONTENTS), '"')
    ]

    def __str__(self):
        return self.value

    def __repr__(self):
        return repr(self.value)

    def query_repr(self, get_field):
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


class EqualsExpression(Expr):
    def __init__(self, field_name=None, value=None):
        self.field_name = field_name
        self.value = value

    grammar = FIELD_NAME, '=', attr('value', [NumericValue, StringValue])

    def __str__(self):
        return '{} = {!r}'.format(self.field_name, self.value)

    def query_repr(self, get_field):
        return get_field(self.field_name) == self.value.query_repr(get_field)


class BetweenExpression(Expr):
    def __init__(self, field_name=None, low_value=None, high_value=None):
        self.field_name = field_name
        self.low_value = low_value
        self.high_value = high_value

    grammar = [
        (attr('low_value', NumericValue), '<', FIELD_NAME, '<', attr('high_value', NumericValue)),
        (attr('high_value', NumericValue), '>', FIELD_NAME, '>', attr('low_value', NumericValue))
    ]

    def __str__(self):
        return '{!r} < {} < {!r}'.format(self.low_value, self.field_name, self.high_values)

    def query_repr(self, get_field):
        return get_field(self.field_name).between(
            self.low_value.query_repr(get_field),
            self.high_value.query_repr(get_field)
        )


class ExpressionList(List):
    grammar = some([EqualsExpression, BetweenExpression])

    def __str__(self):
        return ' and '.join(map(str, self))


def _parse_raw_expressions(*expression_text):
    """
    :rtype: Expr
    :type expression_text: str
    """
    return peg_parse(' '.join(expression_text), ExpressionList)


class UnknownFieldException(Exception):
    pass


def parse_expressions(get_field, *expression_text):
    """
    :type expression_text: list[str]
    :type get_field: (str) -> datacube.index.db._fields.Field
    :rtype: list[datacube.index.db._fields.Expression]
    """

    def _get_field(name):
        field = get_field(name)
        if field is None:
            raise UnknownFieldException('Unknown field %r' % name)
        return field

    raw_expr = _parse_raw_expressions(' '.join(expression_text))
    return [expr.query_repr(_get_field) for expr in raw_expr]


CLICK_SETTINGS = dict(help_option_names=['-h', '--help'])


@click.group(help="Search the Data Cube", context_settings=CLICK_SETTINGS)
@click.option('--verbose', '-v', count=True, help="Use multiple times for more verbosity")
@click.option('--log-queries', is_flag=True, help="Print database queries.")
def cli(verbose, log_queries):
    config.init_logging(verbosity_level=verbose, log_queries=log_queries)


@cli.command(help='Datasets')
@click.argument('expression',
                nargs=-1)
def dataset(expression):
    i = index.data_index_connect()

    for d in i.search_datasets(*parse_expressions(i.get_dataset_field, *expression)):
        print(repr(d))


if __name__ == '__main__':
    cli()

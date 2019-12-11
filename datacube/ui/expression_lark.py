"""

"""
# flake8: noqa

import logging
from datetime import datetime
from dateutil.tz import tzutc
from lark import Lark, v_args, Transformer

from datacube.model import Range

logging.basicConfig()

search_grammar = """
    start: expression+
    ?expression: equals_expr
               | in_expr
               | between_expr
    
    equals_expr: field "=" value
    in_expr: field "in" date
    between_expr: field "in" "[" orderable "," orderable "]"
    
    ?field: /[a-zA-Z][\w\d_]*/
    
    ?orderable: date
              | SIGNED_NUMBER -> number
    
    value: date
         | INT -> integer
         | SIGNED_NUMBER -> number
         | ESCAPED_STRING -> string
         | simple_string
         | /[a-z0-9+.-]+:\/\/([:\/\w._-])*/
    
    simple_string: /[a-zA-Z][\w._-]*/
    
    date: YEAR ["-" MONTH ["-" DAY ]]

    YEAR: DIGIT ~ 4
    MONTH: DIGIT ~ 1..2
    DAY: DIGIT ~ 1..2


    %import common.ESCAPED_STRING
    %import common.SIGNED_NUMBER
    %import common.INT
    %import common.DIGIT
    %import common.CNAME
    %import common.WS
    %ignore WS
"""


@v_args(inline=True)
class TreeToSearchExprs(Transformer):
    # Convert the expressions
    def in_expr(self, k, v):
        return {str(k): v}
    equals_expr = in_expr

    def range_expr(self, field, lower, upper):
        return {str(field): Range(lower, upper)}

    between_expr = range_expr

    # Convert the literals
    def string(self, val):
        return str(val[1:-1])

    simple_string = str
    field = str
    value = str
    number = float
    integer = int

    def date(self, y, m=None, d=None):
        m = 1 if m is None else int(m)
        d = 1 if d is None else int(d)
        return datetime(year=int(y), month=m, day=d, tzinfo=tzutc())

    # Merge everything into a single dict
    def start(self, *vals):
        result = {}
        for val in vals:
            result.update(val)
        return result


def parse_expressions(*expression_text):
    expr_parser = Lark(search_grammar)
    tree = expr_parser.parse(' '.join(expression_text))
    return TreeToSearchExprs().transform(tree)


def main():
    expr_parser = Lark(search_grammar)  # , parser='lalr')

    sample_inputs = """platform = "LANDSAT_8"
    platform = "LAND SAT_8"
    platform = 4
    lat in [4, 6]
    time in [2014, 2014]
    time in [2014-03-01, 2014-04-01]
    time = 2014-03-02
    time = 2014-3-2
    time = 2014-3
    time = 2014
    platform = LANDSAT_8
    lat in [4, 6] time = 2014-03-02
    platform=LS8 lat in [-14, -23.5] instrument="OTHER"
    """.strip().split('\n')

    for sample in sample_inputs:
        transformer = TreeToSearchExprs()
        tree = expr_parser.parse(sample)

        print(sample)
        print(tree)
        print(transformer.transform(tree))
        print()


if __name__ == '__main__':
    main()

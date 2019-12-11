"""


Examples:

    platform = "LANDSAT_8"
    platform = "LAND SAT_8"
    platform = 4
    4<lat<6
    6 > lat > 4
    lat in range(4, 6)
    time = 2014-03-02
    time = 2014-3-2
    time in 2014-03-08

"""

# flake8: noqa

from collections import namedtuple

import logging
from datetime import datetime
from dateutil.tz import tzutc

logging.basicConfig()

from lark import Lark, v_args, Transformer

Range = namedtuple('Range', ('begin', 'end'))

search_grammar = """

    start: expression+
    ?expression: equals_expr
               | in_expr
               | range_expr
               | lt_expr
               | gt_expr
    
    equals_expr: FIELD "=" value
    in_expr: FIELD "in" date
           | FIELD "in" "[" orderable "," orderable "]" -> range_expr
    range_expr: FIELD "in" "range(" orderable "," orderable ")"
    lt_expr: orderable "<" FIELD "<" orderable
    gt_expr: orderable ">" FIELD ">" orderable
    
    FIELD: /[a-zA-Z][\w\d_]*/
    
    ?orderable: SIGNED_NUMBER | date

    
    value: date
         | SIGNED_NUMBER -> number
         | ESCAPED_STRING -> string
         | simple_string
    
    
    simple_string: /[a-zA-Z][\w._-]*/
    
    date: INT ["-" INT ["-" INT]]
        
        
    
    %import common.ESCAPED_STRING
    %import common.SIGNED_NUMBER
    %import common.INT
    %import common.CNAME
    %import common.WS
    %ignore WS
"""


@v_args(inline=True)
class TreeToSearchExprs(Transformer):
    # Convert the of expressions
    def in_expr(self, k, v):
        return {str(k): str(v)}

    def lt_expr(self, lower, field, upper):
        return {str(field): Range(lower, upper)}

    def gt_expr(self, upper, field, lower):
        return {str(field): Range(lower, upper)}

    def range_expr(self, field, lower, upper):
        return {str(field): Range(lower, upper)}

    equals_expr = in_expr

    # Convert the literals
    def string(self, val):
        return str(val[1:-1])

    simple_string = str
    number = float

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
    4 < lat < 6
    -4 < lat < 6
    6 > lat > -4
    6 > lat > 4
    -6 > lat > -4
    lat in range(4, 6)
    time = 2014-03-02
    time = 2014-3-2
    time = 2014-3
    time = 2014
    platform = LANDSAT_8
    lat in range(4, 6) time = 2014-03-02
    platform=LS8 -4<lat<23.5 instrument="OTHER"
    """.strip().split('\n')
    # time in 2014-03-08

    for sample in sample_inputs:
        transformer = TreeToSearchExprs()
        tree = expr_parser.parse(sample)

        print(sample)
        print(tree)
        print(transformer.transform(tree))
        print()


if __name__ == '__main__':
    main()

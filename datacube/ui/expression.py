"""
Search expression parsing for command line applications.

Three types of expressions are available:

    FIELD = VALUE
    FIELD in DATE-RANGE
    FIELD in [START, END]

Where DATE-RANGE is one of YYYY, YYYY-MM or YYYY-MM-DD
and START, END are either numbers or dates.
"""
# flake8: noqa

from lark import Lark, v_args, Transformer

from datacube.api.query import _time_to_search_dims
from datacube.model import Range


search_grammar = r"""
    start: expression*
    ?expression: equals_expr
               | time_in_expr
               | field_in_expr

    equals_expr: field "=" value
    time_in_expr: time "in" date_range
    field_in_expr: field "in" "[" orderable "," orderable "]"

    field: FIELD
    time: TIME

    ?orderable: INT -> integer
              | SIGNED_NUMBER -> number

    ?value: INT -> integer
          | SIGNED_NUMBER -> number
          | ESCAPED_STRING -> string
          | SIMPLE_STRING -> simple_string
          | URL_STRING -> url_string


    ?date_range: date -> single_date
               | "[" date "," date "]" -> date_pair

    date: YEAR ["-" MONTH ["-" DAY ]]

    TIME: "time"
    FIELD: /[a-zA-Z][\w\d_]*/
    YEAR: DIGIT ~ 4
    MONTH: DIGIT ~ 1..2
    DAY: DIGIT ~ 1..2
    SIMPLE_STRING: /[a-zA-Z][\w._-]*/
    URL_STRING: /[a-z0-9+.-]+:\/\/([:\/\w._-])*/


    %import common.ESCAPED_STRING
    %import common.SIGNED_NUMBER
    %import common.INT
    %import common.DIGIT
    %import common.CNAME
    %import common.WS
    %ignore WS
"""


def identity(x):
    return x


@v_args(inline=True)
class TreeToSearchExprs(Transformer):
    # Convert the expressions
    def equals_expr(self, field, value):
        return {str(field): value}

    def field_in_expr(self, field, lower, upper):
        return {str(field): Range(lower, upper)}

    def time_in_expr(self, time_field, date_range):
        return {str(time_field): date_range}

    # Convert the literals
    def string(self, val):
        return str(val[1:-1])

    simple_string = url_string = field = time = str
    number = float
    integer = int
    value = identity

    def single_date(self, date):
        return _time_to_search_dims(date)

    def date_pair(self, start, end):
        return _time_to_search_dims((start, end))

    def date(self, y, m=None, d=None):
        return "-".join(x for x in [y, m, d] if x is not None)

    # Merge everything into a single dict
    def start(self, *search_exprs):
        combined = {}
        for expr in search_exprs:
            combined.update(expr)
        return combined


def parse_expressions(*expression_text):
    expr_parser = Lark(search_grammar)
    tree = expr_parser.parse(' '.join(expression_text))
    return TreeToSearchExprs().transform(tree)


def main():
    expr_parser = Lark(search_grammar)

    sample_inputs = """platform = "LANDSAT_8"
    platform = "LAND SAT_8"
    platform = 4
    lat in [4, 6]
    time in [2014, 2014]
    time in [2014-03-01, 2014-04-01]
    time in 2014-03-02
    time in 2014-3-2
    time in 2014-3
    time in 2014
    platform = LANDSAT_8
    lat in [4, 6] time in 2014-03-02
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

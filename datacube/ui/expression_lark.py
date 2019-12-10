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

from lark import Lark, Transformer, v_args

from collections import namedtuple

Range = namedtuple('Range', ('begin', 'end'))

search_grammar = """

    ?start: expression+
    expression: equals_expr
              | in_expr
              | range_expr
              | lt_gt_expr
    
    equals_expr: FIELD "=" value
    
    range_expr: FIELD range
    in_expr: FIELD "in" DATE
    range: "in range(" orderable "," orderable ")"
    
    lt_gt_expr: orderable OP FIELD OP orderable
    
    OP: "<" | ">"
    
    ?orderable: NUMBER | DATE

    ?value: DATE | NUMBER | ESCAPED_STRING | SIMPLE_STRING
    
    FIELD: CNAME
    
    SIMPLE_STRING: /[a-zA-Z][\w._-]*/
    
    DATE: DIGIT ~ 4 "-" DIGIT ~ 1..2 "-" DIGIT ~ 1..2 
        | DIGIT ~ 4 "-" DIGIT ~ 1..2 
        | DIGIT ~ 4
        
    
    %import common.ESCAPED_STRING
    %import common.SIGNED_NUMBER -> NUMBER
    %import common.DIGIT
    %import common.CNAME
    %import common.WS_INLINE
    %ignore WS_INLINE
"""

class TreeToJson(Transformer):
    # @v_args(inline=True)
    # def string(self, s):
    #     return s[1:-1].replace('\\"', '"')

    range = Range
    number = v_args(inline=True)(float)

    null = lambda self, _: None
    true = lambda self, _: True
    false = lambda self, _: False

expr_parser = Lark(search_grammar)#, parser='lalr')

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
time in 2014-03-08
platform = LANDSAT_8
lat in range(4, 6) time = 2014-03-02
""".strip().split('\n')

for sample in sample_inputs:
    print(sample)
    print(expr_parser.parse(sample))
    print(expr_parser.parse(sample).pretty())

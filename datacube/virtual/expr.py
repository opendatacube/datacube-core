import lark
import numpy

from datacube.utils.masking import valid_data_mask


def formula_parser():
    return lark.Lark("""
                ?expr: num_expr | bool_expr

                ?bool_expr: or_clause | comparison_clause

                ?or_clause: or_clause "|" and_clause -> or_
                          | or_clause "^" and_clause -> xor
                          | and_clause
                ?and_clause: and_clause "&" term -> and_
                           | term
                ?term: "not" term -> not_
                     | "(" bool_expr ")"

                ?comparison_clause: eq | ne | le | ge | lt | gt

                eq: num_expr "==" num_expr
                ne: num_expr "!=" num_expr
                le: num_expr "<=" num_expr
                ge: num_expr ">=" num_expr
                lt: num_expr "<" num_expr
                gt: num_expr ">" num_expr


                ?num_expr: shift

                ?shift: shift "<<" sum -> lshift
                      | shift ">>" sum -> rshift
                      | sum

                ?sum: sum "+" product -> add
                    | sum "-" product -> sub
                    | product

                ?product: product "*" atom -> mul
                        | product "/" atom -> truediv
                        | product "//" atom -> floordiv
                        | product "%" atom -> mod
                        | atom

                ?atom: "-" subatom -> neg
                     | "+" subatom -> pos
                     | "~" subatom -> inv
                     | subatom "**" atom -> pow
                     | subatom

                ?subatom: NAME -> var_name
                        | FLOAT -> float_literal
                        | INT -> int_literal
                        | "(" num_expr ")"


                %import common.FLOAT
                %import common.INT
                %import common.WS_INLINE
                %import common.CNAME -> NAME

                %ignore WS_INLINE
                """, start='expr')


@lark.v_args(inline=True)
class FormulaEvaluator(lark.Transformer):
    from operator import not_, or_, and_, xor              # type: ignore[misc]
    from operator import eq, ne, le, ge, lt, gt            # type: ignore[misc]
    from operator import add, sub, mul, truediv, floordiv  # type: ignore[misc]
    from operator import neg, pos, inv                     # type: ignore[misc]
    from operator import mod, pow, lshift, rshift          # type: ignore[misc]

    float_literal = float
    int_literal = int


@lark.v_args(inline=True)
class MaskEvaluator(lark.Transformer):
    # the result of an expression is nodata whenever any of its subexpressions is nodata
    from operator import or_     # type: ignore[misc]

    # pylint: disable=invalid-name
    and_ = _xor = or_
    eq = ne = le = ge = lt = gt = or_
    add = sub = mul = truediv = floordiv = mod = pow = lshift = rshift = or_

    def not_(self, value):
        return value

    neg = pos = inv = not_

    @staticmethod
    def float_literal(value):
        return False

    @staticmethod
    def int_literal(value):
        return False


def evaluate_type(formula, env, parser, evaluator):
    """
    Evaluates the type of the output of a formula given a parser,
    a corresponding evaluator class, and an environment.
    The environment is a dict-like object (such as an `xarray.Dataset`) that maps variable names to values.
    """
    @lark.v_args(inline=True)
    class TypeEvaluator(evaluator):
        def var_name(self, key):
            return numpy.array([], dtype=env[key.value].dtype)

    return TypeEvaluator().transform(parser.parse(formula))


def evaluate_data(formula, env, parser, evaluator):
    """
    Evaluates a formula given a parser, a corresponding evaluator class, and an environment.
    The environment is a dict-like object (such as an `xarray.Dataset`) that maps variable names to values.
    """
    @lark.v_args(inline=True)
    class DataEvaluator(evaluator):
        def var_name(self, key):
            return env[key.value]

    return DataEvaluator().transform(parser.parse(formula))


def evaluate_nodata_mask(formula, env, parser, evaluator):
    """
    Evaluates the nodata mask for a formula given a parser, a corresponding evaluator class, and an environment.
    The environment is a dict-like object (such as an `xarray.Dataset`) that maps variable names to values.
    """
    @lark.v_args(inline=True)
    class NodataMaskEvaluator(evaluator):
        def var_name(self, key):
            # pylint: disable=invalid-unary-operand-type
            return ~valid_data_mask(env[key.value])

    return NodataMaskEvaluator().transform(parser.parse(formula))

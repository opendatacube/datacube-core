# ------------------------------------------------------------------------------
# Name:       ndexpr.py
# Purpose:    ndarray Math Expression evaluator
#
# Author:     Peter Wang
#
# Created:    7 October 2015
# Copyright:  2015 Commonwealth Scientific and Industrial Research Organisation
#             (CSIRO)
#             Code based on PyParsing fourFn example by Paul McGuire
#             Used with his with permission
#             (http://pyparsing.wikispaces.com/file/view/fourFn.py)
#             Adapted get_pqa_mask function from stacker.py by Josh Sixsmith &
#             Alex IP of Geoscience Australia
#             https://github.com/GeoscienceAustralia/agdc/blob/master/src/stacker.py
# License:    This software is open source under the Apache v2.0 License
#             as provided in the accompanying LICENSE file or available from
#             https://github.com/data-cube/agdc-v2/blob/master/LICENSE
#             By continuing, you acknowledge that you have read and you accept
#             and will abide by the terms of the License.
#
# Updates:
# 7/10/2015:  Initial Version.
#
# ------------------------------------------------------------------------------

# pylint: disable=too-many-statements, too-many-branches, expression-not-assigned, too-many-locals,
# pylint: disable=too-many-return-statements, protected-access, undefined-variable, too-many-public-methods

from __future__ import absolute_import
from __future__ import print_function
import math
import operator
import inspect
import sys
import ctypes
import numpy as np
import xarray as xr
from xarray import ufuncs
from scipy import ndimage
from pprint import pprint
import matplotlib.pyplot as plt

from pyparsing import Literal, CaselessLiteral, Word, Combine, Group,\
    Optional, ZeroOrMore, Forward, nums, alphas, delimitedList,\
    ParserElement, FollowedBy

ParserElement.enablePackrat()


class NDexpr(object):

    def __init__(self):

        self.ae = False
        self.local_dict = None
        self.f = None

        self.expr_stack = []
        self.texpr_stack = []

        # Define constants
        self.constants = {}

        # Define Operators
        self.opn = {"+": operator.add,
                    "-": operator.sub,
                    "*": operator.mul,
                    "/": operator.truediv,
                    ">": operator.gt,
                    ">=": operator.ge,
                    "<": operator.lt,
                    "<=": operator.le,
                    "==": operator.eq,
                    "!=": operator.ne,
                    "|": operator.or_,
                    "&": operator.and_,
                    "!": operator.inv}

        # Define xarray DataArray operators with 1 input parameter
        self.xfn1 = {"angle": xr.ufuncs.angle,
                     "arccos": xr.ufuncs.arccos,
                     "arccosh": xr.ufuncs.arccosh,
                     "arcsin": xr.ufuncs.arcsin,
                     "arcsinh": xr.ufuncs.arcsinh,
                     "arctan": xr.ufuncs.arctan,
                     "arctanh": xr.ufuncs.arctanh,
                     "ceil": xr.ufuncs.ceil,
                     "conj": xr.ufuncs.conj,
                     "cos": xr.ufuncs.cos,
                     "cosh": xr.ufuncs.cosh,
                     "deg2rad": xr.ufuncs.deg2rad,
                     "degrees": xr.ufuncs.degrees,
                     "exp": xr.ufuncs.exp,
                     "expm1": xr.ufuncs.expm1,
                     "fabs": xr.ufuncs.fabs,
                     "fix": xr.ufuncs.fix,
                     "floor": xr.ufuncs.floor,
                     "frexp": xr.ufuncs.frexp,
                     "imag": xr.ufuncs.imag,
                     "iscomplex": xr.ufuncs.iscomplex,
                     "isfinite": xr.ufuncs.isfinite,
                     "isinf": xr.ufuncs.isinf,
                     "isnan": xr.ufuncs.isnan,
                     "isreal": xr.ufuncs.isreal,
                     "log": xr.ufuncs.log,
                     "log10": xr.ufuncs.log10,
                     "log1p": xr.ufuncs.log1p,
                     "log2": xr.ufuncs.log2,
                     "rad2deg": xr.ufuncs.rad2deg,
                     "radians": xr.ufuncs.radians,
                     "real": xr.ufuncs.real,
                     "rint": xr.ufuncs.rint,
                     "sign": xr.ufuncs.sign,
                     "signbit": xr.ufuncs.signbit,
                     "sin": xr.ufuncs.sin,
                     "sinh": xr.ufuncs.sinh,
                     "sqrt": xr.ufuncs.sqrt,
                     "square": xr.ufuncs.square,
                     "tan": xr.ufuncs.tan,
                     "tanh": xr.ufuncs.tanh,
                     "trunc": xr.ufuncs.trunc}

        # Define xarray DataArray operators with 2 input parameter
        self.xfn2 = {"arctan2": xr.ufuncs.arctan2,
                     "copysign": xr.ufuncs.copysign,
                     "fmax": xr.ufuncs.fmax,
                     "fmin": xr.ufuncs.fmin,
                     "fmod": xr.ufuncs.fmod,
                     "hypot": xr.ufuncs.hypot,
                     "ldexp": xr.ufuncs.ldexp,
                     "logaddexp": xr.ufuncs.logaddexp,
                     "logaddexp2": xr.ufuncs.logaddexp2,
                     "logicaland": xr.ufuncs.logical_and,
                     "logicalnot": xr.ufuncs.logical_not,
                     "logicalor": xr.ufuncs.logical_or,
                     "logicalxor": xr.ufuncs.logical_xor,
                     "maximum": xr.ufuncs.maximum,
                     "minimum": xr.ufuncs.minimum,
                     "nextafter": xr.ufuncs.nextafter}

        # Define non-xarray DataArray operators with 2 input parameter
        self.fn2 = {"percentile": np.percentile}

        # Define xarray DataArray reduction operators
        self.xrfn = {"all": xr.DataArray.all,
                     "any": xr.DataArray.any,
                     "argmax": xr.DataArray.argmax,
                     "argmin": xr.DataArray.argmin,
                     "max": xr.DataArray.max,
                     "mean": xr.DataArray.mean,
                     "median": xr.DataArray.median,
                     "min": xr.DataArray.min,
                     "prod": xr.DataArray.prod,
                     "sum": xr.DataArray.sum,
                     "std": xr.DataArray.std,
                     "var": xr.DataArray.var}

        # Define non-xarray DataArray operators with 2 input parameter
        self.xcond = {"<": np.percentile}

        # Define Grammar
        point = Literal(".")
        e = CaselessLiteral("E")
        fnumber = Combine(Word("+-"+nums, nums) +
                          Optional(point + Optional(Word(nums))) +
                          Optional(e + Word("+-"+nums, nums)))
        variable = Word(alphas, alphas+nums+"_$")

        seq = Literal("=")
        b_not = Literal("~")
        plus = Literal("+")
        minus = Literal("-")
        mult = Literal("*")
        div = Literal("/")
        gt = Literal(">")
        gte = Literal(">=")
        lt = Literal("<")
        lte = Literal("<=")
        eq = Literal("==")
        neq = Literal("!=")
        b_or = Literal("|")
        b_and = Literal("&")
        l_not = Literal("!")
        lpar = Literal("(").suppress()
        rpar = Literal(")").suppress()
        comma = Literal(",")
        colon = Literal(":")
        lbrac = Literal("[")
        rbrac = Literal("]")
        lcurl = Literal("{")
        rcurl = Literal("}")
        qmark = Literal("?")
        scolon = Literal(";")
        addop = plus | minus
        multop = mult | div
        sliceop = colon
        compop = gte | lte | gt | lt
        eqop = eq | neq
        bitcompop = b_or | b_and
        bitnotop = b_not
        logicalnotop = l_not
        assignop = seq
        expop = Literal("^")

        expr = Forward()
        indexexpr = Forward()

        atom = (Optional("-") +
                (variable + seq + expr).setParseAction(self.push_assign) |
                indexexpr.setParseAction(self.push_index) |
                (lpar + expr + qmark.setParseAction(self.push_ternary1) + expr +
                 scolon.setParseAction(self.push_ternary2) + expr +
                 rpar).setParseAction(self.push_ternary) |
                (lpar + expr + qmark + expr + scolon + expr +
                 rpar).setParseAction(self.push_ternary) |
                (logicalnotop + expr).setParseAction(self.push_ulnot) |
                (bitnotop + expr).setParseAction(self.push_unot) |
                (variable + lcurl + expr +
                 rcurl).setParseAction(self.push_mask) |
                (variable + lpar + expr + (comma + expr)*3 +
                 rpar).setParseAction(self.push_expr4) |
                (variable + lpar + expr + (comma + expr)*2 +
                 rpar).setParseAction(self.push_expr3) |
                (variable + lpar + expr + comma + expr +
                 rpar).setParseAction(self.push_expr2) |
                (variable + lpar + expr + rpar |
                 variable).setParseAction(self.push_expr1) |
                fnumber.setParseAction(self.push_expr) |
                (lpar + expr.suppress() +
                 rpar).setParseAction(self.push_uminus))

        # Define order of operations for operators

        factor = Forward()
        factor << atom + ZeroOrMore((expop + factor).setParseAction(self.push_op))
        term = factor + ZeroOrMore((multop + factor).setParseAction(self.push_op))
        term2 = term + ZeroOrMore((addop + term).setParseAction(self.push_op))
        term3 = term2 + ZeroOrMore((sliceop + term2).setParseAction(self.push_op))
        term4 = term3 + ZeroOrMore((compop + term3).setParseAction(self.push_op))
        term5 = term4 + ZeroOrMore((eqop + term4).setParseAction(self.push_op))
        term6 = term5 + ZeroOrMore((bitcompop + term5).setParseAction(self.push_op))
        expr << term6 + ZeroOrMore((assignop + term6).setParseAction(self.push_op))

        # Define index operators

        colon_expr = (colon + FollowedBy(comma) ^ colon +
                      FollowedBy(rbrac)).setParseAction(self.push_colon)
        range_expr = colon_expr | expr | colon
        indexexpr << (variable + lbrac + delimitedList(range_expr, delim=',') +
                      rbrac).setParseAction(self.push_expr)

        self.parser = expr

    def set_ae(self, flag):
        self.ae = flag

    def push_expr(self, strg, loc, toks):
        self.expr_stack.append(toks[0])

    def push_expr1(self, strg, loc, toks):
        if toks[0] in self.xrfn:
            self.expr_stack.append('1')
        self.expr_stack.append(toks[0])

    def push_expr2(self, strg, loc, toks):
        if toks[0] in self.xrfn:
            self.expr_stack.append('2')
        self.expr_stack.append(toks[0])

    def push_expr3(self, strg, loc, toks):
        if toks[0] in self.xrfn:
            self.expr_stack.append('3')
        self.expr_stack.append(toks[0])

    def push_expr4(self, strg, loc, toks):
        if toks[0] in self.xrfn:
            self.expr_stack.append('4')
        self.expr_stack.append(toks[0])

    def push_op(self, strg, loc, toks):
        self.expr_stack.append(toks[0])

    def push_uminus(self, strg, loc, toks):
        if toks and toks[0] == '-':
            self.expr_stack.append('unary -')

    def push_unot(self, strg, loc, toks):
        if toks and toks[0] == '~':
            self.expr_stack.append('unary ~')

    def push_ulnot(self, strg, loc, toks):
        if toks and toks[0] == '!':
            self.expr_stack.append('unary !')

    def push_index(self, strg, loc, toks):
        self.expr_stack.append("[]")

    def push_colon(self, strg, loc, toks):
        self.expr_stack.append("::")

    def push_mask(self, strg, loc, toks):
        self.expr_stack.append(toks[0])
        self.expr_stack.append("{}")

    def push_assign(self, strg, loc, toks):
        self.expr_stack.append(toks[0])
        self.expr_stack.append("=")

    def push_ternary(self, strg, loc, toks):
        self.texpr_stack.append(self.expr_stack)
        self.expr_stack = []
        self.expr_stack.append(self.texpr_stack[::-1])
        self.expr_stack.append('?')
        self.expr_stack = self.flatten_list(self.expr_stack)
        self.texpr_stack = []

    def push_ternary1(self, strg, loc, toks):
        self.texpr_stack.append(self.expr_stack)
        self.expr_stack = []

    def push_ternary2(self, strg, loc, toks):
        self.texpr_stack.append(self.expr_stack)
        self.expr_stack = []

    def evaluate_stack(self, s):
        op = s.pop()
        if op == 'unary -':
            return -self.evaluate_stack(s)
        elif op == 'unary ~':
            return ~self.evaluate_stack(s)
        elif op == 'unary !':
            return not self.evaluate_stack(s)
        elif op == "=":
            op1 = s.pop()
            op2 = self.evaluate_stack(s)
            self.f.f_globals[op1] = op2

            # code to write to locals, need to sort out when to write to locals/globals.
            # self.f.f_locals[op1] = op2
            # ctypes.pythonapi.PyFrame_LocalsToFast(ctypes.py_object(self.f), ctypes.c_int(1))
        elif op in self.opn.keys():
            op2 = self.evaluate_stack(s)
            op1 = self.evaluate_stack(s)
            if op == '+' and isinstance(op2, xr.DataArray) and \
               op2.dtype.type == np.bool_:
                return xr.DataArray.where(op1, op2)
            return self.opn[op](op1, op2)
        elif op == "::":
            return slice(None, None, None)
        elif op in self.xrfn:
            dim = int(self.evaluate_stack(s))
            dims = ()
            for i in range(1, dim):
                dims += int(self.evaluate_stack(s)),
            op1 = self.evaluate_stack(s)

            args = {}
            if op == 'argmax' or op == 'argmin':
                if dim != 1:
                    args['axis'] = dims[0]
            elif dim != 1:
                args['axis'] = dims

            if 'skipna' in inspect.getargspec(self.xrfn[op])[0] and \
               op != 'prod':
                args['skipna'] = True

            val = self.xrfn[op](xr.DataArray(op1), **args)
            return val
        elif op in self.xfn1:
            val = self.xfn1[op](self.evaluate_stack(s))

            if isinstance(val, tuple) or isinstance(val, np.ndarray):
                return xr.DataArray(val)
            return val
        elif op in self.xfn2:
            op2 = self.evaluate_stack(s)
            op1 = self.evaluate_stack(s)
            val = self.xfn2[op](op1, op2)

            if isinstance(val, tuple) or isinstance(val, np.ndarray):
                return xr.DataArray(val)
            return val
        elif op in self.fn2:
            op2 = self.evaluate_stack(s)
            op1 = self.evaluate_stack(s)
            val = self.fn2[op](op1, op2)

            if isinstance(val, tuple) or isinstance(val, np.ndarray):
                return xr.DataArray(val)
            return val
        elif op in ":":
            op2 = int(self.evaluate_stack(s))
            op1 = int(self.evaluate_stack(s))

            return slice(op1, op2, None)
        elif op in "[]":
            op1 = self.evaluate_stack(s)
            ops = ()
            i = 0
            dims = len(s)
            while len(s) > 0:
                val = self.evaluate_stack(s)
                if not isinstance(val, slice):
                    val = int(val)
                ops += val,
                i = i+1
            ops = ops[::-1]
            return op1[ops]
        elif op in "{}":
            op1 = self.evaluate_stack(s)
            if self.ae:
                op2 = self.evaluate_stack(s).astype(np.int64).values
                op2 = self.get_pqa_mask(op2)
            else:
                op2 = self.evaluate_stack(s)

            val = xr.DataArray.where(op1, op2)
            return val
        elif op == "?":
            op1 = s.pop()
            op2 = s.pop()
            op3 = s.pop()

            ifval = self.evaluate_stack(op1)
            if ifval:
                return self.evaluate_stack(op2)
            else:
                return self.evaluate_stack(op3)
        elif op[0].isalpha():
            if self.local_dict is not None and op in self.local_dict:
                return self.local_dict[op]
            frame = self.getframe(op)
            if op in frame.f_locals:
                return frame.f_locals[op]
            if op in frame.f_globals:
                return frame.f_globals[op]
        else:
            return float(op)

    def is_number(self, s):
        try:
            float(s)
            return True
        except ValueError:
            return False

    def flatten_list(self, l):
        return [item for sublist in l for item in sublist]

    def getframe(self, var):
        try:
            limit = sys.getrecursionlimit()
            for i in range(0, limit):
                frame = sys._getframe(i)
                if var in frame.f_locals or var in frame.f_globals:
                    return frame
            return self.f
        except ValueError:
            return self.f

    def evaluate(self, s, local_dict=None):
        if local_dict is None:
            self.local_dict = None
            self.f = sys._getframe(1)
        else:
            self.f = None
            self.local_dict = local_dict
        self.expr_stack = []
        results = self.parser.parseString(s)
        #print(self.expr_stack)
        val = self.evaluate_stack(self.expr_stack[:])
        return val

    def test(self, s, e):
        result = self.evaluate(s)
        self.f = sys._getframe(1)
        if isinstance(result, int) or isinstance(result, float) or \
           isinstance(result, np.float64):
            r = e == result
        else:
            r = e.equals(result)
        if r:
            print(s, "=", r)
            return True
        else:
            print(s, "=", r, " ****** FAILED ******")
            return False

    def get_pqa_mask(self, pqa_ndarray):
        '''
        create pqa_mask from a ndarray

        Parameters:
            pqa_ndarray: input pqa array
            good_pixel_masks: known good pixel values
            dilation: amount of dilation to apply
        '''
        good_pixel_masks = [32767, 16383, 2457]
        dilation = 3
        pqa_mask = np.zeros(pqa_ndarray.shape, dtype=np.bool)
        for i in range(len(pqa_ndarray)):
            pqa_array = pqa_ndarray[i]
            # Ignore bit 6 (saturation for band 62) - always 0 for Landsat 5
            pqa_array = pqa_array | 64

            # Dilating both the cloud and cloud shadow masks
            s = [[1, 1, 1], [1, 1, 1], [1, 1, 1]]
            acca = (pqa_array & 1024) >> 10
            erode = ndimage.binary_erosion(acca, s, iterations=dilation,
                                           border_value=1)
            dif = erode - acca
            dif[dif < 0] = 1
            pqa_array += (dif << 10)
            del acca
            fmask = (pqa_array & 2048) >> 11
            erode = ndimage.binary_erosion(fmask, s, iterations=dilation,
                                           border_value=1)
            dif = erode - fmask
            dif[dif < 0] = 1
            pqa_array += (dif << 11)
            del fmask
            acca_shad = (pqa_array & 4096) >> 12
            erode = ndimage.binary_erosion(acca_shad, s, iterations=dilation,
                                           border_value=1)
            dif = erode - acca_shad
            dif[dif < 0] = 1
            pqa_array += (dif << 12)
            del acca_shad
            fmask_shad = (pqa_array & 8192) >> 13
            erode = ndimage.binary_erosion(fmask_shad, s, iterations=dilation,
                                           border_value=1)
            dif = erode - fmask_shad
            dif[dif < 0] = 1
            pqa_array += (dif << 13)

            for good_pixel_mask in good_pixel_masks:
                pqa_mask[i][pqa_array == good_pixel_mask] = True
        return pqa_mask

    def plot_3d(self, array_result):
        print('plot3D')

        img = array_result
        num_t = img.shape[0]
        num_rowcol = math.ceil(math.sqrt(num_t))
        fig = plt.figure(1)
        fig.clf()
        plot_count = 1
        for i in range(img.shape[0]):
            data = img[i]
            # data[data == -999] = 0
            ax = fig.add_subplot(num_rowcol, num_rowcol, plot_count)
            cax = ax.imshow(data, interpolation='nearest', aspect='equal')
            plot_count += 1
        fig.tight_layout()
        plt.subplots_adjust(wspace=0.5, hspace=0.5)
        plt.show()

    def test_1_level(self):
        x5 = xr.DataArray(np.random.randn(2, 3))
        self.evaluate("z5 = x5 + 1")
        print(z5)

    def test_2_level(self):
        self.test_2_level_fn()

    def test_2_level_fn(self):
        x6 = xr.DataArray(np.random.randn(2, 3))
        self.evaluate("z6 = x6 + 1")
        print(z6)

    def test2(self):
        x1 = xr.DataArray(np.random.randn(2, 3))
        y1 = xr.DataArray(np.random.randn(2, 3))
        z1 = xr.DataArray(np.array([[[0, 1, 2], [3, 4, 5], [6, 7, 8]],
                                    [[9, 10, 11], [12, 13, 14], [15, 16, 17]],
                                    [[18, 19, 20], [21, 22, 23], [24, 25, 26]]]))
        z2 = z1*2
        z3 = np.arange(27)
        mask1 = z1 > 4

        ne = NDexpr()

        ne.test("angle(z1)", xr.ufuncs.angle(z1))
        ne.test("arccos(z1)", xr.ufuncs.arccos(z1))
        ne.test("arccosh(z1)", xr.ufuncs.arccosh(z1))
        ne.test("arcsin(z1)", xr.ufuncs.arcsin(z1))
        ne.test("arcsinh(z1)", xr.ufuncs.arcsinh(z1))
        ne.test("arctan(z1)", xr.ufuncs.arctan(z1))
        ne.test("arctanh(z1)", xr.ufuncs.arctanh(z1))
        ne.test("ceil(z1)", xr.ufuncs.ceil(z1))
        ne.test("conj(z1)", xr.ufuncs.conj(z1))
        ne.test("cos(z1)", xr.ufuncs.cos(z1))
        ne.test("cosh(z1)", xr.ufuncs.cosh(z1))
        ne.test("deg2rad(z1)", xr.ufuncs.deg2rad(z1))
        ne.test("degrees(z1)", xr.ufuncs.degrees(z1))
        ne.test("exp(z1)", xr.ufuncs.exp(z1))
        ne.test("expm1(z1)", xr.ufuncs.expm1(z1))
        ne.test("fabs(z1)", xr.ufuncs.fabs(z1))
        ne.test("fix(z1)", xr.ufuncs.fix(z1))
        ne.test("floor(z1)", xr.ufuncs.floor(z1))
        ne.test("frexp(z3)", xr.DataArray(xr.ufuncs.frexp(z3)))
        ne.test("imag(z1)", xr.ufuncs.imag(z1))
        ne.test("iscomplex(z1)", xr.ufuncs.iscomplex(z1))
        ne.test("isfinite(z1)", xr.ufuncs.isfinite(z1))
        ne.test("isinf(z1)", xr.ufuncs.isinf(z1))
        ne.test("isnan(z1)", xr.ufuncs.isnan(z1))
        ne.test("isreal(z1)", xr.ufuncs.isreal(z1))
        ne.test("log(z1)", xr.ufuncs.log(z1))
        ne.test("log10(z1)", xr.ufuncs.log10(z1))
        ne.test("log1p(z1)", xr.ufuncs.log1p(z1))
        ne.test("log2(z1)", xr.ufuncs.log2(z1))
        ne.test("rad2deg(z1)", xr.ufuncs.rad2deg(z1))
        ne.test("radians(z1)", xr.ufuncs.radians(z1))
        ne.test("real(z1)", xr.ufuncs.real(z1))
        ne.test("rint(z1)", xr.ufuncs.rint(z1))
        ne.test("sign(z1)", xr.ufuncs.sign(z1))
        ne.test("signbit(z1)", xr.ufuncs.signbit(z1))
        ne.test("sin(z1)", xr.ufuncs.sin(z1))
        ne.test("sinh(z1)", xr.ufuncs.sinh(z1))
        ne.test("sqrt(z1)", xr.ufuncs.sqrt(z1))
        ne.test("square(z1)", xr.ufuncs.square(z1))
        ne.test("tan(z1)", xr.ufuncs.tan(z1))
        ne.test("tanh(z1)", xr.ufuncs.tanh(z1))
        ne.test("trunc(z1)", xr.ufuncs.trunc(z1))

        ne.test("arctan2(z1, z2)", xr.ufuncs.arctan2(z1, z2))
        ne.test("copysign(z1, z2)", xr.ufuncs.copysign(z1, z2))
        ne.test("fmax(z1, z2)", xr.ufuncs.fmax(z1, z2))
        ne.test("fmin(z1, z2)", xr.ufuncs.fmin(z1, z2))
        ne.test("fmod(z1, z2)", xr.ufuncs.fmod(z1, z2))
        ne.test("hypot(z1, z2)", xr.ufuncs.hypot(z1, z2))
        ne.test("ldexp(z1, z2)", xr.DataArray(xr.ufuncs.ldexp(z1, z2)))
        ne.test("logaddexp(z1, z2)", xr.ufuncs.logaddexp(z1, z2))
        ne.test("logaddexp2(z1, z2)", xr.ufuncs.logaddexp2(z1, z2))
        ne.test("logicaland(z1, z2)", xr.ufuncs.logical_and(z1, z2))
        ne.test("logicalnot(z1, z2)", xr.ufuncs.logical_not(z1, z2))
        ne.test("logicalor(z1, z2)", xr.ufuncs.logical_or(z1, z2))
        ne.test("logicalxor(z1, z2)", xr.ufuncs.logical_xor(z1, z2))
        ne.test("maximum(z1, z2)", xr.ufuncs.maximum(z1, z2))
        ne.test("minimum(z1, z2)", xr.ufuncs.minimum(z1, z2))
        ne.test("nextafter(z1, z2)", xr.ufuncs.nextafter(z1, z2))

        ne.test("all(z1)", xr.DataArray.all(z1))
        ne.test("all(z1, 0)", xr.DataArray.all(z1, axis=0))
        ne.test("all(z1, 0, 1)", xr.DataArray.all(z1, axis=(0, 1)))
        ne.test("all(z1, 0, 1, 2)", xr.DataArray.all(z1, axis=(0, 1, 2)))

        ne.test("any(z1)", xr.DataArray.any(z1))
        ne.test("any(z1, 0)", xr.DataArray.any(z1, axis=0))
        ne.test("any(z1, 0, 1)", xr.DataArray.any(z1, axis=(0, 1)))
        ne.test("any(z1, 0, 1, 2)", xr.DataArray.any(z1, axis=(0, 1, 2)))

        ne.test("argmax(z1)", xr.DataArray.argmax(z1))
        ne.test("argmax(z1, 0)", xr.DataArray.argmax(z1, axis=0))
        ne.test("argmax(z1, 1)", xr.DataArray.argmax(z1, axis=1))
        ne.test("argmax(z1, 2)", xr.DataArray.argmax(z1, axis=2))

        ne.test("argmin(z1)", xr.DataArray.argmin(z1))
        ne.test("argmin(z1, 0)", xr.DataArray.argmin(z1, axis=0))
        ne.test("argmin(z1, 1)", xr.DataArray.argmin(z1, axis=1))
        ne.test("argmin(z1, 2)", xr.DataArray.argmin(z1, axis=2))

        ne.test("max(z1)", xr.DataArray.max(z1))
        ne.test("max(z1, 0)", xr.DataArray.max(z1, axis=0))
        ne.test("max(z1, 0, 1)", xr.DataArray.max(z1, axis=(0, 1)))
        ne.test("max(z1, 0, 1, 2)", xr.DataArray.max(z1, axis=(0, 1, 2)))

        ne.test("mean(z1)", xr.DataArray.mean(z1))
        ne.test("mean(z1, 0)", xr.DataArray.mean(z1, axis=0))
        ne.test("mean(z1, 0, 1)", xr.DataArray.mean(z1, axis=(0, 1)))
        ne.test("mean(z1, 0, 1, 2)", xr.DataArray.mean(z1, axis=(0, 1, 2)))

        ne.test("median(z1)", xr.DataArray.median(z1))
        ne.test("median(z1, 0)", xr.DataArray.median(z1, axis=0))
        ne.test("median(z1, 0, 1)", xr.DataArray.median(z1, axis=(0, 1)))
        ne.test("median(z1, 0, 1, 2)", xr.DataArray.median(z1, axis=(0, 1, 2)))

        ne.test("min(z1)", xr.DataArray.min(z1))
        ne.test("min(z1, 0)", xr.DataArray.min(z1, axis=0))
        ne.test("min(z1, 0, 1)", xr.DataArray.min(z1, axis=(0, 1)))
        ne.test("min(z1, 0, 1, 2)", xr.DataArray.min(z1, axis=(0, 1, 2)))

        ne.test("prod(z1)", xr.DataArray.prod(z1))
        ne.test("prod(z1, 0)", xr.DataArray.prod(z1, axis=0))
        ne.test("prod(z1, 0, 1)", xr.DataArray.prod(z1, axis=(0, 1)))
        ne.test("prod(z1, 0, 1, 2)", xr.DataArray.prod(z1, axis=(0, 1, 2)))

        ne.test("sum(z1)", xr.DataArray.sum(z1))
        ne.test("sum(z1, 0)", xr.DataArray.sum(z1, axis=0))
        ne.test("sum(z1, 0, 1)", xr.DataArray.sum(z1, axis=(0, 1)))
        ne.test("sum(z1, 0, 1, 2)", xr.DataArray.sum(z1, axis=(0, 1, 2)))

        ne.test("std(z1)", xr.DataArray.std(z1))
        ne.test("std(z1, 0)", xr.DataArray.std(z1, axis=0))
        ne.test("std(z1, 0, 1)", xr.DataArray.std(z1, axis=(0, 1)))
        ne.test("std(z1, 0, 1, 2)", xr.DataArray.std(z1, axis=(0, 1, 2)))

        ne.test("var(z1)", xr.DataArray.var(z1))
        ne.test("var(z1, 0)", xr.DataArray.var(z1, axis=0))
        ne.test("var(z1, 0, 1)", xr.DataArray.var(z1, axis=(0, 1)))
        ne.test("var(z1, 0, 1, 2)", xr.DataArray.var(z1, axis=(0, 1, 2)))

        ne.test("percentile(z1, 50)", np.percentile(z1, 50))
        ne.test("percentile(z1, 50)+percentile(z1, 50)", np.percentile(z1, 50) + np.percentile(z1, 50))
        ne.test("1 + var(z1, 0, 0+1, 2) + 1", 1+xr.DataArray.var(z1, axis=(0, 0+1, 2))+1)

        ne.test("z1{mask1}", xr.DataArray.where(z1, mask1))
        ne.test("z1{z1>2}", xr.DataArray.where(z1, z1 > 2))
        ne.test("z1{z1>=2}", xr.DataArray.where(z1, z1 >= 2))
        ne.test("z1{z1<2}", xr.DataArray.where(z1, z1 < 2))
        ne.test("z1{z1<=2}", xr.DataArray.where(z1, z1 <= 2))
        ne.test("z1{z1==2}", xr.DataArray.where(z1, z1 == 2))
        ne.test("z1{z1!=2}", xr.DataArray.where(z1, z1 != 2))

        ne.test("z1{z1<2 | z1>5}", xr.DataArray.where(z1, (z1 < 2) | (z1 > 5)))
        ne.test("z1{z1>2 & z1<5}", xr.DataArray.where(z1, (z1 > 2) & (z1 < 5)))

        ne.evaluate("m = z1+1")
        ne.test("m", z1+1)

        ne.test("z1{~mask1}", xr.DataArray.where(z1, ~mask1))

        print(ne.evaluate("(1<0?1+1;2+2)"))
        print(ne.evaluate("(1<2?z1+1;2+2)"))
        ne.test("z1+mask1", xr.DataArray.where(z1, mask1))

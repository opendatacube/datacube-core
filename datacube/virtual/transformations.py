import numpy
import xarray
import lark

from datacube.storage.masking import make_mask as make_mask_prim
from datacube.storage.masking import mask_invalid_data as mask_invalid_data_prim

from .impl import VirtualProductException, Transformation, Measurement


def selective_apply_dict(dictionary, apply_to=None, key_map=None, value_map=None):
    def skip(key):
        return apply_to is not None and key not in apply_to

    def key_worker(key):
        if key_map is None or skip(key):
            return key

        return key_map(key)

    def value_worker(key, value):
        if value_map is None or skip(key):
            return value

        return value_map(key, value)

    return {key_worker(key): value_worker(key, value)
            for key, value in dictionary.items()}


def selective_apply(data, apply_to=None, key_map=None, value_map=None):
    return xarray.Dataset(data_vars=selective_apply_dict(data.data_vars, apply_to=apply_to,
                                                         key_map=key_map, value_map=value_map),
                          coords=data.coords, attrs=data.attrs)


class MakeMask(Transformation):
    """
    Create a mask that would only keep pixels for which the measurement with `mask_measurement_name`
    of the `product` satisfies `flags`.
    """

    def __init__(self, mask_measurement_name, flags):
        self.mask_measurement_name = mask_measurement_name
        self.flags = flags

    def measurements(self, input_measurements):
        if self.mask_measurement_name not in input_measurements:
            raise VirtualProductException("required measurement {} not found"
                                          .format(self.mask_measurement_name))

        def worker(_, value):
            result = value.copy()
            result['dtype'] = 'bool'
            return Measurement(**result)

        return selective_apply_dict(input_measurements,
                                    apply_to=[self.mask_measurement_name], value_map=worker)

    def compute(self, data):
        def worker(_, value):
            return make_mask_prim(value, **self.flags)

        return selective_apply(data, apply_to=[self.mask_measurement_name], value_map=worker)


class ApplyMask(Transformation):
    def __init__(self, mask_measurement_name, apply_to=None,
                 preserve_dtype=True, fallback_dtype='float32'):
        self.mask_measurement_name = mask_measurement_name
        self.apply_to = apply_to
        self.preserve_dtype = preserve_dtype
        self.fallback_dtype = fallback_dtype

    def measurements(self, input_measurements):
        rest = {key: value
                for key, value in input_measurements.items()
                if key != self.mask_measurement_name}

        def worker(_, value):
            if self.preserve_dtype:
                return value

            result = value.copy()
            result['dtype'] = self.fallback_dtype
            return Measurement(**result)

        return selective_apply_dict(rest, apply_to=self.apply_to, value_map=worker)

    def compute(self, data):
        mask = data[self.mask_measurement_name]
        rest = data.drop(self.mask_measurement_name)

        def worker(key, value):
            if self.preserve_dtype:
                if 'nodata' not in value.attrs:
                    raise VirtualProductException("measurement {} has no nodata value".format(key))
                return value.where(mask, value.attrs['nodata'])

            return value.where(mask).astype(self.fallback_dtype)

        return selective_apply(rest, apply_to=self.apply_to, value_map=worker)


class ToFloat(Transformation):
    def __init__(self, apply_to=None, dtype='float32'):
        self.apply_to = apply_to
        self.dtype = dtype

    def measurements(self, input_measurements):
        def worker(_, value):
            result = value.copy()
            result['dtype'] = self.dtype
            return Measurement(**result)

        return selective_apply_dict(input_measurements, apply_to=self.apply_to, value_map=worker)

    def compute(self, data):
        def worker(_, value):
            if hasattr(value, 'dtype') and value.dtype == self.dtype:
                return value

            return mask_invalid_data_prim(value).astype(self.dtype)

        return selective_apply(data, apply_to=self.apply_to, value_map=worker)


class Rename(Transformation):
    def __init__(self, measurement_names):
        self.measurement_names = measurement_names

    def measurements(self, input_measurements):
        def key_map(key):
            return self.measurement_names[key]

        def value_map(key, value):
            result = value.copy()
            result['name'] = self.measurement_names[key]
            return Measurement(**result)

        return selective_apply_dict(input_measurements, apply_to=self.measurement_names,
                                    key_map=key_map, value_map=value_map)

    def compute(self, data):
        return data.rename(self.measurement_names)


class Select(Transformation):
    def __init__(self, measurement_names):
        self.measurement_names = measurement_names

    def measurements(self, input_measurements):
        return {key: value
                for key, value in input_measurements.items()
                if key in self.measurement_names}

    def compute(self, data):
        return data.drop([measurement
                          for measurement in data.data_vars
                          if measurement not in self.measurement_names])


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
class EvaluateTree(lark.Transformer):
    from operator import not_, or_, and_, xor
    from operator import eq, ne, le, ge, lt, gt
    from operator import add, sub, mul, truediv, floordiv, neg, pos, inv, mod, pow, lshift, rshift

    float_literal = float
    int_literal = int


class Formula(Transformation):
    def __init__(self, output):
        self.output = output

    def measurements(self, input_measurements):
        parser = formula_parser()

        @lark.v_args(inline=True)
        class EvaluateType(EvaluateTree):
            def var_name(self, key):
                return numpy.array([0], dtype=input_measurements[key.value].dtype)

        ev = EvaluateType()

        def deduce_type(output_var, output_desc):
            formula = output_desc['formula']
            tree = parser.parse(formula)

            result = ev.transform(tree)
            return result.dtype

        def measurement(output_var, output_desc):
            return Measurement(name=output_var, dtype=deduce_type(output_var, output_desc),
                               nodata=output_desc.get('nodata'), units=output_desc.get('units'))

        return {output_var: measurement(output_var, output_desc)
                for output_var, output_desc in self.output.items()}

    def compute(self, data):
        parser = formula_parser()

        @lark.v_args(inline=True)
        class EvaluateData(EvaluateTree):
            def var_name(self, key):
                return data[key.value]

        ev = EvaluateData()

        def result(output_var, output_desc):
            formula = output_desc['formula']
            tree = parser.parse(formula)

            return ev.transform(tree)

        return xarray.Dataset(data_vars={output_var: result(output_var, output_desc)
                                         for output_var, output_desc in self.output.items()},
                              coords=data.coords, attrs=data.attrs)


def year(time):
    return time.astype('datetime64[Y]')


def month(time):
    return time.astype('datetime64[M]')


def week(time):
    return time.astype('datetime64[W]')


def day(time):
    return time.astype('datetime64[D]')


# TODO: all time stats

class Mean(Transformation):
    """
    Take the mean of the measurements.
    """

    def __init__(self, dim='time'):
        self.dim = dim

    def measurements(self, input_measurements):
        return input_measurements

    def compute(self, data):
        return data.mean(dim=self.dim)

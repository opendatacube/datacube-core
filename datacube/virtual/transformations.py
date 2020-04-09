import numpy
import xarray
import lark

from datacube.utils.masking import make_mask as make_mask_prim
from datacube.utils.masking import mask_invalid_data as mask_invalid_data_prim
from datacube.utils.masking import valid_data_mask

from datacube.utils.math import dtype_is_float

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
    Create a mask that would only keep pixels for which the specified measurement
    satisfies `flags` specification.

    Alias in recipe: ``make_mask``.
    """

    def __init__(self, mask_measurement_name, flags):
        """
        Initialize transformation.

        :param mask_measurement_name: the name of the measurement to turn into a boolean mask
        :param flags: a dictionary of flag value specifications for the ``flags_definition`` of that measurement
        """
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
    """
    Apply a boolean mask to other measurements.

    Alias in recipe: ``apply_mask``.
    """
    def __init__(self, mask_measurement_name, apply_to=None,
                 preserve_dtype=True, fallback_dtype='float32', dilation=0):
        """
        Initialize transformation.

        :param mask_measurement_name: the measurement name of the boolean mask
        :param apply_to: list of names of measurements to apply the mask to
        :param preserve_dtype: whether to cast back to original ``dtype`` after masking
        :param fallback_dtype: default ``dtype`` for masked measurements
        :param dilation: the dilation to apply to mask in pixels
        """
        self.mask_measurement_name = mask_measurement_name
        self.apply_to = apply_to
        self.preserve_dtype = preserve_dtype
        self.fallback_dtype = fallback_dtype
        self.dilation = int(dilation)

    def measurements(self, input_measurements):
        rest = {key: value
                for key, value in input_measurements.items()
                if key != self.mask_measurement_name}

        def worker(_, value):
            if self.preserve_dtype:
                return value

            result = value.copy()
            result['dtype'] = self.fallback_dtype
            result['nodata'] = float('nan')
            return Measurement(**result)

        return selective_apply_dict(rest, apply_to=self.apply_to, value_map=worker)

    def compute(self, data):
        mask = data[self.mask_measurement_name]
        rest = data.drop(self.mask_measurement_name)

        def dilate(array):
            """Dilation e.g. for the mask"""
            # e.g. kernel = [[1] * 7] * 7 # blocky 3-pixel dilation
            # pylint: disable=invalid-unary-operand-type
            y, x = numpy.ogrid[-self.dilation:(self.dilation+1), -self.dilation:(self.dilation+1)]
            kernel = ((x * x) + (y * y) <= (self.dilation + 0.5) ** 2)  # disk-like `self.dilation` radial dilation
            return ~scipy.ndimage.binary_dilation(~array.astype(numpy.bool),
                                                  structure=kernel.reshape((1, )+kernel.shape))

        if self.dilation > 0:
            import scipy.ndimage
            mask = xarray.apply_ufunc(dilate, mask, output_dtypes=[numpy.bool], dask='parallelized',
                                      keep_attrs=True)

        def worker(key, value):
            if self.preserve_dtype:
                if 'nodata' not in value.attrs:
                    raise VirtualProductException("measurement {} has no nodata value".format(key))
                return value.where(mask, value.attrs['nodata'])

            result = value.where(mask).astype(self.fallback_dtype)
            result.attrs['nodata'] = float('nan')
            return result

        return selective_apply(rest, apply_to=self.apply_to, value_map=worker)


class ToFloat(Transformation):
    """
    Convert measurements to floats and mask invalid data.

    Alias in recipe: ``to_float``.
    """
    def __init__(self, apply_to=None, dtype='float32'):
        """
        Initialize transformation.

        :param apply_to: list of names of measurements to apply conversion to
        :param dtype: default ``dtype`` for conversion
        """
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
    """
    Rename measurements.

    Alias in recipe: ``rename``.
    """
    def __init__(self, measurement_names):
        """
        Initialize transformation.

        :param measurement_names: a dictionary mapping original names to new names
        """
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
    """
    Keep only specified measurements.

    Alias in recipe: ``select``.
    """
    def __init__(self, measurement_names):
        """
        Initialize transformation.

        :param measurement_names: list of measurements to keep
        """
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


class Expressions(Transformation):
    """
    Calculate measurements on-the-fly using arithmetic expressions.

    Alias in recipe: ``expressions``. For example,

    .. code-block:: yaml

       transform: expressions
       output:
           ndvi:
               formula: (nir - red) / (nir + red)

       input:
           product: example_surface_reflectance_product
           measurements: [nir, red]

    """
    def __init__(self, output, masked=True):
        """
        Initialize transformation.

        :param output:
            A dictionary mapping output measurement names to specifications.
            That specification can be one of:

            - a measurement name from the input product in which case it is copied over
            - a dictionary containing a ``formula``,
              and optionally a ``dtype``, a new ``nodata`` value, and a ``units`` specification

        :param masked:
            Defaults to ``True``. If set to ``False``, the inputs and outputs are not masked for no data.
        """
        self.output = output
        self.masked = masked

    def measurements(self, input_measurements):
        parser = formula_parser()

        @lark.v_args(inline=True)
        class EvaluateType(EvaluateTree):
            def var_name(self, key):
                return numpy.array([], dtype=input_measurements[key.value].dtype)

        ev = EvaluateType()

        def deduce_type(output_var, output_desc):
            if 'dtype' in output_desc:
                return numpy.dtype(output_desc['dtype'])

            formula = output_desc['formula']
            tree = parser.parse(formula)

            result = ev.transform(tree)
            return result.dtype

        def measurement(output_var, output_desc):
            if isinstance(output_desc, str):
                # copy measurement over
                return input_measurements[output_desc]

            return Measurement(name=output_var, dtype=deduce_type(output_var, output_desc),
                               nodata=output_desc.get('nodata', float('nan')),
                               units=output_desc.get('units', '1'))

        return {output_var: measurement(output_var, output_desc)
                for output_var, output_desc in self.output.items()}

    def compute(self, data):
        parser = formula_parser()

        @lark.v_args(inline=True)
        class EvaluateData(EvaluateTree):
            def var_name(self, key):
                return data[key.value]

        @lark.v_args(inline=True)
        class EvaluateNodataMask(lark.Transformer):
            # the result of an expression is nodata whenever any of its subexpressions is nodata
            from operator import or_

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

            def var_name(self, key):
                # pylint: disable=invalid-unary-operand-type
                return ~valid_data_mask(data[key.value])

        ev_data = EvaluateData()
        ev_mask = EvaluateNodataMask()

        def result(output_var, output_desc):
            # pylint: disable=invalid-unary-operand-type

            if isinstance(output_desc, str):
                # copy measurement over
                return data[output_desc]

            nodata = output_desc.get('nodata')
            dtype = output_desc.get('dtype')

            formula = output_desc['formula']
            tree = parser.parse(formula)
            result = ev_data.transform(tree)
            result.attrs['crs'] = data.attrs['crs']
            if nodata is not None:
                result.attrs['nodata'] = nodata
            result.attrs['units'] = output_desc.get('units', '1')

            if 'masked' in output_desc:
                masked = output_desc['masked']
            else:
                masked = self.masked

            if not masked:
                if dtype is None:
                    return result
                return result.astype(dtype)

            # masked output
            if dtype is not None:
                result = result.astype(dtype)

            dtype = result.dtype
            mask = ev_mask.transform(tree)

            if numpy.dtype(dtype) == numpy.bool:
                # any operation on nodata should evaluate to False
                # omission of attrs['nodata'] is deliberate
                result = result.where(~mask, False)

            elif nodata is None:
                if not dtype_is_float(dtype):
                    raise VirtualProductException("cannot mask without specified nodata")

                result = result.where(~mask)
                result.attrs['nodata'] = numpy.nan

            else:
                result = result.where(~mask, nodata)
                result.attrs['nodata'] = nodata

            return result

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

def earliest_time(time):
    earliest_time = time.copy()
    earliest_time.data[0:] = year(time).data[0]
    return earliest_time

class XarrayReduction(Transformation):
    """
    Apply an `xarray` reduction method to the data.
    """

    def __init__(self, method=None, apply_to=None, dtype=None, dim='time', **kwargs):
        if method is None:
            raise VirtualProductException("no method specified in xarray reduction")

        self.method = method
        self.kwargs = kwargs
        self.apply_to = apply_to
        self.dtype = dtype
        self.dim = dim

    def measurements(self, input_measurements):
        def worker(_, value):
            if self.dtype is None:
                return value

            result = value.copy()
            result['dtype'] = self.dtype
            return Measurement(**result)

        return selective_apply_dict(input_measurements,
                                    apply_to=self.apply_to, value_map=worker)

    def compute(self, data):
        func = getattr(xarray.DataArray, self.method)

        def worker(_, value):
            return func(value, dim=self.dim, **self.kwargs)

        return selective_apply(data, apply_to=self.apply_to, value_map=worker)

# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2020 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
from typing import Optional, Collection
import warnings

import numpy
import xarray
import pandas as pd

from datacube.utils.masking import make_mask as make_mask_prim
from datacube.utils.masking import mask_invalid_data as mask_invalid_data_prim

from datacube.utils.math import dtype_is_float

from .impl import VirtualProductException, Transformation, Measurement
from .expr import FormulaEvaluator, MaskEvaluator
from .expr import formula_parser, evaluate_data, evaluate_nodata_mask, evaluate_type


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

    Alias in recipe: ``make_mask``.

    :param mask_measurement_name: the name of the measurement to create the mask from
    :param flags: definition of the flags for the mask
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
    """
    Apply a boolean mask to other measurements.

    Alias in recipe: ``apply_mask``.

    :param mask_measurement_name: name of the measurement to use as a mask
    :param apply_to: list of names of measurements to apply the mask to
    :param preserve_dtype: whether to cast back to original ``dtype`` after masking
    :param fallback_dtype: default ``dtype`` for masked measurements
    :param erosion: the erosion to apply to mask in pixels
    :param dilation: the dilation to apply to mask in pixels
    """
    def __init__(self, mask_measurement_name, apply_to: Optional[Collection[str]] = None,
                 preserve_dtype=True, fallback_dtype='float32', erosion: int = 0, dilation: int = 0):
        self.mask_measurement_name = mask_measurement_name
        self.apply_to = apply_to
        self.preserve_dtype = preserve_dtype
        self.fallback_dtype = fallback_dtype
        self.erosion = int(erosion)
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
        rest = data.drop_vars([self.mask_measurement_name])

        if self.erosion > 0:
            from skimage.morphology import binary_erosion, disk
            kernel = disk(self.erosion)
            mask = ~xarray.apply_ufunc(binary_erosion,
                                       ~mask,
                                       kernel.reshape((1, ) + kernel.shape),
                                       output_dtypes=[bool],
                                       dask='parallelized',
                                       keep_attrs=True)

        if self.dilation > 0:
            from skimage.morphology import binary_dilation, disk
            kernel = disk(self.dilation)
            mask = ~xarray.apply_ufunc(binary_dilation,
                                       ~mask,
                                       kernel.reshape((1, ) + kernel.shape),
                                       output_dtypes=[bool],
                                       dask='parallelized',
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

    .. note::

        The ``to_float`` transform is deprecated. Please use ``expressions`` instead.

        Using ``to_float``:

        .. code-block:: yaml

            transform: to_float
            apply_to: [green]
            dtype: float32
            input: ...

        Using equivalent ``expressions``:

        .. code-block:: yaml

            transform: expressions
            output:
                green:
                    formula: green
                    dtype: float32

                # copy unaffected other bands
                red: red
                blue: blue
            input: ...

    :param apply_to: list of names of measurements to apply conversion to
    :param dtype: default ``dtype`` for conversion
    """
    def __init__(self, apply_to=None, dtype='float32'):
        warnings.warn("the `to_float` transform is deprecated, please use `expressions` instead",
                      category=DeprecationWarning)
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

    .. note::

        The ``rename`` transform is deprecated. Please use ``expressions`` instead.

        Using ``rename``:

        .. code-block:: yaml

            transform: rename
            measurement_names:
                green: verde
            input: ...

        Using equivalent ``expressions``:

        .. code-block:: yaml

            transform: expressions
            output:
                verde: green

                # copy other unaffected bands
                red: red
                blue: blue
            input: ...

    :param measurement_names: mapping from INPUT NAME to OUTPUT NAME
    """
    def __init__(self, measurement_names):
        warnings.warn("the `rename` transform is deprecated, please use `expressions` instead",
                      category=DeprecationWarning)
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

    .. note::

        The ``select`` transform is deprecated. Please use ``expressions`` instead.

        Using ``select``:

        .. code-block:: yaml

            transform: select
            measurement_names: [green]
            input: ...

        Using equivalent ``expressions``:

        .. code-block:: yaml

            transform: expressions
            output:
                green: green
            input: ...

    :param measurement_names: list of measurements to keep
    """
    def __init__(self, measurement_names):
        warnings.warn("the `select` transform is deprecated, please use `expressions` instead",
                      category=DeprecationWarning)
        self.measurement_names = measurement_names

    def measurements(self, input_measurements):
        return {key: value
                for key, value in input_measurements.items()
                if key in self.measurement_names}

    def compute(self, data):
        return data.drop_vars([measurement
                               for measurement in data.data_vars
                               if measurement not in self.measurement_names])


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

        def deduce_type(output_var, output_desc):
            if 'dtype' in output_desc:
                return numpy.dtype(output_desc['dtype'])

            formula = output_desc['formula']
            result = evaluate_type(formula, input_measurements, parser, FormulaEvaluator)

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

        def result(output_var, output_desc):
            # pylint: disable=invalid-unary-operand-type

            if isinstance(output_desc, str):
                # copy measurement over
                return data[output_desc]

            nodata = output_desc.get('nodata')
            dtype = output_desc.get('dtype')

            formula = output_desc['formula']
            result = evaluate_data(formula, data, parser, FormulaEvaluator)
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
            mask = evaluate_nodata_mask(formula, data, parser, MaskEvaluator)

            if numpy.dtype(dtype) == bool:
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


def fiscal_year(time):
    """"
    This function supports group-by financial years
    """
    def convert_to_quarters(x):
        df = pd.Series(x)
        return df.apply(lambda x: numpy.datetime64(str(x.to_period('Q-JUN').qyear))).values

    ds = xarray.apply_ufunc(convert_to_quarters,
                            time,
                            input_core_dims=[["time"]],
                            output_core_dims=[["time"]],
                            vectorize=True)

    df = time['time'].to_series()
    years = df.apply(lambda x: numpy.datetime64(str(x.to_period('Q-JUN').qyear))).values
    ds = ds.assign_coords({"time": years})

    return ds


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

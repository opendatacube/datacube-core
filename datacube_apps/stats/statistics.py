"""
Functions for performing statistical data analysis.
"""
from __future__ import absolute_import

import collections
from collections import OrderedDict

import numpy
from functools import reduce as reduce_, partial
from operator import mul as mul_op

import xarray

try:
    from bottleneck import anynan, nansum
except ImportError:
    nansum = numpy.nansum

    def anynan(x, axis=None):
        return numpy.isnan(x).any(axis=axis)


class StatsConfigurationError(RuntimeError):
    pass


def argnanmedoid(x, axis=1):
    """
    Return the indices of the medoid

    :param x: input array
    :param axis: axis to medoid along
    :return: indices of the medoid
    """
    if axis == 0:
        x = x.T

    invalid = anynan(x, axis=0)
    band, time = x.shape
    diff = x.reshape(band, time, 1) - x.reshape(band, 1, time)
    dist = numpy.sqrt(numpy.sum(diff * diff, axis=0))  # dist = numpy.linalg.norm(diff, axis=0) is slower somehow...
    dist_sum = nansum(dist, axis=0)
    dist_sum[invalid] = numpy.inf
    i = numpy.argmin(dist_sum)

    return i


def nanmedoid(x, axis=1):
    i = argnanmedoid(x, axis)

    return x[:, i]


def combined_var_reduction(dataset, method, dim='time', keep_attrs=True):
    """
    Apply a reduction to a dataset by combining data variables into a single ndarray, running `method`, then
    un-combining to separate data variables.

    :param dataset: Input `xarray.Dataset`
    :param method: function to apply to DataArray
    :param bool keep_attrs: Should dataset attributes be retained, defaults to True.
    :param dim: Dimension to apply reduction along
    :return: xarray.Dataset with same data_variables but one less dimension
    """
    flattened = dataset.to_array(dim='variable')

    hdmedian_out = flattened.reduce(_array_hdmedian, dim=dim, keep_attrs=keep_attrs, method=method)

    hdmedian_out = hdmedian_out.to_dataset(dim='variable')

    if keep_attrs:
        for k, v in dataset.attrs.items():
            hdmedian_out.attrs[k] = v

    return hdmedian_out


def _array_hdmedian(inarray, method, axis=1, **kwargs):
    """
    Apply cross band reduction across time for each x/y coordinate in a 4-D nd-array

    ND-Array is expected to have dimensions of (bands, time, y, x)

    :param inarray:
    :param method:
    :param axis:
    :param kwargs:
    :return:
    """
    if len(inarray.shape) != 4:
        raise ValueError("Can only operate on 4-D arrays")
    if axis != 1:
        raise ValueError("Reduction axis must be 1")

    variable, time, y, x = inarray.shape
    output = numpy.empty((variable, y, x), dtype='float64')
    for iy in range(y):
        for ix in range(x):
            try:
                output[:, iy, ix] = method(inarray[:, :, iy, ix])
            except ValueError:
                output[:, iy, ix] = numpy.nan
    return output


def prod(a):
    """Product of a sequence"""
    return reduce_(mul_op, a, 1)


def _blah(shape, step=1, dtype=None):
    return numpy.arange(0, prod(shape) * step, step, dtype=dtype).reshape(shape)


def axisindex(a, index, axis=0):
    """
    Index array 'a' using 'index' as depth along 'axis'
    """
    shape = index.shape
    lshape = shape[:axis] + (1,) * (index.ndim - axis)
    rshape = (1,) * axis + shape[axis:]
    step = prod(shape[axis:])
    idx = _blah(lshape, step * a.shape[axis]) + _blah(rshape) + index * step
    return a.take(idx)


def argpercentile(a, q, axis=0):
    """
    Compute the index of qth percentile of the data along the specified axis.
    Returns the index of qth percentile of the array elements.
    Parameters
    ----------
    a : array_like
        Input array or object that can be converted to an array.
    q : float in range of [0,100] (or sequence of floats)
        Percentile to compute which must be between 0 and 100 inclusive.
    axis : int or sequence of int, optional
        Axis along which the percentiles are computed. The default is 0.
    """
    q = numpy.array(q, dtype=numpy.float64, copy=True) / 100.0
    nans = numpy.isnan(a).sum(axis=axis)
    q = q.reshape(q.shape + (1,) * nans.ndim)
    index = numpy.round(q * (a.shape[axis] - 1 - nans)).astype(numpy.int32)
    # NOTE: assuming nans are gonna sort larger than everything else
    return axisindex(numpy.argsort(a, axis=axis), index, axis=axis)


def nan_percentile(arr, q, axis=0):
    """
    Return requested percentile(s) of a 3D array, ignoring NaNs

    For the case of 3D->2D reductions, this function is ~200x faster than numpy.nanpercentile()

    See http://krstn.eu/np.nanpercentile()-there-has-to-be-a-faster-way/ for further explanation

    :param np.ndarray arr:
    :param q: number between 0-100, or list of numbers between 0-100
    :param int axis: must be zero, for compatibility with :meth:`xarray.Dataset.reduce`
    """
    if axis != 0:
        raise ValueError('This function only works with axis=0')

    # valid (non NaN) observations along the first axis
    valid_obs = numpy.sum(numpy.isfinite(arr), axis=0)
    # replace NaN with maximum
    max_val = numpy.nanmax(arr)
    arr[numpy.isnan(arr)] = max_val
    # sort - former NaNs will move to the end
    arr = numpy.sort(arr, axis=0)

    # loop over requested quantiles
    if isinstance(q, collections.Sequence):
        qs = []
        qs.extend(q)
    else:
        qs = [q]
    if len(qs) < 2:
        quant_arr = numpy.zeros(shape=(arr.shape[1], arr.shape[2]))
    else:
        quant_arr = numpy.zeros(shape=(len(qs), arr.shape[1], arr.shape[2]))

    result = []
    for quant in qs:
        # desired position as well as floor and ceiling of it
        k_arr = (valid_obs - 1) * (quant / 100.0)
        f_arr = numpy.floor(k_arr).astype(numpy.int32)
        c_arr = numpy.ceil(k_arr).astype(numpy.int32)
        fc_equal_k_mask = f_arr == c_arr

        # linear interpolation (like numpy percentile) takes the fractional part of desired position
        floor_val = axisindex(a=arr, index=f_arr) * (c_arr - k_arr)
        ceil_val = axisindex(a=arr, index=c_arr) * (k_arr - f_arr)

        quant_arr = floor_val + ceil_val
        # if floor == ceiling take floor value
        quant_arr[fc_equal_k_mask] = axisindex(a=arr, index=k_arr.astype(numpy.int32))[fc_equal_k_mask]

        result.append(quant_arr)

    if len(result) == 1:
        return result[0]
    else:
        return result


class ValueStat(object):
    """
    Holder class describing the outputs of a statistic and how to calculate it

    :param stat_func: callable to compute statistics
    :param bool masked: whether to apply masking to the input data
    """

    def __init__(self, stat_func, masked=True):
        self.masked = masked
        self.stat_func = stat_func

    def compute(self, data):
        """
        Compute a statistic on the given Dataset.

        :param xarray.Dataset data:
        :return: xarray.Dataset
        """
        return self.stat_func(data)

    @staticmethod
    def measurements(input_measurements):
        """
        Turn a list of input measurements into a list of output measurements.

        :param input_measurements:
        :rtype: list(dict)
        """
        return [
            {attr: measurement[attr] for attr in ['name', 'dtype', 'nodata', 'units']}
            for measurement in input_measurements]

    @classmethod
    def from_stat_name(cls, name, masked=True, **kwargs):
        """
        A value returning statistic, relying on an xarray function of `name` being available

        :param name: The name of an `xarray.Dataset` statistical function
        :param masked:
        :return:
        """
        return cls(masked=masked,
                   stat_func=partial(getattr(xarray.Dataset, name), dim='time', **kwargs))


class WofsStats(object):
    def __init__(self):
        self.masked = True

    @staticmethod
    def compute(data):
        wet = (data.water == 128).sum(dim='time')
        dry = (data.water == 0).sum(dim='time')
        clear = wet + dry
        frequency = wet / clear
        return xarray.Dataset({'count_wet': wet,
                               'count_clear': clear,
                               'frequency': frequency}, attrs=dict(crs=data.crs))

    @staticmethod
    def measurements(input_measurements):
        measurement_names = set(m['name'] for m in input_measurements)
        assert 'water' in measurement_names
        return [
            {
                'name': 'count_wet',
                'dtype': 'int16',
                'nodata': -1,
                'units': '1'
            },
            {
                'name': 'count_clear',
                'dtype': 'int16',
                'nodata': -1,
                'units': '1'
            },
            {
                'name': 'frequency',
                'dtype': 'float32',
                'nodata': -1,
                'units': '1'
            },

        ]


class NormalisedDifferenceStats(object):
    """
    Simple NDVI/NDWI and other Normalised Difference stats

    Computes (band1 - band2)/(band1 + band2), and then summarises using the list of `stats` into
    separate output variables.
    """

    def __init__(self, band1, band2, name, stats=None, masked=True):
        self.stats = stats if stats else ['min', 'max', 'mean']
        self.band1 = band1
        self.band2 = band2
        self.name = name
        self.masked = masked

    def compute(self, data):
        nd = (data[self.band1] - data[self.band2]) / (data[self.band1] + data[self.band2])
        outputs = {}
        for stat in self.stats:
            name = '_'.join([self.name, stat])
            outputs[name] = getattr(nd, stat)(dim='time')
        return xarray.Dataset(outputs,
                              attrs=dict(crs=data.crs))

    def measurements(self, input_measurements):
        measurement_names = [m['name'] for m in input_measurements]
        if self.band1 not in measurement_names or self.band2 not in measurement_names:
            raise StatsConfigurationError('Input measurements for %s must include "%s" and "%s"',
                                          self.name, self.band1, self.band2)

        return [dict(name='_'.join([self.name, stat]), dtype='float32', nodata=-1, units='1')
                for stat in self.stats]


class IndexStat(ValueStat):
    def __init__(self, stat_func, masked=True):
        super(IndexStat, self).__init__(stat_func, masked)

    def compute(self, data):
        index = super(IndexStat, self).compute(data)

        def index_dataset(var):
            return axisindex(data.data_vars[var.name].values, var.values)

        data_values = index.apply(index_dataset)
        return data_values

    @staticmethod
    def measurements(input_measurements):
        return ValueStat.measurements(input_measurements)


class PerBandIndexStat(ValueStat):
    """
    Each output variable contains values that actually exist in the input data.

    It uses a function that returns the indexes of these values, then pulls them out of the source data,
    along with provenance information.

    :param stat_func: A function which takes an xarray.Dataset and returns an xarray.Dataset of indexes
    """

    def __init__(self, stat_func, masked=True):
        super(PerBandIndexStat, self).__init__(stat_func, masked)

    def compute(self, data):
        index = super(PerBandIndexStat, self).compute(data)

        def index_dataset(var):
            return axisindex(data.data_vars[var.name].values, var.values)

        data_values = index.apply(index_dataset)

        def index_time(var):
            return data.time.values[var.values]

        time_values = index.apply(index_time).rename(OrderedDict((name, name + '_observed')
                                                                 for name in index.data_vars))

        text_values = time_values.apply(_datetime64_to_inttime).rename(OrderedDict((name, name + '_date')
                                                                                   for name in time_values.data_vars))

        def index_source(var):
            return data.source.values[var.values]

        time_values = index.apply(index_source).rename(OrderedDict((name, name + '_source')
                                                                   for name in index.data_vars))

        return xarray.merge([data_values, time_values, text_values])

    @staticmethod
    def measurements(input_measurements):
        index_measurements = [
            {
                'name': measurement['name'] + '_source',
                'dtype': 'int8',
                'nodata': -1,
                'units': '1'
            }
            for measurement in input_measurements
            ]
        date_measurements = [
            {
                'name': measurement['name'] + '_observed',
                'dtype': 'float64',
                'nodata': 0,
                'units': 'seconds since 1970-01-01 00:00:00'
            }
            for measurement in input_measurements
            ]
        text_measurements = [
            {
                'name': measurement['name'] + '_observed_date',
                'dtype': 'int32',
                'nodata': 0,
                'units': 'Date as YYYYMMDD'
            }
            for measurement in input_measurements
            ]

        return ValueStat.measurements(input_measurements) + date_measurements + index_measurements + text_measurements


class PerStatIndexStat(ValueStat):
    """
    :param stat_func: A function which takes an xarray.Dataset and returns an xarray.Dataset of indexes
    """

    def __init__(self, stat_func, masked=True):
        super(PerStatIndexStat, self).__init__(stat_func, masked)

    def compute(self, data):
        index = super(PerStatIndexStat, self).compute(data)

        def index_dataset(var, axis):
            return axisindex(var, index, axis=axis)

        data_values = data.reduce(index_dataset, dim='time')
        observed = data.time.values[index]
        data_values['observed'] = (('y', 'x'), observed)
        data_values['observed_date'] = (('y', 'x'), _datetime64_to_inttime(observed))
        data_values['source'] = (('y', 'x'), data.source.values[index])

        return data_values

    @staticmethod
    def measurements(input_measurements):
        index_measurements = [
            {
                'name': 'source',
                'dtype': 'int8',
                'nodata': -1,
                'units': '1'
            }
        ]
        date_measurements = [
            {
                'name': 'observed',
                'dtype': 'float64',
                'nodata': 0,
                'units': 'seconds since 1970-01-01 00:00:00'
            }
        ]
        text_measurements = [
            {
                'name': 'observed_date',
                'dtype': 'int32',
                'nodata': 0,
                'units': 'Date as YYYYMMDD'
            }
        ]
        return ValueStat.measurements(input_measurements) + date_measurements + index_measurements + text_measurements


def compute_medoid(data):
    flattened = data.to_array(dim='variable')
    variable, time, y, x = flattened.shape
    index = numpy.empty((y, x), dtype='int64')
    # TODO: nditer?
    for iy in range(y):
        for ix in range(x):
            index[iy, ix] = argnanmedoid(flattened.values[:, :, iy, ix])
    return index


def percentile_stat(q):
    return PerBandIndexStat(masked=True,
                            # pylint: disable=redundant-keyword-arg
                            stat_func=partial(getattr(xarray.Dataset, 'reduce'),
                                              dim='time',
                                              func=argpercentile,
                                              q=q))


def percentile_stat_no_prov(q):
    return IndexStat(masked=True,
                     # pylint: disable=redundant-keyword-arg
                     stat_func=partial(getattr(xarray.Dataset, 'reduce'),
                                       dim='time',
                                       func=argpercentile,
                                       q=q))


def _datetime64_to_inttime(var):
    """
    Return an "inttime" representing a datetime64.

    For example, 2016-09-29 as an "inttime" would be 20160929

    :param var: datetime64
    :return: int representing the given time
    """
    values = getattr(var, 'values', var)
    years = values.astype('datetime64[Y]').astype('int32') + 1970
    months = values.astype('datetime64[M]').astype('int32') % 12 + 1
    days = (values.astype('datetime64[D]') - values.astype('datetime64[M]') + 1).astype('int32')
    return years * 10000 + months * 100 + days

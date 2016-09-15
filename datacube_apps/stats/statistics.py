"""
Useful tools for performing data analysis
"""
from __future__ import absolute_import

import collections
import numpy

try:
    from bottleneck import anynan, nansum
except ImportError:
    nansum = numpy.nansum


    def anynan(x, axis=None):
        return numpy.isnan(x).any(axis=axis)


def nanmedoid(x, axis=1, return_index=False):
    if axis == 0:
        x = x.T

    invalid = anynan(x, axis=0)
    band, time = x.shape
    diff = x.reshape(band, time, 1) - x.reshape(band, 1, time)
    dist = numpy.sqrt(numpy.sum(diff * diff, axis=0))  # dist = numpy.linalg.norm(diff, axis=0) is slower somehow...
    dist_sum = nansum(dist, axis=0)
    dist_sum[invalid] = numpy.inf
    i = numpy.argmin(dist_sum)

    return (x[:, i], i) if return_index else x[:, i]


def apply_cross_measurement_reduction(dataset, method=nanmedoid, dim='time', keep_attrs=True):
    """
    Apply a cross measurement reduction (like medioid) to an xarray dataset

    :param dataset: Input `xarray.Dataset`
    :param method: function to apply. Defaults to nanmedoid
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


def _zvalue_from_index(arr, ind):
    """
    Private helper function to work around the limitation of np.choose() by employing np.take()
    arr has to be a 3D array
    ind has to be a 2D array containing values for z-indicies to take from arr
    See: http://stackoverflow.com/a/32091712/4169585
    This is faster and more memory efficient than using the ogrid based solution with fancy indexing.
    """
    # get number of columns and rows
    _, n_c, n_r = arr.shape

    # get linear indices and extract elements with np.take()
    idx = n_c * n_r * ind + n_r * numpy.arange(n_r)[:, None] + numpy.arange(n_c)
    return numpy.take(arr, idx)


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
        floor_val = _zvalue_from_index(arr=arr, ind=f_arr) * (c_arr - k_arr)
        ceil_val = _zvalue_from_index(arr=arr, ind=c_arr) * (k_arr - f_arr)

        quant_arr = floor_val + ceil_val
        quant_arr[fc_equal_k_mask] = _zvalue_from_index(arr=arr, ind=k_arr.astype(numpy.int32))[
            fc_equal_k_mask]  # if floor == ceiling take floor value

        result.append(quant_arr)

    if len(result) == 1:
        return result[0]
    else:
        return result

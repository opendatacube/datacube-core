"""
Useful tools for performing data analysis
"""
from __future__ import absolute_import

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

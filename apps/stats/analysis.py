"""
Useful tools for performing data analysis
"""
from __future__ import absolute_import

import numpy as np
try:
    from hdmedians import nanmedoid
except ImportError:  #
    try:
        import bottleneck as bn
    except ImportError:
        bn = np

    def nanmedoid(x, axis=1, return_index=False):
        def naneuclidean(x, y):
            return np.sqrt(bn.nansum(np.square(x - y)))

        if axis == 0:
            x = x.T

        p, n = x.shape
        d = np.empty(n)
        for i in range(n):
            if np.isnan(x[0, i]):
                d[i] = np.nan
            else:
                d[i] = bn.nansum([naneuclidean(x[:, i], x[:, j])
                                  for j in range(n) if j != i])

        i = bn.nanargmin(d)

        if return_index:
            return (x[:, i], i)
        else:
            return x[:, i]


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
    output = np.empty((variable, y, x), dtype='float64')
    for iy in range(y):
        for ix in range(x):
            try:
                output[:, iy, ix] = method(inarray[:, :, iy, ix])
            except ValueError:
                output[:, iy, ix] = np.nan
    return output

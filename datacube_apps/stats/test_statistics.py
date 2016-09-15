"""
Tests for the custom statistics functions

"""
from __future__ import absolute_import

from .statistics import nan_percentile, argpercentile
import numpy


def test_nan_percentile():
    # create array of shape(5,100,100) - image of size 100x100 with 5 layers
    test_arr = numpy.random.randint(0, 10000, 50000).reshape(5, 100, 100).astype(numpy.float32)
    numpy.random.shuffle(test_arr)
    # place random NaNs
    random_nans = numpy.random.randint(0, 50000, 500).astype(numpy.float32)
    for r in random_nans:
        test_arr[test_arr == r] = numpy.NaN

    # Test with single q
    q = 45
    input_arr = numpy.array(test_arr, copy=True)
    std_np_func = numpy.nanpercentile(input_arr, q=q, axis=0)
    new_func = nan_percentile(input_arr, q=q)

    assert numpy.allclose(std_np_func, new_func)

    # Test with all qs
    qs = range(0, 100)
    input_arr = numpy.array(test_arr, copy=True)
    std_np_func = numpy.nanpercentile(input_arr, q=qs, axis=0)
    new_func = nan_percentile(input_arr, q=qs)

    assert numpy.allclose(std_np_func, new_func)


def test_argpercentile():
    stack = numpy.full((20, 20, 20), numpy.nan)
    stack[10:, ...] = numpy.arange(10).reshape(10, 1, 1)

    index = argpercentile(stack, 10, axis=0)
    indices = numpy.indices(stack.shape[1:])
    index = (index,) + tuple(indices)

    assert numpy.all(stack[index] == 1)

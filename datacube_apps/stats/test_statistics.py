"""
Tests for the custom statistics functions

"""
from __future__ import absolute_import

from .statistics import nan_percentile, argpercentile, axisindex
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
    test_arr = numpy.random.randint(0, 10000, 50000).reshape(5, 100, 100).astype(numpy.float32)
    numpy.random.shuffle(test_arr)
    # place random NaN
    rand_nan = numpy.random.randint(0, 50000, 500).astype(numpy.float32)
    for r in rand_nan:
        test_arr[test_arr == r] = numpy.NaN

    npres = numpy.nanpercentile(test_arr, q=25, axis=0, interpolation='nearest')
    gregres = axisindex(test_arr, argpercentile(test_arr, q=25, axis=0), axis=0)
    assert numpy.isclose(npres, gregres).all()

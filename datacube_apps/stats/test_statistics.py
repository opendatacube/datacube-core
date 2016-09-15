"""
Tests for the custom statistics functions module

"""
from __future__ import absolute_import

from .statistics import nan_percentile
import numpy as np


def test_nan_percentile():
    # create array of shape(5,100,100) - image of size 100x100 with 5 layers
    test_arr = np.random.randint(0, 10000, 50000).reshape(5, 100, 100).astype(np.float32)
    np.random.shuffle(test_arr)
    # place random NaNs
    random_nans = np.random.randint(0, 50000, 500).astype(np.float32)
    for r in random_nans:
        test_arr[test_arr == r] = np.NaN

    # Test with single q
    q = 45
    input_arr = np.array(test_arr, copy=True)
    std_np_func = np.nanpercentile(input_arr, q=q, axis=0)
    new_func = nan_percentile(input_arr, q=q)

    assert np.allclose(std_np_func, new_func)

    # Test with all qs
    q = range(0, 100)
    input_arr = np.array(test_arr, copy=True)
    std_np_func = np.nanpercentile(input_arr, q=q, axis=0)
    new_func = nan_percentile(input_arr, q=q)

    assert np.allclose(std_np_func, new_func)

# coding=utf-8
"""
Functions for creating a lazy-loaded array from storage units.
"""
from __future__ import absolute_import

import itertools
import operator
import collections
import functools

import dask
import dask.array as da
import numpy


def get_dask_array(storage_units, var_name, dimensions, dim_props, is_fake_array=False):
    """
    Create an xarray.DataArray
    :return xarray.DataArray
    """
    dsk_id = var_name  # unique name for the requested dask
    dsk = {}
    if not is_fake_array:
        dsk = _get_dask_for_storage_units(storage_units, var_name, dimensions, dim_props['dim_vals'], dsk_id)
        _fill_in_dask_blanks(dsk, storage_units, var_name, dimensions, dim_props, dsk_id)

    dtype = storage_units[0].variables[var_name].dtype
    chunks = tuple(tuple(dim_props['sus_size'][dim]) for dim in dimensions)
    dask_array = da.Array(dsk, dsk_id, chunks, dtype=dtype)
    return dask_array


def _get_dask_for_storage_units(storage_units, var_name, dimensions, dim_vals, dsk_id):
    dsk = {}
    for storage_unit in storage_units:
        dsk_index = (dsk_id,)   # Dask is indexed by a tuple of ("Name", x-index pos, y-index pos, z-index pos, ...)
        for dim in dimensions:
            ordinal = dim_vals[dim].index(storage_unit.coordinates[dim].begin)
            dsk_index += (ordinal,)
        # TODO: Wrap in a chunked dask for sub-file dask chunks
        dsk[dsk_index] = (storage_unit.get_chunk, var_name, Ellipsis)
        #dsk[dsk_index] = (_get_chunked_data_func, storage_unit, var_name)
    return dsk


def _fill_in_dask_blanks(dsk, storage_units, var_name, dimensions, dim_props, dsk_id):
    all_dsk_keys = set(itertools.product((dsk_id,), *[[i for i, _ in enumerate(dim_props['dim_vals'][dim])]
                                                      for dim in dimensions]))
    missing_dsk_keys = all_dsk_keys - set(dsk.keys())

    if missing_dsk_keys:
        dtype, nodata = _nodata_properties(storage_units, var_name)
        for key in missing_dsk_keys:
            shape = _get_chunk_shape(key, dimensions, dim_props['sus_size'])
            dsk[key] = (_no_data_block, shape, dtype, nodata)
        return dsk


def _nodata_properties(storage_units, var_name):
    sample = storage_units[0]
    dtype = sample.variables[var_name].dtype
    nodata = sample.variables[var_name].nodata
    return dtype, nodata


def _get_chunk_shape(key, dimensions, chunksize):
    coords = list(key)[1:]
    shape = tuple(operator.getitem(chunksize[dim], i) for dim, i in zip(dimensions, coords))
    return shape


def _no_data_block(shape, dtype, fill):
    arr = numpy.empty(shape, dtype)
    if fill is None:
        fill = numpy.NaN
    arr.fill(fill)
    return arr


# def _get_chunked_data_func(storage_unit, var_name):
#     storage_array = NDArrayProxy(storage_unit, var_name)
#     # TODO: Chunk along chunk direction
#     chunks = (1000,) * storage_array.ndim
#     return da.from_array(storage_array, chunks=chunks)
#
#
# # TODO: Move into storage.access.StorageUnitBase
# class NDArrayProxy(object):
#     def __init__(self, storage_unit, var_name):
#         self._storage_unit = storage_unit
#         self._var_name = var_name
#
#     @property
#     def ndim(self):
#         return len(self._storage_unit.variables[self._var_name].dimensions)
#
#     @property
#     def size(self):
#         return functools.reduce(operator.mul, self.shape)
#
#     @property
#     def dtype(self):
#         return self._storage_unit.variables[self._var_name].dtype
#
#     @property
#     def shape(self):
#         dims = self._storage_unit.variables[self._var_name].dimensions
#         return tuple(self._storage_unit.coordinates[dim].length for dim in dims)
#
#     def __len__(self):
#         return self.shape[0]
#
#     def __array__(self, dtype=None):
#         x = self[...]
#         if dtype and x.dtype != dtype:
#             x = x.astype(dtype)
#         if not isinstance(x, numpy.ndarray):
#             x = numpy.array(x)
#         return x
#
#     def __getitem__(self, key):
#         # if not isinstance(key, collections.Iterable):
#         #     # dealing with a single value
#         #     if not isinstance(key, slice):
#         #         key = slice(key, key + 1)
#         #     key = [key]
#         # if len(key) < self.ndim:
#         #     key = [key[i] if i < len(key) else slice(0,self.shape[i]) for i in range(self.ndim)]
#
#         return self._storage_unit.get_chunk(self._var_name, key)
#
#     def __repr__(self):
#         return '%s(array=%r)' % (type(self).__name__, self.shape)

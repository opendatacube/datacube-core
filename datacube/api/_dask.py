# coding=utf-8
"""
Functions for creating a lazy-loaded array from storage units.
"""
from __future__ import absolute_import

import itertools
import operator
import uuid

import numpy
import dask.array as da


def get_dask_array(storage_units, var_name, dimensions, dim_props):
    """
    Create an xarray.DataArray
    :return xarray.DataArray
    """
    dsk_id = str(uuid.uuid1())  # unique name for the requested dask
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
        dsk[dsk_index] = (storage_unit.get, var_name)
        # dsk[dsk_index] = (get_chunked_data_func, storage_unit, var_name)
    return dsk


def _fill_in_dask_blanks(dsk, storage_units, var_name, dimensions, dim_props, dsk_id):
    nodata_dsk = _make_nodata_func(storage_units, var_name, dimensions, dim_props['sus_size'])

    all_dsk_keys = set(itertools.product((dsk_id,), *[[i for i, _ in enumerate(dim_props['dim_vals'][dim])]
                                                      for dim in dimensions]))
    missing_dsk_keys = all_dsk_keys - set(dsk.keys())

    for key in missing_dsk_keys:
        dsk[key] = nodata_dsk(key)
    return dsk


def _make_nodata_func(storage_units, var_name, dimensions, chunksize):
    sample = storage_units[0]
    dtype = sample.variables[var_name].dtype
    nodata = sample.variables[var_name].nodata

    def make_nodata_dask(key):
        coords = list(key)[1:]
        shape = tuple(operator.getitem(chunksize[dim], i) for dim, i in zip(dimensions, coords))
        return no_data_block, shape, dtype, nodata

    return make_nodata_dask


def no_data_block(shape, dtype, fill):
    arr = numpy.empty(shape, dtype)
    if fill is None:
        fill = numpy.NaN
    arr.fill(fill)
    return arr


# def get_chunked_data_func(storage_unit, var_name):
#     # TODO: Provide dask array to chunked NetCDF calls
#     return NDArrayProxy(storage_unit, var_name)


# TODO: Move into storage.access.StorageUnitBase
# class NDArrayProxy(object):
#     def __init__(self, storage_unit, var_name):
#         self._storage_unit = storage_unit
#         self._var_name = var_name
#
#     @property
#     def ndim(self):
#         return len(self._storage_unit.coordinates)
#
#     @property
#     def size(self):
#         return functools.reduce(operator.mul, [coord.length for coord in self._storage_unit.coordinates])
#
#     @property
#     def dtype(self):
#         return self._storage_unit.variables[self._var_name].dtype
#
#     @property
#     def shape(self):
#         return tuple(coord.length for coord in self._storage_unit.coordinates)
#
#     def __len__(self):
#         return self.shape[0]
#
#     def __array__(self, dtype=None):
#         return self._storage_unit.get(self._var_name)
#
#     def __getitem__(self, key):
#         return self._storage_unit.get_chunk(self._var_name, key)
#
#     def __repr__(self):
#         return '%s(array=%r)' % (type(self).__name__, self.shape)

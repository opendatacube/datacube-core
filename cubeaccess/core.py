#    Copyright 2015 Geoscience Australia
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.


from __future__ import absolute_import, division, print_function
from collections import namedtuple

from builtins import *
from functools import reduce
import numpy

from .utils import merge_unique
from .indexing import make_index, index_shape

try:
    from xray import DataArray
except ImportError:
    from .ghetto import DataArray

Range = namedtuple('Range', ('begin', 'end'))
Coordinate = namedtuple('Coordinate', ('dtype', 'begin', 'end', 'length'))
Variable = namedtuple('Variable', ('dtype', 'ndv', 'coordinates'))


def is_consistent_coords(coord1, coord2):
    return coord1.dtype == coord2.dtype and (coord1.begin > coord1.end) == (coord2.begin > coord2.end)


def comp_dict(d1, d2, p):
    return len(d1) == len(d2) and all(k in d2 and p(d1[k], d2[k]) for k in d1)


def is_consistent_coord_set(coords1, coords2):
    return comp_dict(coords1, coords2, is_consistent_coords)


def check_storage_unit_set_consistent(storage_units):
    first_coord = storage_units[0].coordinates
    all_vars = dict()

    for su in storage_units:
        if not is_consistent_coord_set(first_coord, su.coordinates):
            raise RuntimeError("inconsistent dimensions")

        for var in all_vars:
            if var in su.variables and all_vars[var] != su.variables[var]:
                raise RuntimeError("inconsistent variables")

        all_vars.update(su.variables)


class StorageUnitBase(object):
    def _get_coord(self, name):
        raise NotImplementedError()

    def _fill_data(self, name, index, dest):
        raise NotImplementedError()

    def get(self, name, dest=None, **kwargs):
        var = self.variables[name]
        coords = [self._get_coord(dim) for dim in var.coordinates]
        index = tuple(make_index(coord, kwargs.get(dim)) for coord, dim in zip(coords, var.coordinates))
        shape = index_shape(index)

        if not dest:
            dest = numpy.empty(shape, dtype=var.dtype)
            # dest.fill(var.ndv)
        assert(all(a <= b for a, b in zip(shape, dest.shape)))
        # TODO: subindex dest
        self._fill_data(name, index, dest)

        return DataArray(dest, coords=[coord[idx] for coord, idx in zip(coords, index)], dims=var.coordinates)


class StorageUnitDimensionProxy(StorageUnitBase):
    def __init__(self, storage_unit, *coords):
        self._storage_unit = storage_unit
        self._dimensions = tuple(name for name, value in coords)
        self.coordinates = {name: Coordinate(getattr(value, 'dtype', numpy.dtype(type(value))), value, value, 1) for name, value in coords}
        self.coordinates.update(storage_unit.coordinates)

        def expand_var(var):
            return Variable(var.dtype, var.ndv, self._dimensions + var.coordinates)
        self.variables = {name: expand_var(var) for name, var in storage_unit.variables.items()}

    def _get_coord(self, name):
        if name in self._dimensions:
            value = self.coordinates[name].begin
            return numpy.array([value], dtype=self.coordinates[name].dtype)
        return self._storage_unit._get_coord(name)

    def _fill_data(self, name, index, dest):
        shape = index_shape(index)
        ndims = len(self._dimensions)
        if any(i == 0 for i in shape[:ndims]):
            return dest
        self._storage_unit._fill_data(name, index[ndims:], dest[(0,)*ndims])


class StorageUnitStack(StorageUnitBase):
    def __init__(self, storage_units, stack_dim):
        for a, b in zip(storage_units[:-1], storage_units[1:]):
            if a.coordinates[stack_dim].begin >= b.coordinates[stack_dim].begin:
                raise RuntimeError("source storage units must be sorted")
            if a.coordinates[stack_dim].end > b.coordinates[stack_dim].begin:
                raise RuntimeError("overlapping coordinates are not supported yet")

        StorageUnitStack.check_consistent(storage_units, stack_dim)

        stack_coord_data = merge_unique([su._get_coord(stack_dim) for su in storage_units])

        self._stack_dim = stack_dim
        self._storage_units = storage_units
        self._stack_coord_data = stack_coord_data
        self.coordinates = storage_units[0].coordinates.copy()
        self.coordinates[stack_dim] = Coordinate(stack_coord_data.dtype,
                                                 stack_coord_data[0],
                                                 stack_coord_data[-1],
                                                 len(stack_coord_data))
        self.variables = reduce(lambda a, b: a.update(b) or a, (su.variables for su in storage_units), {})

    def _get_coord(self, name):
        if name == self._stack_dim:
            return self._stack_coord_data
        return self._storage_units[0]._get_coord(name)

    def _fill_data(self, name, index, dest):
        idx = 0
        for su in self._storage_units:
            length = su.coordinates[self._stack_dim].length
            if idx < index[0].stop and idx+length > index[0].start:
                # TODO: slice dest
                # TODO: make new index
                data_slice = slice(
                    max(0, index[0].start-idx),
                    min(length, index[0].stop-idx),
                    index[0].step)
                subindex = (data_slice,) + index[1:]
                dest_slice = slice(idx+data_slice.start-index[0].start, idx+data_slice.stop-index[0].start)
                su._fill_data(name, subindex, dest[dest_slice])
            idx += length
            if idx >= index[0].stop:
                break

    @staticmethod
    def check_consistent(storage_units, stack_dim):
        first_coord = storage_units[0].coordinates
        all_vars = dict()

        if stack_dim not in first_coord:
            raise KeyError("dimension to stack along is missing")

        for su in storage_units:
            if len(su.coordinates) != len(first_coord) or \
                    any(k not in su.coordinates or
                        su.coordinates[k] != first_coord[k] for k in first_coord if k != stack_dim):
                raise RuntimeError("inconsistent coordinates")

            for var in all_vars:
                if var in su.variables and all_vars[var] != su.variables[var]:
                    raise RuntimeError("inconsistent variables")

            all_vars.update(su.variables)

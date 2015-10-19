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

from .utils import coord2index, merge_unique

try:
    from xxray import DataArray
except ImportError:
    from .ghetto import DataArray

Coordinate = namedtuple('Coordinate', ['dtype', 'begin', 'end', 'length'])
Variable = namedtuple('Variable', ['dtype', 'ndv', 'coordinates'])


def _make_index(ndims):
    try:
        from rtree.index import Index, Property
        p = Property()
        p.dimension = ndims
        return Index(properties=p)

    except ImportError:
        from .ghetto import Index
        return Index()


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


class StorageUnitDimensionProxy(object):
    def __init__(self, storage_unit, *coords):
        self._storage_unit = storage_unit
        self._dimensions = tuple(name for name, value in coords)
        self.coordinates = {name: Coordinate(getattr(value, 'dtype', numpy.dtype(type(value))), value, value, 1) for name, value in coords}
        self.coordinates.update(storage_unit.coordinates)

        def expand_var(var):
            return Variable(var.dtype, var.ndv, self._dimensions + var.coordinates)
        self.variables = {name: expand_var(var) for name, var in storage_unit.variables.items()}

    def get(self, name, **kwargs):
        if name in self._dimensions:
            value = self.coordinates[name].begin
            if name in kwargs and (kwargs[name].start and kwargs[name].start > value or
                                   kwargs[name].stop and kwargs[name].stop < value):
                data = numpy.empty(0, dtype=self.coordinates[name].dtype)
            else:
                data = numpy.array([value], dtype=self.coordinates[name].dtype)
            return DataArray(data, coords=[data], dims=[name])

        if name in self.coordinates:
            return self._storage_unit.get(name, **kwargs)

        if name in self.variables:
            var = self.variables[name]
            coords = [self.get(dim, **kwargs).values for dim in self._dimensions]
            if any(coord.size == 0 for coord in coords):
                coords += [self._storage_unit.get(dim, **kwargs).values
                           for dim in self._storage_unit.variables[name].coordinates]
                shape = [coord.size for coord in coords]
                data = numpy.empty(shape, dtype=var.dtype)
            else:
                data = self._storage_unit.get(name, **kwargs)
                coords += [data.coords[dim] for dim in data.dims]
                shape = [coord.size for coord in coords]
                data = data.values.reshape(shape)
            return DataArray(data, coords=coords, dims=var.coordinates)

        raise KeyError(name + " is not a variable or coordinate")


class StorageUnitStack(object):
    def __init__(self, storage_units, stack_dim):
        def su_cmp(a, b):
            return cmp(a.coordinates[stack_dim].begin, b.coordinates[stack_dim].begin)

        storage_units = sorted(storage_units, su_cmp)
        StorageUnitStack.check_consistent(storage_units, stack_dim)

        for a, b in zip(storage_units[:-1], storage_units[1:]):
            if a.coordinates[stack_dim].end > b.coordinates[stack_dim].begin:
                raise RuntimeError("overlapping coordinates are not supported yet")

        stack_coord_data = merge_unique([su.get(stack_dim).values for su in storage_units])

        self._stack_dim = stack_dim
        self._storage_units = storage_units
        self._stack_coord_data = stack_coord_data
        self.coordinates = storage_units[0].coordinates.copy()
        self.coordinates[stack_dim] = Coordinate(stack_coord_data.dtype,
                                                 stack_coord_data[0],
                                                 stack_coord_data[-1],
                                                 len(stack_coord_data))
        self.variables = reduce(lambda a, b: a.update(b) or a, (su.variables for su in storage_units), {})

    def get(self, name, **kwargs):
        if name == self._stack_dim:
            data = self._stack_coord_data
            index = coord2index(data, kwargs.get(name, None))
            data = data[index]
            return DataArray(data, coords=[data], dims=[name])

        if name in self.coordinates:
            return self._storage_units[0].get(name, **kwargs)

        if name in self.variables:
            var = self.variables[name]
            # TODO: call get only on the relevant subset of storage units
            # TODO: use index version of get
            # TODO: use 'fill' version of get
            arrays = [su.get(name, **kwargs) for su in self._storage_units]
            coords = [numpy.concatenate([ar.coords[self._stack_dim] for ar in arrays], axis=0)] + \
                     [arrays[0].coords[dim] for dim in var.coordinates[1:]]

            data = numpy.concatenate([ar.values for ar in arrays], axis=0)
            assert(data.shape == tuple(coord.size for coord in coords))
            return DataArray(data, coords=coords, dims=var.coordinates)

        raise KeyError(name + " is not a variable or coordinate")



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


class StorageUnitSet(object):
    def __init__(self, storage_units):
        check_storage_unit_set_consistent(storage_units)
        self._build_unchecked(storage_units)

    def _build_unchecked(self, storage_units):
        self._storage_units = [sus for su in storage_units for sus in (su._storage_units if isinstance(su, StorageUnitSet) else [su])]
        first_coord = self._storage_units[0].coordinates
        self._dims = first_coord.keys()
        self.variables = {}

        self._index = _make_index(len(self._dims))
        for id_, su in enumerate(self._storage_units):
            coords = [min(su.coordinates[dim].begin, su.coordinates[dim].end) for dim in self._dims] \
                     + [max(su.coordinates[dim].begin, su.coordinates[dim].end) for dim in self._dims]
            self._index.insert(id_, coords)
            self.variables.update(su.variables)

        self.coordinates = {}
        self._coord_data = {}
        for idx, dim in enumerate(self._dims):
            begin = self._index.bounds[idx]
            end = self._index.bounds[idx + len(self._dims)]

            def density(coord): return (coord.length-1)/abs(coord.end - coord.begin)
            max_density = max(density(su.coordinates[dim]) for su in self._storage_units if su.coordinates[dim] > 1)

            self.coordinates[dim] = Coordinate(first_coord[dim].dtype,
                                               begin if first_coord[dim].begin <= first_coord[dim].end else end,
                                               end if first_coord[dim].begin <= first_coord[dim].end else begin,
                                               int((end-begin)*max_density+1.5))

    def get(self, name, precision=None, **kwargs):
        precision = precision or {}
        bounds = self._index.bounds[:]
        for idx, dim in enumerate(self._dims):
            if dim in kwargs:
                if kwargs[dim].start:
                    bounds[idx] = max(bounds[idx], kwargs[dim].start)
                if kwargs[dim].stop:
                    bounds[idx+len(self._dims)] = min(bounds[idx+len(self._dims)], kwargs[dim].stop)

        storage_units = [self._storage_units[idx] for idx in self._index.intersection(bounds)]

        if name in self.variables:
            var = self.variables[name]
            coords = [StorageUnitSet.merge_coordinate(dim, storage_units, precision.get(dim, 6))
                      for dim in var.coordinates]
            shape = [len(coord) for coord in coords]
            data = numpy.empty(shape, dtype=var.dtype)
            data.fill(var.ndv)

            def idx_from_coord(coord):
                result = slice(coord[0], coord[-1])
                if result.start > result.stop:
                    return slice(result.stop, result.start)
                return result

            for su in storage_units:
                su_data = su.get(name, **kwargs)
                coord_idx = [idx_from_coord(su_data.coords[dim]) for dim in su_data.dims]
                indexes = tuple(coord2index(coord, idx) for coord, idx in zip(coords, coord_idx))

                # if you're here you need to implement interleaved copy
                assert(all(idx.stop - idx.start == shape for idx, shape in zip(indexes, su_data.values.shape)))
                data[indexes] = su_data.values

            return DataArray(data, coords=coords, dims=var.coordinates)

        if name in self.coordinates:
            data = StorageUnitSet.merge_coordinate(name, storage_units, precision.get(name, 6), **kwargs)
            return DataArray(data, coords=[data], dims=[name])

        raise KeyError(name + " is not a variable or coordinate")

    @staticmethod
    def merge_coordinate(dim, storage_units, precision, **kwargs):
        coord = storage_units[0].coordinates[dim]
        return merge_unique([su.get(dim, **kwargs).values.round(precision) for su in storage_units],
                            reverse=coord.begin > coord.end)


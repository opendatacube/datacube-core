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
import numpy

from .utils import coord2index

try:
    from xxray import DataArray
except ImportError:
    from .ghetto import DataArray

Coordinate = namedtuple('Coordinate', ['dtype', 'begin', 'end', 'length'])
Variable = namedtuple('Variable', ['dtype', 'ndv', 'coordinates'])


def _make_index(nDims):
    try:
        from rtree.index import Index, Property
        p = Property()
        p.dimension = nDims
        return Index(properties=p)

    except ImportError:
        from .ghetto import Index
        return Index()


def check_storage_consistent(storage_units):
    first_coord = storage_units[0].coordinates
    all_vars = dict()

    for su in storage_units:
        if len(first_coord) != len(su.coordinates):
            raise RuntimeError("inconsistent dimensions")
        for dim in first_coord:
            coord = su.coordinates[dim]
            if dim not in su.coordinates:
                raise RuntimeError("inconsistent dimensions")
            if first_coord[dim].dtype != coord.dtype:
                raise RuntimeError("inconsistent dimensions")
            if (first_coord[dim].begin > first_coord[dim].end) != (coord.begin > coord.end):
                # if begin == end assume ascending order
                raise RuntimeError("inconsistent dimensions")

        for var in all_vars:
            if var in su.variables and all_vars[var] != su.variables[var]:
                raise RuntimeError("inconsistent variables")

        all_vars.update(su.variables)


def merge_unique(ars, kind='mergesort', reverse=False):
    c = numpy.concatenate(ars)
    c[::-1 if reverse else 1].sort(kind=kind)
    flag = numpy.ones(len(c), dtype=bool)
    numpy.not_equal(c[1:], c[:-1], out=flag[1:])
    return c[flag]


class StorageUnitSet(object):
    @staticmethod
    def merge_coordinate(dim, storage_units, precision, **kwargs):
        coord = storage_units[0].coordinates[dim]
        return merge_unique([su.get(dim, **kwargs).values.round(precision) for su in storage_units],
                            reverse=coord.begin > coord.end)

    def __init__(self, storage_units):
        check_storage_consistent(storage_units)
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

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
from builtins import *

from collections import namedtuple


Range = namedtuple('Range', ['begin', 'end'])
Coordinate = namedtuple('Coordinate', ['dtype', 'begin', 'end', 'length'])
Variable = namedtuple('Variable', ['dtype', 'ndv', 'coordinates'])

def _make_index(nDims):
    try:
        from rtree.index import Index, Property
        p = Property()
        p.dimension = nDims
        return Index(properties=p)

    except ImportError:
        class Index(object):
            def __init__(self):
                self.bounds = None

            def insert(self, id_, bounds):
                if self.bounds:
                    self.bounds = [min(self.bounds[i], bounds[i]) for i in xrange(len(self.bounds)//2)] \
                                + [max(self.bounds[len(self.bounds)//2+i], bounds[len(self.bounds)//2+i]) for i in xrange(len(self.bounds)//2)]
                else:
                    self.bounds = bounds

        return Index()


class ConcatDataset(object):
    def __init__(self, datasets):
        ConcatDataset._check_consistency(datasets)
        self._build_unchecked(datasets, {})

    @classmethod
    def _check_consistency(cls, datasets):
        first_coord = datasets[0].coordinates
        all_vars = dict()

        for ds in datasets:
            if len(first_coord) != len(ds.coordinates):
                raise RuntimeError("inconsistent dimensions")
            for dim in first_coord:
                coord = ds.coordinates[dim]
                if dim not in ds.coordinates:
                    raise RuntimeError("inconsistent dimensions")
                if first_coord[dim].dtype != coord.dtype:
                    raise RuntimeError("inconsistent dimensions")
                if (first_coord[dim].begin > first_coord[dim].end) != (coord.begin > coord.end):
                    # if begin == end assume ascending order
                    raise RuntimeError("inconsistent dimensions")

            for var in all_vars:
                if var in ds.variables and all_vars[var] != ds.variables[var]:
                    raise RuntimeError("inconsistent variables")

            all_vars.update(ds.variables)

    def _build_unchecked(self, datasets, precision):
        self._datasets = [sds for ds in datasets for sds in (ds._datasets if isinstance(ds, ConcatDataset) else [ds])]
        first_coord = self._datasets[0].coordinates
        self._dims = first_coord.keys()
        self.variables = {}

        self._index = _make_index(len(self._dims))
        for id_, ds in enumerate(self._datasets):
            coords = [min(ds.coordinates[dim].begin, ds.coordinates[dim].end) for dim in self._dims] \
                     + [max(ds.coordinates[dim].begin, ds.coordinates[dim].end) for dim in self._dims]
            self._index.insert(id_, coords)
            self.variables.update(ds.variables)

        self.coordinates = {}
        self._coord_data = {}
        for idx, dim in enumerate(self._dims):
            begin = self._index.bounds[idx]
            end = self._index.bounds[idx + len(self._dims)]
            density = lambda coord: (coord.length-1)/abs(coord.end - coord.begin)
            max_density = max(density(ds.coordinates[dim]) for ds in self._datasets if ds.coordinates[dim] > 1)
            # for ds in self._datasets:
            #
            #     coord_set = np.around(ds.get(dim), precision.get(dim, 6))
            #     if dim in self._coord_data:
            #         self._coord_data[dim] = np.union1d(self._coord_data[dim], coord_set)
            #     else:
            #         self._coord_data[dim] = coord_set

            self.coordinates[dim] = Coordinate(first_coord[dim].dtype,
                                               begin if first_coord[dim].begin <= first_coord[dim].end else end,
                                               end if first_coord[dim].begin <= first_coord[dim].end else begin,
                                               int((end-begin)*max_density+1.5))

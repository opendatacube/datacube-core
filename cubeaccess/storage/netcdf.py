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
import contextlib

from builtins import *
import netCDF4 as nc4

from ..core import Coordinate, Variable, DataArray
from ..utils import coord2index


class NetCDF4StorageUnit(object):
    def __init__(self, filepath):
        self._filepath = filepath
        self.coordinates = dict()
        self.variables = dict()

        with contextlib.closing(self._open_dataset()) as ncds:
            for name, var in ncds.variables.items():
                dims = var.dimensions
                if len(dims) == 1 and name == dims[0]:
                    self.coordinates[name] = Coordinate(var.dtype, var[0], var[-1], var.shape[0])
                else:
                    ndv = getattr(var, 'missing_value', None) or getattr(var, 'fill_value', None)
                    self.variables[name] = Variable(var.dtype, ndv, var.dimensions)

    def _open_dataset(self):
        return nc4.Dataset(self._filepath, mode='r', clobber=False, diskless=False, persist=False, format='NETCDF4')

    def get(self, name, **kwargs):
        with contextlib.closing(self._open_dataset()) as ncds:
            if name in self.coordinates:
                data = ncds.variables[name]
                index = coord2index(data, kwargs.get(name, None))
                data = data[index]
                return DataArray(data, coords=[data], dims=[name])

            if name in self.variables:
                var = self.variables[name]
                coords = [ncds.variables[dim] for dim in var.coordinates]
                indexes = tuple(coord2index(data, kwargs.get(name, None)) for data in coords)
                coords = [data[idx] for data, idx in zip(coords, indexes)]
                return DataArray(ncds.variables[name][indexes], coords=coords, dims=var.coordinates)

        raise KeyError(name + " is not a variable or coordinate")

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

import numpy
import contextlib
import netCDF4 as nc4

from ..core import Coordinate, Variable, StorageUnitBase


class NetCDF4StorageUnit(StorageUnitBase):
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

    def _get_coord(self, name):
        coord = self.coordinates[name]
        with contextlib.closing(self._open_dataset()) as ncds:
            return ncds[name][:]

    def _fill_data(self, name, index, dest):
        with contextlib.closing(self._open_dataset()) as ncds:
            numpy.copyto(dest, ncds[name][index])

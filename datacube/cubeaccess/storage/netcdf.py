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

import numpy
import netCDF4 as nc4

from ..core import Coordinate, Variable, StorageUnitBase
from ..indexing import Range, range_to_index, normalize_index


def _open_dataset(filepath):
    return nc4.Dataset(filepath, mode='r', clobber=False, diskless=False, persist=False, format='NETCDF4')


class NetCDF4StorageUnit(StorageUnitBase):
    def __init__(self, filepath, variables, coordinates, attributes=None):
        """
        :param variables: variables in the SU
        :param coordinates: coordinates in the SU
        """
        self._filepath = filepath
        self.coordinates = coordinates
        self.variables = variables
        self.attributes = attributes or {}

    @classmethod
    def from_file(cls, filepath):
        coordinates = {}
        variables = {}
        with contextlib.closing(_open_dataset(filepath)) as ncds:
            attributes = {k: getattr(ncds, k) for k in ncds.ncattrs()}
            for name, var in ncds.variables.items():
                dims = var.dimensions
                units = getattr(var, 'units', '1')
                if len(dims) == 1 and name == dims[0]:
                    coordinates[name] = Coordinate(dtype=var.dtype, begin=var[0], end=var[-1], length=var.shape[0],
                                                   units=units)
                    # TODO Store units in coordinates
                else:
                    ndv = (getattr(var, '_FillValue', None) or
                           getattr(var, 'missing_value', None) or
                           getattr(var, 'fill_value', None))
                    variables[name] = Variable(var.dtype, ndv, var.dimensions, units)
        return cls(filepath, variables=variables, coordinates=coordinates, attributes=attributes)

    def get_coord(self, dim, index=None):
        coord = self.coordinates[dim]
        index = normalize_index(coord, index)

        if isinstance(index, slice):
            with contextlib.closing(_open_dataset(self._filepath)) as ncds:
                return ncds[dim][index], index

        if isinstance(index, Range):
            with contextlib.closing(_open_dataset(self._filepath)) as ncds:
                data = ncds[dim][:]
                index = range_to_index(data, index)
                return data[index], index

    def _fill_data(self, name, index, dest):
        with contextlib.closing(_open_dataset(self._filepath)) as ncds:
            numpy.copyto(dest, ncds[name][index])

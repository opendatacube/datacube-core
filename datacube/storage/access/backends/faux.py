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

from functools import reduce as reduce_
import numpy

from ..core import StorageUnitBase


class FauxStorageUnit(StorageUnitBase):
    """ Fake Storage Unit for testing """
    def __init__(self, coordinates, variables):
        self.coordinates = coordinates
        self.variables = variables

    def _get_coord(self, name):
        coord = self.coordinates[name]
        data = numpy.linspace(coord.begin, coord.end, coord.length).astype(coord.dtype)
        return data

    def _fill_data(self, name, index, dest):
        var = self.variables[name]
        shape = tuple(self.coordinates[dim].length for dim in var.dimensions)
        size = reduce_(lambda x, y: x*y, shape, 1)
        numpy.copyto(dest, numpy.arange(size).reshape(shape)[index])

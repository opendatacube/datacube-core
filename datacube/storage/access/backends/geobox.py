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


class GeoBoxStorageUnit(StorageUnitBase):
    """ Fake Storage Unit for testing """
    def __init__(self, geobox, coordinates, variables):
        self.geobox = geobox
        self.coordinates = geobox.coordinates.copy()
        self.coordinates.update(coordinates)
        self.variables = variables

    @property
    def crs(self):
        return self.geobox.crs

    @property
    def affine(self):
        return self.geobox.affine

    @property
    def extent(self):
        return self.geobox.extent

    def _get_coord(self, name):
        if name in self.geobox.coordinate_labels:
            return self.geobox.coordinate_labels[name]
        else:
            coord = self.coordinates[name]
            data = numpy.linspace(coord.begin, coord.end, coord.length).astype(coord.dtype)
            return data

    def _fill_data(self, name, index, dest):
        var = self.variables[name]
        shape = tuple(self.coordinates[dim].length for dim in var.dimensions)
        size = reduce_(lambda x, y: x*y, shape, 1)
        numpy.copyto(dest, numpy.arange(size).reshape(shape)[index])

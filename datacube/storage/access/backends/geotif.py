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

import numpy
import rasterio

from ..core import StorageUnitBase
from datacube.model import Coordinate, Variable


class GeoTifStorageUnit(StorageUnitBase):
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
        with rasterio.open(filepath) as dataset:
            t = dataset.get_transform()
            coordinates = {
                'x': Coordinate(numpy.float64, t[0], t[0] + (dataset.width - 1) * t[1], dataset.width, '1'),
                'y': Coordinate(numpy.float64, t[3], t[3] + (dataset.height - 1) * t[5], dataset.height, '1')
            }

        def band2var(i):
            return Variable(dataset.dtypes[i], dataset.nodatavals[i], ('y', 'x'), '1')
        variables = {'layer%d' % (i + 1): band2var(i) for i in range(dataset.count)}

        return cls(filepath, variables=variables, coordinates=coordinates)

    @classmethod
    def from_other(cls, filepath, other):
        """
        :param other: template to copy coords/variables from (to speed up loading)
        :type other: GeoTifStorageUnit
        """
        return cls(filepath, variables=other.variables, coordinates=other.coordinates)

    def _get_coord(self, name):
        coord = self.coordinates[name]
        data = numpy.linspace(coord.begin, coord.end, coord.length, dtype=coord.dtype)
        return data

    def _fill_data(self, name, index, dest):
        layer = int(name[5:])
        with rasterio.open(self._filepath) as dataset:
            dataset.read(layer, out=dest,
                         window=((index[0].start, index[0].stop), (index[1].start, index[1].stop)))

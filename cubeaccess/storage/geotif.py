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

from ..core import Coordinate, Variable, StorageUnitBase


class GeoTifStorageUnit(StorageUnitBase):
    def __init__(self, filepath, other=None):
        """
        :param other: template to copy coords/variables from (to speed up loading)
        :type other: GeoTifStorageUnit
        """
        self._filepath = filepath
        if not other:
            with rasterio.open(self._filepath) as dataset:
                t = self._transform = dataset.get_transform()
                self._projection = str(dataset.crs_wkt)
                self.coordinates = {
                    'x': Coordinate(numpy.float64, t[0], t[0]+(dataset.width-1)*t[1], dataset.width),
                    'y': Coordinate(numpy.float64, t[3], t[3]+(dataset.height-1)*t[5], dataset.height)
                }

            def band2var(i):
                return Variable(dataset.dtypes[i], dataset.nodatavals[i], ('y', 'x'))
            self.variables = {str(i+1): band2var(i) for i in range(dataset.count)}
        else:
            self._transform = other._transform
            self._projection = other._projection
            self.coordinates = other.coordinates
            self.variables = other.variables

    def _get_coord(self, name):
        coord = self.coordinates[name]
        data = numpy.linspace(coord.begin, coord.end, coord.length, dtype=coord.dtype)
        return data

    def _fill_data(self, name, index, dest):
        with rasterio.open(self._filepath) as dataset:
            dataset.read(int(name), out=dest,
                         window=((index[0].start, index[0].stop), (index[1].start, index[1].stop)))

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
from osgeo import gdal, gdalconst
from osgeo.gdal_array import GDALTypeCodeToNumericTypeCode

from ..core import Coordinate, Variable, DataArray
from ..utils import coord2index


class GeoTifStorageUnit(object):
    def __init__(self, filepath):
        self._filepath = filepath
        dataset = gdal.Open(self._filepath, gdalconst.GA_ReadOnly)
        if dataset is None:
            raise IOError("failed to open " + self._filepath)

        t = self._transform = dataset.GetGeoTransform()
        self.coordinates = {
            'x': Coordinate(numpy.float32, t[0], t[0]+(dataset.RasterXSize-1)*t[1], dataset.RasterXSize),
            'y': Coordinate(numpy.float32, t[3], t[3]+(dataset.RasterYSize-1)*t[5], dataset.RasterYSize)
        }

        def band2var(band):
            return Variable(GDALTypeCodeToNumericTypeCode(band.DataType), band.GetNoDataValue(), ('y', 'x'))
        self.variables = {str(i+1): band2var(dataset.GetRasterBand(i+1)) for i in xrange(dataset.RasterCount)}

    def get(self, name, **kwargs):
        name = str(name)
        if name in self.coordinates:
            coord = self.coordinates[name]
            data = numpy.linspace(coord.begin, coord.end, coord.length, dtype=coord.dtype)
            index = coord2index(data, kwargs.get(name, None))
            data = data[index]
            return DataArray(data, coords=[data], dims=[name])

        if name in self.variables:
            var = self.variables[name]
            coords = [self.get(dim).values for dim in var.coordinates]
            indexes = tuple(coord2index(data, kwargs.get(dim, None)) for dim, data in zip(var.coordinates,coords))
            coords = [data[idx] for data, idx in zip(coords, indexes)]
            dataset = gdal.Open(self._filepath, gdalconst.GA_ReadOnly)
            if dataset is None:
                raise IOError("failed to open " + self._filepath)
            data = dataset.GetRasterBand(int(name)).ReadAsArray(indexes[1].start,
                                                                indexes[0].start,
                                                                indexes[1].stop-indexes[1].start,
                                                                indexes[0].stop-indexes[0].start)
            return DataArray(data, coords=coords, dims=var.coordinates)

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

from ..core import Coordinate, Variable, StorageUnitBase


class GeoTifStorageUnit(StorageUnitBase):
    def __init__(self, filepath, other=None):
        self._filepath = filepath
        if not other:
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
        else:
            self._transform = other._transform
            self.coordinates = other.coordinates
            self.variables = other.variables

    def _get_coord(self, name):
        coord = self.coordinates[name]
        data = numpy.linspace(coord.begin, coord.end, coord.length, dtype=coord.dtype)
        return data

    def _fill_data(self, name, index, dest):
        dataset = gdal.Open(self._filepath, gdalconst.GA_ReadOnly)
        if dataset is None:
            raise IOError("failed to open " + self._filepath)
        dataset.GetRasterBand(int(name)).ReadAsArray(index[1].start,
                                                     index[0].start,
                                                     index[1].stop - index[1].start,
                                                     index[0].stop - index[0].start,
                                                     buf_obj=dest)

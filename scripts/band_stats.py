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

from cubeaccess.core import StorageUnitDimensionProxy, StorageUnitStack
from cubeaccess.storage import GeoTifStorageUnit


def _time_from_filename(f):
    from datetime import datetime
    dtstr = f.split('/')[-1].split('_')[-1][:-4]
    # 2004-11-07T00-05-33.311000
    dt = datetime.strptime(dtstr, "%Y-%m-%dT%H-%M-%S.%f")
    return numpy.datetime64(dt, 's')


def _get_dataset_files():
    import glob
    files = glob.glob("/g/data/rs0/tiles/EPSG4326_1deg_0.00025pixel/LS5_TM/142_-033/2004/LS5_TM_NBAR_142_-033_*.tif")
    return files


@profile
def _get_dataset():
    files = _get_dataset_files()
    input = [(GeoTifStorageUnit(f), _time_from_filename(f)) for f in files]
    input.sort(key=lambda p: p[1])
    stack = StorageUnitStack([StorageUnitDimensionProxy(su, ('t', t)) for su, t in input], 't')
    return stack


@profile
def main():
    stack = _get_dataset()
    # TODO: group by
    # TODO: chunk by index
    n = 500

    nir = stack.get('4', y=slice(n))
    red = stack.get('3', y=slice(n))

    # TODO: mask
    nir = nir.values.astype(numpy.float32)
    nir[nir == -999] = numpy.nan
    red = red.values.astype(numpy.float32)
    red[red == -999] = numpy.nan
    # nir = numpy.ma.masked_equal(nir.values, -999)
    # red = numpy.ma.masked_equal(red.values, -999)
    ndvi = (nir-red)/(nir+red)

    # print(ndvi)


if __name__ == "__main__":
    main()

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

from __future__ import absolute_import, print_function
from datetime import datetime

import numpy
from osgeo import gdal

from cubeaccess.core import StorageUnitDimensionProxy, StorageUnitStack
from cubeaccess.storage import GeoTifStorageUnit


def argpercentile(a, q, axis=0):
    # TODO: pass ndv?
    # TODO: keepdim?
    q = numpy.array(q, dtype=numpy.float64, copy=True)/100.0
    nans = numpy.isnan(a).sum(axis=axis)
    q = q.reshape(q.shape+(1,)*nans.ndim)
    index = (q*(a.shape[axis]-1-nans) + 0.5).astype(numpy.int32)
    indices = numpy.indices(a.shape[:axis] + a.shape[axis+1:])
    index = tuple(indices[:axis]) + (index,) + tuple(indices[axis:])
    return numpy.argsort(a, axis=axis)[index]


def _time_from_filename(f):
    dtstr = f.split('/')[-1].split('_')[-1][:-4]
    # 2004-11-07T00-05-33.311000
    dt = datetime.strptime(dtstr, "%Y-%m-%dT%H-%M-%S.%f")
    return numpy.datetime64(dt, 's')


def _get_dataset(lat, lon, dataset='NBAR', sat='LS5_TM'):
    import glob
    lat_lon_str = '{:03d}_-{:03d}'.format(lat, abs(lon))
    pattern = '/g/data/rs0/tiles/EPSG4326_1deg_0.00025pixel/{sat}/{ll}/*/{sat}_{ds}_{ll}_*.tif'.format(sat=sat,
                                                                                                       ll=lat_lon_str,
                                                                                                       ds=dataset)
    files = glob.glob(pattern)
    template = GeoTifStorageUnit(files[0])
    input = [(GeoTifStorageUnit(f, template), _time_from_filename(f)) for f in files]
    input.sort(key=lambda p: p[1])
    stack = StorageUnitStack([StorageUnitDimensionProxy(su, ('t', t)) for su, t in input], 't')
    return stack


def write_file(name, data):
    driver = gdal.GetDriverByName("GTiff")
    raster = driver.Create(name+'.tif', 4000, 4000, 3, gdal.GDT_Int16,
                           options=["INTERLEAVE=BAND", "COMPRESS=LZW", "TILED=YES"])
    for idx, y in enumerate(range(0, 4000, 250)):
        raster.GetRasterBand(1).WriteArray(data[idx][0], 0, y)
        raster.GetRasterBand(2).WriteArray(data[idx][1], 0, y)
        raster.GetRasterBand(3).WriteArray(data[idx][2], 0, y)
    raster.FlushCache()
    del raster


def ndv_to_nan(a, ndv=-999):
    a = a.astype(numpy.float32)
    a[a == ndv] = numpy.nan
    return a


def do_thing(nir, red, green, blue, pqa):
    masked = 255 | 256 | 15360
    pqa_idx = ((pqa & masked) != masked)

    nir = ndv_to_nan(nir)
    nir[pqa_idx] = numpy.nan
    red = ndv_to_nan(red)
    red[pqa_idx] = numpy.nan

    ndvi = (nir-red)/(nir+red)
    index = argpercentile(ndvi, 90, axis=0)
    index = (index,) + tuple(numpy.indices(index.shape))

    red = red[index]
    green = ndv_to_nan(green[index])
    blue = ndv_to_nan(blue[index])

    return red, green, blue


def do_work(stack, pq, **kwargs):
    print(datetime.now(), kwargs)

    nir = stack.get('4', **kwargs).values
    red = stack.get('3', **kwargs).values
    pqa = pq.get('1', **kwargs).values
    green = stack.get('2', **kwargs).values
    blue = stack.get('1', **kwargs).values
    return do_thing(nir, red, green, blue, pqa)

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
import datetime


import builtins
if 'profile' not in builtins.__dict__:
    profile = lambda x: x
    builtins.__dict__['profile'] = lambda x: x


from cubeaccess.core import StorageUnitDimensionProxy, StorageUnitStack
from cubeaccess.storage import GeoTifStorageUnit
from cubeaccess.indexing import Range


def _time_from_filename(f):
    from datetime import datetime
    dtstr = f.split('/')[-1].split('_')[-1][:-4]
    # 2004-11-07T00-05-33.311000
    dt = datetime.strptime(dtstr, "%Y-%m-%dT%H-%M-%S.%f")
    return numpy.datetime64(dt, 's')


@profile
def _get_dataset(lat, lon, dataset='NBAR', sat='LS5_TM'):
    import glob
    lat_lon_str = '{:03d}_-{:03d}'.format(lat, abs(lon))
    pattern = '/g/data/rs0/tiles/EPSG4326_1deg_0.00025pixel/{sat}/{ll}/*/{sat}_{ds}_{ll}_*.tif'.format(sat=sat,
                                                                                                       ll=lat_lon_str,
                                                                                                       ds=dataset)
    files = glob.glob(pattern)
    input = [(GeoTifStorageUnit(f), _time_from_filename(f)) for f in files]
    input.sort(key=lambda p: p[1])
    stack = StorageUnitStack([StorageUnitDimensionProxy(su, ('t', t)) for su, t in input], 't')
    return stack


def argpercentile(a, q, axis=0):
    # TODO: pass ndv?
    # TODO: keepdim?
    q = numpy.array(q, dtype=numpy.float64, copy=True)/100.0
    nans = numpy.isnan(a).sum(axis=axis)
    index = (numpy.outer(q, (a.shape[axis]-1)-nans) + 0.5).astype(int)
    if q.ndim == 0:
        index = index[0]
    indices = numpy.indices(a.shape[:axis] + a.shape[axis+1:])
    index = tuple(indices[:axis]) + (index,) + tuple(indices[axis:])
    return numpy.argsort(a, axis=axis)[index]


def argpercentile1(a, q, axis=0):
    idx = int(q*(a.shape[axis]-1)/100.0 + 0.5)
    return numpy.argpartition(a, idx, axis=axis).take(idx, axis=axis)


@profile
def do_work(stack, **kwargs):
    print(datetime.datetime.now(), kwargs)

    nir = stack.get('4', **kwargs)
    red = stack.get('3', **kwargs)

    nir = nir.values.astype(numpy.float32)
    nir[nir == -999] = numpy.nan
    red = red.values.astype(numpy.float32)
    red[red == -999] = numpy.nan

    ndvi = (nir-red)/(nir+red)
    del nir
    index = argpercentile(ndvi, 10, axis=0)
    index = (index,) + tuple(numpy.indices(index.shape))

    red = red[index]
    green = stack.get('2', **kwargs).values[index].astype(numpy.float32)
    green[green == -999] = numpy.nan
    blue = stack.get('1', **kwargs).values[index].astype(numpy.float32)
    blue[blue == -999] = numpy.nan

    return red, green, blue


def main():
    N = 400
    stack = _get_dataset(142, -033)
    for dt in numpy.arange('1988', '1989', dtype='datetime64[Y]'):
        for yoff in range(0, 4000, N):
            do_work(stack, y=slice(yoff, yoff+N), t=Range(dt, dt+numpy.timedelta64(1, 'Y')))


if __name__ == "__main__":
    main()

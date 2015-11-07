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
from itertools import groupby

import numpy
from osgeo import gdal

from cubeaccess.core import StorageUnitVariableProxy, StorageUnitDimensionProxy, StorageUnitStack
from cubeaccess.storage import GeoTifStorageUnit
from cubeaccess.indexing import make_index


def argpercentile(a, q, axis=0):
    # TODO: pass ndv?
    # TODO: keepdim?
    q = numpy.array(q, dtype=numpy.float64, copy=True)/100.0
    nans = numpy.isnan(a).sum(axis=axis)
    q = q.reshape(q.shape+(1,)*nans.ndim)
    index = (q*(a.shape[axis]-1-nans) + 0.5).astype(numpy.int32)
    indices = numpy.indices(a.shape[:axis] + a.shape[axis+1:])
    index = tuple(indices[:axis]) + (index,) + tuple(indices[axis:])
    return numpy.argsort(a, axis=axis)[index], nans == a.shape[axis]


def _time_from_filename(f):
    dtstr = f.split('/')[-1].split('_')[-1][:-4].split('.')[0]
    # 2004-11-07T00-05-33.311000
    # dt = dateutil.parser.parse(dtstr)
    # dt = datetime.strptime(dtstr, "%Y-%m-%dT%H-%M-%S.%f")
    dt = datetime.strptime(dtstr, "%Y-%m-%dT%H-%M-%S")
    return numpy.datetime64(dt, 's')


def _get_dataset(lat, lon, dataset='NBAR', sat='LS5_TM'):
    import glob
    lat_lon_str = '{:03d}_-{:03d}'.format(lat, abs(lon))
    input = []
    LS57varmap = {'blue': '1', 'green': '2', 'red': '3', 'nir': '4', 'ir1': '5', 'ir2': '6'}
    PQAvarmap = {'pqa': '1'}
    varmaps = {
        'NBAR': {
            'LS5_TM': LS57varmap,
            'LS7_ETM': LS57varmap,
            'LS8_OLI_TIRS': {'blue': '2', 'green': '3', 'red': '4', 'nir': '5', 'ir1': '6', 'ir2': '7'}
        },
        'PQA': {
            'LS5_TM': PQAvarmap,
            'LS7_ETM': PQAvarmap,
            'LS8_OLI_TIRS': PQAvarmap
        }
    }

    for sat in ['LS5_TM', 'LS7_ETM', 'LS8_OLI_TIRS']:
        pattern = '{sat}/{ll}/*/{sat}_{ds}_{ll}_*[0-9][0-9].tif'.format(sat=sat,
                                                              ll=lat_lon_str,
                                                              ds=dataset)
        files = glob.glob('/g/data/rs0/tiles/EPSG4326_1deg_0.00025pixel/' + pattern)
        template = GeoTifStorageUnit(files[0])
        input += [(StorageUnitVariableProxy(GeoTifStorageUnit(f, template),
                                            varmaps[dataset][sat]),
                   _time_from_filename(f)) for f in files]
    input.sort(key=lambda p: p[1])
    input = [i.next() for k, i in groupby(input, key=lambda p: p[1])]
    stack = StorageUnitStack([StorageUnitDimensionProxy(su, ('t', t)) for su, t in input], 't')
    return stack


def write_files(name, data, qs, N, geotr, proj):
    driver = gdal.GetDriverByName("GTiff")
    nbands = len(data[0])
    for qidx, q in enumerate(qs):
        print('writing', name+'_'+str(q)+'.tif')
        raster = driver.Create(name+'_'+str(q)+'.tif', 4000, 4000, nbands, gdal.GDT_Int16,
                               options=["INTERLEAVE=BAND", "COMPRESS=LZW", "TILED=YES"])
        raster.SetProjection(proj)
        raster.SetGeoTransform(geotr)
        for band_num in range(nbands):
            band = raster.GetRasterBand(band_num+1)
            band.SetNoDataValue(-999)
            for idx, y in enumerate(range(0, 4000, N)):
                # TODO: hadle writing ndv nicer
                chunk = data[idx][band_num][qidx]
                if chunk.dtype == numpy.float32:
                    chunk = nan_to_ndv(chunk)
                band.WriteArray(chunk, 0, y)
            band.FlushCache()
        raster.FlushCache()
        del raster


def ndv_to_nan(a, ndv=-999):
    a = a.astype(numpy.float32)
    a[a == ndv] = numpy.nan
    return a


def nan_to_ndv(a, ndv=-999):
    a[numpy.isnan(a)] = ndv
    return a


def do_work(stack, pq, qs, **kwargs):
    print('starting', datetime.now(), kwargs)
    pqa = pq.get('pqa', **kwargs).values
    red = ndv_to_nan(stack.get('red', **kwargs).values)
    nir = ndv_to_nan(stack.get('nir', **kwargs).values)

    masked = 255 | 256 | 15360
    pqa_idx = ((pqa & masked) != masked)
    del pqa

    nir[pqa_idx] = numpy.nan
    red[pqa_idx] = numpy.nan

    ndvi = (nir-red)/(nir+red)
    index, mask = argpercentile(ndvi, qs, axis=0)

    # TODO: make slicing coordinates nicer
    tcoord = stack._get_coord('t')
    slice_ = make_index(tcoord, kwargs['t'])
    tcoord = tcoord[slice_]
    tcoord = tcoord[index]
    months = tcoord.astype('datetime64[M]').astype(int) % 12 + 1
    months[..., mask] = -999

    index = (index,) + tuple(numpy.indices(ndvi.shape[1:]))

    def index_data(data):
        data = ndv_to_nan(data[index])
        data[..., mask] = numpy.nan
        return data

    nir = index_data(nir)
    red = index_data(red)
    blue = index_data(stack.get('blue', **kwargs).values)
    green = index_data(stack.get('green', **kwargs).values)
    ir1 = index_data(stack.get('ir1', **kwargs).values)
    ir2 = index_data(stack.get('ir2', **kwargs).values)

    print('done', datetime.now(), kwargs)
    return blue, green, red, nir, ir1, ir2, months

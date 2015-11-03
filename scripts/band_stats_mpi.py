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
from functools import partial
from collections import namedtuple
from osgeo import gdal
import cPickle as pickle

import datetime

import numpy
from mpi4py import MPI


import builtins
if 'profile' not in builtins.__dict__:
    profile = lambda x: x
    builtins.__dict__['profile'] = lambda x: x


from cubeaccess.core import StorageUnitDimensionProxy, StorageUnitStack
from cubeaccess.storage import GeoTifStorageUnit
from cubeaccess.indexing import Range


class TaskTag(object):
    TASK=1
    STOP=2
    DONE=3
    FAIL=4


def run_worker(comm, root=0):
    status = MPI.Status()
    while True:
        task_func = comm.recv(source=root, status=status)
        if status.tag == TaskTag.TASK:
            try:
                result = task_func()
                comm.send(result, dest=root, tag=TaskTag.DONE)
            except:
                comm.send(None, dest=root, tag=TaskTag.FAIL)

        if status.tag == TaskTag.STOP:
            return


def stop_workers(comm, root=0):
    for rank in range(comm.size):
        if rank != root:
            comm.send(None, tag=TaskTag.STOP, dest=rank)


class MPIService(object):
    Task = namedtuple('Task', ['func', 'callback'])

    def __init__(self, comm):
        self._comm = comm
        self._unassigned_tasks = []
        self._pending_tasks = [None]*(comm.size-1)

    def run(self):
        status = MPI.Status()
        n_pending = 0
        while True:
            sends = []
            for worker, task in enumerate(self._pending_tasks):
                if not task and self._unassigned_tasks:
                    task = self._pending_tasks[worker] = self._unassigned_tasks.pop(0)
                    r = self._comm.isend(task.func, dest=worker+1, tag=TaskTag.TASK)
                    sends.append(r)
                    n_pending += 1
            MPI.Request.waitall(sends)

            if n_pending == 0:
                return

            # we're either out of workers or out of tasks, so wait for anything to complete
            result = self._comm.recv(status=status)
            task = self._pending_tasks[status.source-1]
            self._pending_tasks[status.source-1] = None
            n_pending -= 1
            if status.tag == TaskTag.FAIL:
                task.callback(None, result)
            else:
                task.callback(result, None)

    def post(self, func, callback):
        self._unassigned_tasks.append(MPIService.Task(func, callback))



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
    template = GeoTifStorageUnit(files[0])
    input = [(GeoTifStorageUnit(f, template), _time_from_filename(f)) for f in files]
    input.sort(key=lambda p: p[1])
    stack = StorageUnitStack([StorageUnitDimensionProxy(su, ('t', t)) for su, t in input], 't')
    return stack


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


@profile
def do_work(stack, pq, **kwargs):
    print(datetime.datetime.now(), kwargs)
    #return 'r', 'g', 'b'

    pqa = pq.get('1', **kwargs)
    masked = 255 | 256 | 15360
    pqa_idx = ((pqa.values & masked) != masked)
    del pqa

    nir = stack.get('4', **kwargs)
    red = stack.get('3', **kwargs)

    nir = nir.values.astype(numpy.float32)
    nir[nir == -999] = numpy.nan
    nir[pqa_idx] = numpy.nan
    red = red.values.astype(numpy.float32)
    red[red == -999] = numpy.nan
    red[pqa_idx] = numpy.nan

    ndvi = (nir-red)/(nir+red)
    del nir
    index = argpercentile(ndvi, 90, axis=0)
    index = (index,) + tuple(numpy.indices(index.shape))

    red = red[index]
    green = stack.get('2', **kwargs).values[index].astype(numpy.float32)
    green[green == -999] = numpy.nan
    #green[pqa_idx] = numpy.nan
    blue = stack.get('1', **kwargs).values[index].astype(numpy.float32)
    blue[blue == -999] = numpy.nan
    #blue[pqa_idx] = numpy.nan

    return red, green, blue


chunks = {}
def chunk_done(key, result, error):
    if key[0] not in chunks:
        chunks[key[0]] = {key[1]: result}
    else:
        chunks[key[0]][key[1]] = result
    if len(chunks[key[0]]) == 16:
        driver = gdal.GetDriverByName("GTiff")
        gdal_type = gdal.GDT_Int16
        raster = driver.Create(str(key[0])+'.tif', 4000, 4000, 3, gdal_type,
                               options=["INTERLEAVE=BAND", "COMPRESS=LZW", "TILED=YES"])
        for y in range(0, 4000, 250):
            raster.GetRasterBand(1).WriteArray(chunks[key[0]][y][0], 0, y)
            raster.GetRasterBand(2).WriteArray(chunks[key[0]][y][1], 0, y)
            raster.GetRasterBand(3).WriteArray(chunks[key[0]][y][2], 0, y)
        raster.FlushCache()
        del raster
        del chunks[key[0]]


def make_tasks(iosrv):
    stack = _get_dataset(146, -034)
    pq = _get_dataset(146, -034, dataset='PQA')
    N = 250

    def do_one(dt):
        chunk = {}
        def update_chunk(key, data):
            chunk[key] = data
            if len(chunk) == 16:
                print('got all')

        for idx, yoff in enumerate(range(0, 4000, N)):
            kwargs = dict(y=slice(yoff, yoff+N), t=Range(dt, dt+numpy.timedelta64(1, 'Y')))
            task = partial(do_work, stack, pq, **kwargs)
            iosrv.post(task, partial(chunk_done, (dt, yoff)))

    for dt in numpy.arange('1989', '1991', dtype='datetime64[Y]'):
        do_one(dt)


def main():
    comm = MPI.COMM_WORLD
    if comm.Get_rank() != 0:
        run_worker(comm)
        return

    iosrv = MPIService(comm)
    make_tasks(iosrv)
    iosrv.run()
    stop_workers(comm)


if __name__ == "__main__":
    main()

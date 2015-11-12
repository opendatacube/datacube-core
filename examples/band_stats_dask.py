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

import dask.imperative
import dask.multiprocessing

import numpy

import builtins
if 'profile' not in builtins.__dict__:
    profile = lambda x: x
    builtins.__dict__['profile'] = lambda x: x

from datacube.cubeaccess.indexing import Range
from common import do_work, _get_dataset, write_file


def main(argv):
    import yappi
    yappi.start(builtins=True)

    lon = int(argv[1])
    lat = int(argv[2])
    dt = numpy.datetime64(argv[3])

    stack = _get_dataset(lon, lat)
    pqa = _get_dataset(lon, lat, dataset='PQA')

    # TODO: this needs to propagate somehow from the input to the output
    geotr = stack._storage_units[0]._storage_unit._storage_unit._transform
    proj = stack._storage_units[0]._storage_unit._storage_unit._projection

    qs = [10, 50, 90]
    num_workers = 16
    N = 4000//num_workers

    tasks = []
    #for tidx, dt in enumerate(numpy.arange('1990', '1991', dtype='datetime64[Y]')):
    filename = '/g/data/u46/gxr547/%s_%s_%s'%(lon, lat, dt)
    data = []
    for yidx, yoff in enumerate(range(0, 4000, N)):
        kwargs = dict(y=slice(yoff, yoff+N), t=Range(dt, dt+numpy.timedelta64(1, 'Y')))
        r = dask.imperative.do(do_work)(stack, pqa, qs, **kwargs)
        data.append(r)
    for qidx, q in enumerate(qs):
        r = dask.imperative.do(write_file)(filename + '_' + str(q) + '.tif', data, qidx, N, geotr, proj)
        tasks.append(r)

    #executor = Executor('127.0.0.1:8787')
    #dask.imperative.compute(tasks, get=executor.get)

    dask.imperative.compute(tasks, get=dask.multiprocessing.get, num_workers=num_workers)

    yappi.convert2pstats(yappi.get_func_stats()).dump_stats('dask_mp.lprof')


if __name__ == "__main__":
    import sys
    main(sys.argv)

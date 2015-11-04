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

import dask.array
import dask.imperative
import dask.multiprocessing

import numpy

import builtins
if 'profile' not in builtins.__dict__:
    profile = lambda x: x
    builtins.__dict__['profile'] = lambda x: x

from cubeaccess.indexing import Range
from .common import do_work, _get_dataset, write_file


def main():
    stack = _get_dataset(146, -034)
    pqa = _get_dataset(146, -034, dataset='PQA')
    N = 250
    zzz = []
    for tidx, dt in enumerate(numpy.arange('1989', '1991', dtype='datetime64[Y]')):
        data = []
        for yidx, yoff in enumerate(range(0, 4000, N)):
            kwargs = dict(y=slice(yoff, yoff+N), t=Range(dt, dt+numpy.timedelta64(1, 'Y')))
            r = dask.imperative.do(do_work)(stack, pqa, **kwargs)
            data.append(r)
        r = dask.imperative.do(write_file)(str(dt), data)
        zzz.append(r)
    dask.imperative.compute(zzz, num_workers=16, get=dask.multiprocessing.get)



if __name__ == "__main__":
    main()

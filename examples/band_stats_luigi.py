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
import json

from builtins import *
import numpy
import builtins

import luigi
if 'profile' not in builtins.__dict__:
    profile = lambda x: x
    builtins.__dict__['profile'] = lambda x: x

from datacube.cubeaccess.indexing import Range
from common import do_work, _get_dataset, write_file


class DTEncoder(json.JSONEncoder):
    def default(self, obj):
        return str(obj)


def get_filename(args, band):
    return '/g/data/u46/gxr547/spam/'+ str(hash(json.dumps(args, sort_keys=True, cls=DTEncoder))) + "_" + str(band)+".npy"


class DoChunk(luigi.Task):
    dt = luigi.Parameter()
    N = luigi.Parameter()
    y = luigi.Parameter()
    stack = luigi.Parameter(significant=False)
    pqa = luigi.Parameter(significant=False)
    qs = luigi.Parameter(significant=False)

    def output(self):
        kwargs = dict(y=slice(self.y, self.y+self.N), t=Range(self.dt, self.dt+numpy.timedelta64(1, 'Y')))
        return [luigi.file.LocalTarget(get_filename(kwargs, idx)) for idx in range(7)]

    def run(self):
        kwargs = dict(y=slice(self.y, self.y+self.N), t=Range(self.dt, self.dt+numpy.timedelta64(1, 'Y')))
        data = do_work(self.stack, self.pqa, self.qs, **kwargs)
        for idx, band in enumerate(data):
            numpy.save(get_filename(kwargs, idx), data[idx])


class DoPretty(luigi.Task):
    dt = luigi.Parameter()
    qidx = luigi.Parameter()
    N = luigi.Parameter(significant=False)
    stack = luigi.Parameter(significant=False)
    pqa = luigi.Parameter(significant=False)
    qs = luigi.Parameter(significant=False)
    filename = luigi.Parameter(significant=False)
    geotr = luigi.Parameter(significant=False)
    proj = luigi.Parameter(significant=False)

    def requires(self):
        for yidx, yoff in enumerate(range(0, 4000, self.N)):
            yield DoChunk(self.dt, self.N, yoff, self.stack, self.pqa, self.qs)

    def output(self):
        return luigi.file.LocalTarget(self.filename)

    def run(self):
        data = []
        for yidx, yoff in enumerate(range(0, 4000, self.N)):
            kwargs = dict(y=slice(yoff, yoff+self.N), t=Range(self.dt, self.dt+numpy.timedelta64(1, 'Y')))
            bands = [numpy.load(get_filename(kwargs, idx)) for idx in range(7)]
            data.append(bands)
        write_file(self.filename, data, self.qidx, self.N, self.geotr, self.proj)


def main(argv):
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
    filename = '/g/data/u46/gxr547/spam/%s_%s_%s'%(lon, lat, dt)

    tasks = [DoPretty(stack=stack, pqa=pqa, dt=dt, N=N, qidx=qidx, qs=qs, geotr=geotr, proj=proj,
                      filename=filename+'_'+str(q)+'.tif') for qidx, q in enumerate(qs)]
    luigi.build(tasks, local_scheduler=True, workers=16)

if __name__ == "__main__":
    import sys
    main(sys.argv)

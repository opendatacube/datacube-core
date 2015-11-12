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
from functools import partial
from collections import namedtuple
from mpi4py import MPI

from builtins import *
import numpy
import builtins

if 'profile' not in builtins.__dict__:
    profile = lambda x: x
    builtins.__dict__['profile'] = lambda x: x

from datacube.cubeaccess.indexing import Range
from .common import do_work, _get_dataset, write_file


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


chunks = {}
def chunk_done(key, result, error):
    if key[0] not in chunks:
        chunks[key[0]] = {key[1]: result}
    else:
        chunks[key[0]][key[1]] = result
    if len(chunks[key[0]]) == 16:
        write_file(str(key[0]), [chunks[key[0]][k] for k in sorted(chunks[key[0]].keys())])
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

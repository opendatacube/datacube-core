#    Copyright 2016 Geoscience Australia
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

from __future__ import absolute_import, division


class SerialExecutor(object):
    map = map

    def apply(self, func, *args, **kwargs):
        return func(*args, **kwargs)


class DistributedExecutor(object):
    def __init__(self, executor):
        self._executor = executor

    def map(self, func, iterable):
        futures = self._executor.map(func, iterable)
        for future in futures:
            yield future.result()


def _get_multiprocessing_executor(workers):
    from multiprocessing import Pool
    return Pool()


def _get_ipyparallel_executor(workers):
    try:
        import ipyparallel
    except ImportError:
        return None
    try:
        rc = ipyparallel.Client()
    except (IOError, ipyparallel.TimeoutError):
        return None
    return rc.load_balanced_view()


def _get_distributed_executor(scheduler, workers):
    try:
        import distributed
    except ImportError:
        return None
    try:
        return DistributedExecutor(distributed.Executor(scheduler))
    except IOError:
        return None


def get_executor(scheduler, workers):
    if not workers:
        return SerialExecutor()

    if scheduler:
        distributed_exec = _get_distributed_executor(scheduler, workers)
        if distributed_exec:
            return distributed_exec

    executor = _get_ipyparallel_executor(workers)
    if executor:
        return executor

    return _get_multiprocessing_executor(workers)

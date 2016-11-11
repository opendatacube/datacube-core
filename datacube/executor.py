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

import sys
import six


class SerialExecutor(object):
    @staticmethod
    def submit(func, *args, **kwargs):
        return func, args, kwargs

    @staticmethod
    def map(func, iterable):
        return [SerialExecutor.submit(func, data) for data in iterable]

    @staticmethod
    def get_ready(futures):
        try:
            result = SerialExecutor.result(futures[0])
            return [(lambda x: x, [result], {})], [], futures[1:]
        except Exception:  # pylint: disable=broad-except
            exc_info = sys.exc_info()
            return [], [(six.reraise, exc_info, {})], futures[1:]

    @staticmethod
    def as_completed(futures):
        for future in futures:
            yield future

    @classmethod
    def next_completed(cls, futures, default):
        results = list(futures)
        if not results:
            return default, results
        result = next(cls.as_completed(results), default)
        results.remove(result)
        return result, results

    @staticmethod
    def results(futures):
        return [SerialExecutor.result(future) for future in futures]

    @staticmethod
    def result(future):
        func, args, kwargs = future
        return func(*args, **kwargs)

    @staticmethod
    def release(future):
        pass


def _get_distributed_executor(scheduler):
    """
    :param scheduler: Address of a scheduler
    """
    try:
        import distributed
    except ImportError:
        return None

    class DistributedExecutor(object):
        def __init__(self, executor):
            """
            :type executor: distributed.Executor
            :return:
            """
            self._executor = executor

        def submit(self, func, *args, **kwargs):
            return self._executor.submit(func, *args, pure=False, **kwargs)

        def map(self, func, iterable):
            return self._executor.map(func, iterable)

        @staticmethod
        def get_ready(futures):
            groups = {}
            for f in futures:
                groups.setdefault(f.status, []).append(f)
            return groups.get('finished', []), groups.get('error', []), groups.get('pending', [])

        @staticmethod
        def as_completed(futures):
            return distributed.as_completed(futures)

        @classmethod
        def next_completed(cls, futures, default):
            results = list(futures)
            if not results:
                return default, results
            result = next(cls.as_completed(results), default)
            results.remove(result)
            return result, results

        def results(self, futures):
            return self._executor.gather(futures)

        @staticmethod
        def result(future):
            return future.result()

        @staticmethod
        def release(future):
            future.release()

    try:
        executor = DistributedExecutor(distributed.Client(scheduler))
        return executor
    except IOError:
        return None


def _get_concurrent_executor(workers):
    try:
        from concurrent.futures import ProcessPoolExecutor, as_completed
    except ImportError:
        return None

    class MultiprocessingExecutor(object):
        def __init__(self, pool):
            self._pool = pool

        def submit(self, func, *args, **kwargs):
            return self._pool.submit(func, *args, **kwargs)

        def map(self, func, iterable):
            return [self.submit(func, data) for data in iterable]

        @staticmethod
        def get_ready(futures):
            completed = []
            failed = []
            pending = []
            for f in futures:
                if f.done():
                    if f.exception():
                        failed.append(f)
                    else:
                        completed.append(f)
                else:
                    pending.append(f)
            return completed, failed, pending

        @staticmethod
        def as_completed(futures):
            return as_completed(futures)

        @classmethod
        def next_completed(cls, futures, default):
            results = list(futures)
            if not results:
                return default, results
            result = next(cls.as_completed(results), default)
            results.remove(result)
            return result, results

        @staticmethod
        def results(futures):
            return [future.result() for future in futures]

        @staticmethod
        def result(future):
            return future.result()

        @staticmethod
        def release(future):
            pass

    return MultiprocessingExecutor(ProcessPoolExecutor(workers if workers > 0 else None))


def get_executor(scheduler, workers):
    """
    Return a task executor based on input parameters. Falling back as required.

    :param scheduler: IP address and port of a distributed.Scheduler, or a Scheduler instance
    :param workers: Number of processes to start for process based parallel execution
    """
    if not workers:
        return SerialExecutor()

    if scheduler:
        distributed_exec = _get_distributed_executor(scheduler)
        if distributed_exec:
            return distributed_exec

    concurrent_exec = _get_concurrent_executor(workers)
    if concurrent_exec:
        return concurrent_exec

    return SerialExecutor()

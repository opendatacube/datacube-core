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
    @staticmethod
    def submit(func, *args, **kwargs):
        return func, args, kwargs

    @staticmethod
    def map(func, iterable):
        return [SerialExecutor.submit(func, data) for data in iterable]

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
    def result(value):
        func, args, kwargs = value
        return func(*args, **kwargs)

    @staticmethod
    def release(value):
        pass


def _get_distributed_executor(scheduler, workers):
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
            return self._executor.submit(func, *args, **kwargs)

        def map(self, func, iterable):
            return self._executor.map(func, iterable)

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

        @staticmethod
        def result(value):
            return value.result()

        @staticmethod
        def release(value):
            value.release()

    try:
        executor = DistributedExecutor(distributed.Executor(scheduler))
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
        def result(value):
            return value.result()

        @staticmethod
        def release(value):
            pass

    return MultiprocessingExecutor(ProcessPoolExecutor(workers if workers > 0 else None))


def get_executor(scheduler, workers):
    if not workers:
        return SerialExecutor()

    if scheduler:
        distributed_exec = _get_distributed_executor(scheduler, workers)
        if distributed_exec:
            return distributed_exec

    concurrent_exec = _get_concurrent_executor(workers)
    if concurrent_exec:
        return concurrent_exec

    return SerialExecutor()

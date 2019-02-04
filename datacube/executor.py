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
import sys

_REMOTE_LOG_FORMAT_STRING = '%(asctime)s {} %(process)d %(name)s %(levelname)s %(message)s'


class SerialExecutor(object):
    def __repr__(self):
        return 'SerialExecutor'

    @staticmethod
    def submit(func, *args, **kwargs):
        return func, args, kwargs

    @staticmethod
    def map(func, iterable):
        return [SerialExecutor.submit(func, data) for data in iterable]

    @staticmethod
    def get_ready(futures):
        def reraise(t, e, traceback):
            raise t.with_traceback(e, traceback)

        try:
            result = SerialExecutor.result(futures[0])
            return [(lambda x: x, [result], {})], [], futures[1:]
        except Exception:  # pylint: disable=broad-except
            exc_info = sys.exc_info()
            return [], [(reraise, exc_info, {})], futures[1:]

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


def setup_logging():
    import logging
    import socket

    hostname = socket.gethostname()
    log_format_string = _REMOTE_LOG_FORMAT_STRING.format(hostname)

    handler = logging.StreamHandler()
    handler.formatter = logging.Formatter(log_format_string)
    logging.root.handlers = [handler]


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
            self.setup_logging()

        def setup_logging(self):
            self._executor.run(setup_logging)

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


def _run_cloud_pickled_function(f_data, *args, **kwargs):
    from cloudpickle import loads
    func = loads(f_data)
    return func(*args, **kwargs)


def _get_concurrent_executor(workers, use_cloud_pickle=False):
    try:
        from concurrent.futures import ProcessPoolExecutor, as_completed
    except ImportError:
        return None

    def mk_submitter(pool, use_cloud_pickle):
        def submit_direct(func, *args, **kwargs):
            return pool.submit(func, *args, **kwargs)

        def submit_cloud_pickle(func, *args, **kwargs):
            from cloudpickle import dumps
            return pool.submit(_run_cloud_pickled_function, dumps(func), *args, **kwargs)

        return submit_cloud_pickle if use_cloud_pickle else submit_direct

    class MultiprocessingExecutor(object):
        def __init__(self, pool, use_cloud_pickle):
            self._pool = pool
            self._submitter = mk_submitter(pool, use_cloud_pickle)

        def __repr__(self):
            max_workers = self._pool.__dict__.get('_max_workers', '??')
            return 'Multiprocessing ({})'.format(max_workers)

        def submit(self, func, *args, **kwargs):
            return self._submitter(func, *args, **kwargs)

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

    if workers <= 0:
        return None

    return MultiprocessingExecutor(ProcessPoolExecutor(workers), use_cloud_pickle)


def get_executor(scheduler, workers, use_cloud_pickle=True):
    """
    Return a task executor based on input parameters. Falling back as required.

    :param scheduler: IP address and port of a distributed.Scheduler, or a Scheduler instance
    :param workers: Number of processes to start for process based parallel execution
    :param use_cloud_pickle: Only applies when scheduler is None and workers > 0, default is True
    """
    if not workers:
        return SerialExecutor()

    if scheduler:
        distributed_exec = _get_distributed_executor(scheduler)
        if distributed_exec:
            return distributed_exec

    concurrent_exec = _get_concurrent_executor(workers, use_cloud_pickle=use_cloud_pickle)
    if concurrent_exec:
        return concurrent_exec

    return SerialExecutor()


def mk_celery_executor(host, port, password=''):
    """
    :param host: Address of the redis database server
    :param port: Port of the redis database server
    :password: Authentication for redis or None or ''
               '' -- load from home folder, or generate if missing,
               None -- no authentication
    """
    from ._celery_runner import CeleryExecutor
    return CeleryExecutor(host, port, password=password)

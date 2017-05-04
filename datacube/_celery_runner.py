from celery import Celery
from time import sleep
import redis
import os

# This can be changed via environment variable `REDIS`
REDIS_URL = 'redis://localhost:6379/0'


def mk_celery_app():
    url = os.environ.get('REDIS', REDIS_URL)

    _app = Celery('datacube_task', broker=url, backend=url)

    _app.conf.update(
        task_serializer='pickle',
        result_serializer='pickle',
        accept_content=['pickle'])

    return _app


# Celery worker launch script expects to see app object at the top level
# pylint: disable=invalid-name
app = mk_celery_app()


@app.task()
def run_cloud_pickled_function(f_data, *args, **kwargs):
    from cloudpickle import loads
    func = loads(f_data)
    return func(*args, **kwargs)


def submit_cloud_pickled_function(f, *args, **kwargs):
    from cloudpickle import dumps
    f_data = dumps(f)
    return run_cloud_pickled_function.delay(f_data, *args, **kwargs)


def launch_worker(argv=None):
    import sys
    argv = sys.argv if argv is None else argv

    app.worker_main(argv)


class CeleryExecutor(object):
    def __init__(self, host=None, port=None):
        # print('Celery: {}:{}'.format(host, port))
        self._shutdown = None

        if port or host:
            db = '0'
            url = 'redis://{}:{}/{}'.format(host if host else 'localhost',
                                            port if port else 6379,
                                            db)
            app.conf.update(result_backend=url,
                            broker_url=url)

        host = host if host else 'localhost'
        port = port if port else 6379

        if not check_redis(host, port):
            if host in ['localhost', '127.0.0.1']:
                self._shutdown = launch_redis(port if port else 6379)
            else:
                raise IOError("Can't connect to redis server @ {}:{}".format(host, port))

    def __del__(self):
        if self._shutdown:
            app.control.shutdown()
            sleep(1)
            self._shutdown()

    def __repr__(self):
        return 'CeleryRunner'

    def submit(self, func, *args, **kwargs):
        return submit_cloud_pickled_function(func, *args, **kwargs)

    def map(self, func, iterable):
        return [self.submit(func, data) for data in iterable]

    @staticmethod
    def get_ready(futures):
        completed = []
        failed = []
        pending = []
        for f in futures:
            if f.ready():
                if f.failed():
                    failed.append(f)
                else:
                    completed.append(f)
            else:
                pending.append(f)
        return completed, failed, pending

    @staticmethod
    def as_completed(futures):
        while len(futures) > 0:
            pending = []

            for promise in futures:
                if promise.ready():
                    yield promise
                else:
                    pending.append(promise)

            if len(pending) == len(futures):
                # If no change detected sleep for a bit
                # TODO: this is sub-optimal, not sure what other options are
                #       though?
                sleep(0.1)

            futures = pending

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
        return [future.get() for future in futures]

    @staticmethod
    def result(future):
        return future.get()

    @staticmethod
    def release(future):
        future.forget()


def check_redis(host='localhost', port=6379):
    server = redis.Redis(host, port)
    try:
        server.ping()
    except redis.exceptions.ConnectionError:
        return False
    return True


def launch_redis(port=6379, params=None, **kwargs):
    import tempfile
    from os import path
    import subprocess
    import shutil

    def stringify(v):
        if isinstance(v, str):
            return '"'+v+'"' if v.find(' ') >= 0 else v

        if isinstance(v, bool):
            return {True: 'yes', False: 'no'}[v]

        return str(v)

    def write_config(params, cfgfile):
        lines = ['{} {}\n'.format(k, stringify(v)) for k, v in params.items()]
        with open(cfgfile, "w") as f:
            f.writelines(lines)

    workdir = tempfile.mkdtemp(prefix='redis-')

    defaults = dict({'maxmemory-policy': 'noeviction'},
                    daemonize=True,
                    port=port,
                    databases=4,
                    maxmemory="100mb",
                    hz=50,
                    loglevel='notice',
                    pidfile=path.join(workdir, 'redis.pid'),
                    logfile=path.join(workdir, 'redis.log'))

    if params:
        defaults.update(params)

    defaults.update(kwargs)

    cfgfile = path.join(workdir, 'redis.cfg')
    write_config(defaults, cfgfile)

    def cleanup():
        shutil.rmtree(workdir)

    def shutdown():
        server = redis.Redis('localhost', port)
        server.shutdown()
        sleep(1)
        cleanup()

    try:
        subprocess.check_call(['redis-server', cfgfile])
    except subprocess.CalledProcessError:
        cleanup()
        return False

    return shutdown


import cloudpickle
from celery import Celery
from time import sleep
import redis
import os
import kombu.serialization

from celery.backends import base as celery_base

# This can be changed via environment variable `REDIS`
REDIS_URL = 'redis://localhost:6379/0'

kombu.serialization.registry.register(
    'cloudpickle',
    cloudpickle.dumps, cloudpickle.loads,
    content_type='application/x-python-cloudpickle',
    content_encoding='binary'
)

# Tell celery that it's ok to serialise exceptions using cloudpickle.
celery_base.EXCEPTION_ABLE_CODECS = celery_base.EXCEPTION_ABLE_CODECS.union({'cloudpickle'})


def mk_celery_app(addr=None):
    if addr is None:
        url = os.environ.get('REDIS', REDIS_URL)
    else:
        url = 'redis://{}:{}/0'.format(*addr)

    _app = Celery('datacube_task', broker=url, backend=url)

    _app.conf.update(
        task_serializer='cloudpickle',
        result_serializer='cloudpickle',
        event_serializer='cloudpickle',
        accept_content=['cloudpickle', 'json', 'pickle']
    )

    return _app


# Celery worker launch script expects to see app object at the top level
# pylint: disable=invalid-name
app = mk_celery_app()


def set_address(host, port=6379, db=0, password=None):
    if password is None:
        url = 'redis://{}:{}/{}'.format(host, port, db)
    else:
        url = 'redis://:{}@{}:{}/{}'.format(password, host, port, db)

    app.conf.update(result_backend=url,
                    broker_url=url)


@app.task()
def run_function(func, *args, **kwargs):
    return func(*args, **kwargs)


def launch_worker(host, port=6379, password=None, nprocs=None):
    if password == '':
        password = get_redis_password(generate_if_missing=False)

    set_address(host, port, password=password)

    argv = ['worker', '-A', 'datacube._celery_runner', '-E', '-l', 'INFO']
    if nprocs is not None:
        argv.extend(['-c', str(nprocs)])

    app.worker_main(argv)


def get_redis_password(generate_if_missing=False):
    from .utils import write_user_secret_file, slurp, gen_password

    REDIS_PASSWORD_FILE = '.datacube-redis'

    password = slurp(REDIS_PASSWORD_FILE, in_home_dir=True)
    if password is not None:
        return password

    if generate_if_missing:
        password = gen_password(12)
        write_user_secret_file(password, REDIS_PASSWORD_FILE, in_home_dir=True)

    return password


class CeleryExecutor(object):
    def __init__(self, host=None, port=None, password=None):
        # print('Celery: {}:{}'.format(host, port))
        self._shutdown = None

        if port or host or password:
            if password == '':
                password = get_redis_password(generate_if_missing=True)

            set_address(host if host else 'localhost',
                        port if port else 6379,
                        password=password)

        host = host if host else 'localhost'
        port = port if port else 6379

        if not check_redis(host, port, password):
            if host in ['localhost', '127.0.0.1']:
                self._shutdown = launch_redis(port if port else 6379, password=password)
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
        return run_function.delay(func, *args, **kwargs)

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


def check_redis(host='localhost', port=6379, password=None):
    if password == '':
        password = get_redis_password()

    server = redis.Redis(host, port, password=password)
    try:
        server.ping()
    except redis.exceptions.ConnectionError:
        return False
    except redis.exceptions.ResponseError as error:
        print('Redis responded with an error: {}'.format(error))
        return False
    return True


def launch_redis(port=6379, password=None, **kwargs):
    import tempfile
    from os import path
    import subprocess
    import shutil
    from .utils import write_user_secret_file

    def stringify(v):
        if isinstance(v, str):
            return '"' + v + '"' if v.find(' ') >= 0 else v

        if isinstance(v, bool):
            return {True: 'yes', False: 'no'}[v]

        return str(v)

    def fix_key(k):
        return k.replace('_', '-')

    def write_config(params, cfgfile):
        lines = ['{} {}'.format(fix_key(k), stringify(v)) for k, v in params.items()]
        cfg_txt = '\n'.join(lines)
        write_user_secret_file(cfg_txt, cfgfile)

    workdir = tempfile.mkdtemp(prefix='redis-')

    defaults = dict(maxmemory_policy='noeviction',
                    daemonize=True,
                    port=port,
                    databases=4,
                    maxmemory="100mb",
                    hz=50,
                    loglevel='notice',
                    pidfile=path.join(workdir, 'redis.pid'),
                    logfile=path.join(workdir, 'redis.log'))

    if password is not None:
        if password == '':
            password = get_redis_password(generate_if_missing=True)

        defaults['requirepass'] = password
    else:
        password = defaults.get('requirepass', None)

    defaults.update(kwargs)

    cfgfile = path.join(workdir, 'redis.cfg')
    write_config(defaults, cfgfile)

    def cleanup():
        shutil.rmtree(workdir)

    def shutdown():
        server = redis.Redis('localhost', port, password=password)
        server.shutdown()
        sleep(1)
        cleanup()

    try:
        subprocess.check_call(['redis-server', cfgfile])
    except subprocess.CalledProcessError:
        cleanup()
        return False

    return shutdown

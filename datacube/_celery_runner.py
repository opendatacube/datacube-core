from celery import Celery
import os

#TODO: make this configureable
REDIS_URL = 'redis://localhost:6379/0'

def mk_celery_app():
    url = os.environ.get('REDIS', REDIS_URL)
    return Celery('datacube_task', broker=url, backend=url)

#Celery worker launch script expects to see app object at the top level
#pylint: disable=invalid-name
app = mk_celery_app()

app.conf.update(
    task_serializer='pickle',
    result_serializer='pickle',
    accept_content=['pickle']
)


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

        if port or host:
            db = '0'
            url = 'redis://{}:{}/{}'.format(host if host else 'localhost',
                                            port if port else 6379,
                                            db)
            app.conf.update(result_backend=url,
                            broker_url=url)


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
        from time import sleep

        while len(futures) > 0:
            pending = []

            for promise in futures:
                if promise.ready():
                    yield promise
                else:
                    pending.append(promise)

            if len(pending) == len(futures):
                #If no change detected sleep for a bit
                #TODO: this is sub-optimal, not sure what other options are
                #      though?
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

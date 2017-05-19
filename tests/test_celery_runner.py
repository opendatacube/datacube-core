"""
Tests for datacube._celery_runner
"""

from time import sleep
import os
import subprocess
import pytest
from datacube import _celery_runner as cr

PORT = 29374
PASS = 'dfhksdjh23iuervao'
WRONG_PASS = 'sdfghdfjsghjdfiueuiwei'
REDIS_WAIT = 0.5


def check_redis_binary():
    try:
        return subprocess.check_call(['redis-server', '--version']) == 0
    except:
        return False


have_redis = check_redis_binary()
skip_if_no_redis = pytest.mark.skipif(not have_redis, reason="Needs redis-server to run")


@skip_if_no_redis
def test_launch_redis_no_password():
    redis_stop = cr.launch_redis(PORT, password=None, loglevel='verbose')
    assert redis_stop is not None

    sleep(REDIS_WAIT)
    is_running = cr.check_redis(port=PORT)
    assert is_running is True

    redis_stop()
    sleep(REDIS_WAIT)
    is_running = cr.check_redis(port=PORT)
    assert is_running is False


@skip_if_no_redis
def test_launch_redis_with_config_password():
    redis_stop = cr.launch_redis(PORT, password='', loglevel='verbose')
    assert redis_stop is not None

    sleep(REDIS_WAIT)
    is_running = cr.check_redis(port=PORT, password='')
    assert is_running is True

    redis_stop()
    sleep(REDIS_WAIT)
    is_running = cr.check_redis(port=PORT, password='')
    assert is_running is False


@skip_if_no_redis
def test_launch_redis_with_custom_password():

    redis_stop = cr.launch_redis(PORT, password=PASS, loglevel='verbose')
    assert redis_stop is not None

    sleep(REDIS_WAIT)
    is_running = cr.check_redis(port=PORT, password=PASS)
    assert is_running is True

    is_running = cr.check_redis(port=PORT, password=WRONG_PASS)
    assert is_running is False

    redis_stop()
    sleep(REDIS_WAIT)
    is_running = cr.check_redis(port=PORT, password=PASS)
    assert is_running is False


@pytest.mark.skip(reason="This test can hang if machine it runs on isn't setup appropriately")
@skip_if_no_redis
def test_celery_with_worker():
    DATA = [1, 2, 3, 4]

    def _echo(x, please_fail=False):
        if please_fail:
            raise IOError('Fake I/O error, cause you asked')
        return x

    def launch_worker():
        args = ['bash', '-c',
                'nohup datacube-worker --executor celery localhost:{} --nprocs 1 &'.format(PORT)]
        try:
            subprocess.check_call(args)
        except subprocess.CalledProcessError:
            return False

        return True

    if os.name == 'nt':
        return

    assert cr.check_redis('localhost', port=PORT, password='') is False

    runner = cr.CeleryExecutor(host='localhost', port=PORT, password='')
    sleep(REDIS_WAIT)

    assert cr.check_redis('localhost', port=PORT, password='')

    # no workers yet
    future = runner.submit(_echo, 0)
    assert future.ready() is False
    runner.release(future)

    futures = runner.map(_echo, DATA)
    assert len(futures) == len(DATA)

    completed, failed, pending = runner.get_ready(futures)

    assert len(completed) == 0
    assert len(failed) == 0
    assert len(pending) == len(DATA)
    # not worker test done

    worker_started_ok = launch_worker()
    assert worker_started_ok

    futures = runner.map(_echo, DATA)
    results = runner.results(futures)

    assert len(results) == len(DATA)
    assert set(results) == set(DATA)

    # Test failure pass-through
    future = runner.submit(_echo, "", please_fail=True)

    for ff in runner.as_completed([future]):
        assert ff.ready() is True
        with pytest.raises(IOError):
            runner.result(ff)

    del runner

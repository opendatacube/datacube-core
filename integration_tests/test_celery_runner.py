"""
Tests for datacube._celery_runner
"""

from time import sleep
import subprocess
import pytest
import sys

from datacube import _celery_runner as cr

PORT = 29374
PASS = 'dfhksdjh23iuervao'
WRONG_PASS = 'sdfghdfjsghjdfiueuiwei'
REDIS_WAIT = 0.5


def check_redis_binary():
    try:
        return subprocess.check_call(['redis-server', '--version']) == 0
    except Exception:
        return False


have_redis = check_redis_binary()
skip_if_no_redis = pytest.mark.skipif(not have_redis, reason="Needs redis-server to run")


@skip_if_no_redis
def test_launch_redis_no_password():
    is_running = cr.check_redis(port=PORT)
    assert is_running is False, "Redis should not be running at the start of the test"

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
    is_running = cr.check_redis(port=PORT)
    assert is_running is False, "Redis should not be running at the start of the test"

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
    is_running = cr.check_redis(port=PORT)
    assert is_running is False, "Redis should not be running at the start of the test"

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


def _echo(x, please_fail=False):
    if please_fail:
        raise IOError('Fake I/O error, cause you asked')
    return x


@pytest.mark.timeout(30)
@pytest.mark.skipif(sys.platform == 'win32',
                    reason="does not run on Windows")
@skip_if_no_redis
def test_celery_with_worker():
    DATA = [1, 2, 3, 4]

    def launch_worker():
        args = ['bash', '-c',
                'nohup {} -m datacube.execution.worker --executor celery localhost:{} --nprocs 1 &'.format(
                    sys.executable, PORT)]
        try:
            subprocess.check_call(args)
        except subprocess.CalledProcessError:
            return False

        return True

    assert cr.check_redis(port=PORT, password='') is False, "Redis should not be running at the start of the test"

    runner = cr.CeleryExecutor(host='localhost', port=PORT, password='')
    sleep(REDIS_WAIT)

    assert cr.check_redis(port=PORT, password='')

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

    # Redis shouldn't be running now.
    is_running = cr.check_redis(port=PORT)
    assert is_running is False

"""
Tests for datacube._celery_runner
"""

from time import sleep
from datacube import _celery_runner as cr

PORT = 29374
PASS = 'dfhksdjh23iuervao'
WRONG_PASS = 'sdfghdfjsghjdfiueuiwei'
REDIS_WAIT = 0.5


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


def _echo(x):
    return x


def test_celery_noop():
    DATA = [1, 2, 3, 4]
    runner = cr.CeleryExecutor(host='localhost', port=PORT)
    future = runner.submit(_echo, 0)

    assert future.ready() is False
    runner.release(future)

    futures = runner.map(_echo, DATA)
    assert len(futures) == len(DATA)

    completed, failed, pending = runner.get_ready(futures)

    assert len(completed) == 0
    assert len(failed) == 0
    assert len(pending) == len(DATA)

    del runner

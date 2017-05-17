"""
Tests for datacube._celery_runner
"""

from time import sleep
from datacube import _celery_runner as cr


def test_launch_redis_no_password():
    PORT = 29374
    redis_stop = cr.launch_redis(PORT, password=None, loglevel='verbose')
    assert redis_stop is not None

    sleep(1)
    is_running = cr.check_redis(port=PORT)
    assert is_running is True

    redis_stop()
    sleep(1)
    is_running = cr.check_redis(port=PORT)
    assert is_running is False


def test_launch_redis_with_config_password():
    PORT = 29374
    redis_stop = cr.launch_redis(PORT, password='', loglevel='verbose')
    assert redis_stop is not None

    sleep(1)
    is_running = cr.check_redis(port=PORT, password='')
    assert is_running is True

    redis_stop()
    sleep(1)
    is_running = cr.check_redis(port=PORT, password='')
    assert is_running is False


def test_launch_redis_with_custom_password():
    PORT = 29374
    PASS = 'dfhksdjh23iuervao'
    WRONG_PASS = 'sdfghdfjsghjdfiueuiwei'

    redis_stop = cr.launch_redis(PORT, password=PASS, loglevel='verbose')
    assert redis_stop is not None

    sleep(1)
    is_running = cr.check_redis(port=PORT, password=PASS)
    assert is_running is True

    is_running = cr.check_redis(port=PORT, password=WRONG_PASS)
    assert is_running is False

    redis_stop()
    sleep(1)
    is_running = cr.check_redis(port=PORT, password=PASS)
    assert is_running is False

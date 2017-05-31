"""
Tests for MultiprocessingExecutor
"""

from datacube.executor import get_executor
from time import sleep
import pytest

DATA = [1, 2, 3, 4]


def _echo(x, please_fail=False):
    if please_fail or x == 'please fail':
        raise IOError('Fake I/O error, cause you asked')
    return x


def run_tests_for_runner(runner, sleep_amount=0.5):
    # get_ready: mostly pending
    futures = runner.map(_echo, DATA)
    assert len(futures) == len(DATA)
    completed, failed, pending = runner.get_ready(futures)
    assert len(failed) == 0
    assert len(completed) + len(pending) == len(DATA)

    # get_ready: processed + failure
    data = ['please fail'] + DATA
    futures = runner.map(_echo, data)
    assert len(futures) == len(data)
    sleep(sleep_amount)  # give it "enough" time to finish
    completed, failed, pending = runner.get_ready(futures)
    if sleep_amount:
        assert len(failed) == 1
    else:
        assert len(failed) in [0, 1]

    assert len(completed) + len(pending) + len(failed) == len(data)

    # test results
    futures = runner.map(_echo, DATA)
    results = runner.results(futures)

    assert len(results) == len(DATA)
    assert set(results) == set(DATA)

    # Test failure pass-through
    future = runner.submit(_echo, "", please_fail=True)

    for ff in runner.as_completed([future]):
        with pytest.raises(IOError):
            runner.result(ff)

    # Next completed with data
    future = runner.submit(_echo, 'tt')
    futures = [future]
    result, futures = runner.next_completed(futures, 'default')
    assert len(futures) == 0
    print(type(result), result)

    # Next completed with empty list
    result, futures = runner.next_completed([], 'default')
    assert result == 'default'
    assert len(futures) == 0

    runner.release(future)


def test_concurrent_executor():
    runner = get_executor(None, 2)
    assert str(runner).find('Multi') >= 0
    run_tests_for_runner(runner, 0.3)

    runner = get_executor(None, 2, use_cloud_pickle=False)
    assert str(runner).find('Multi') >= 0
    run_tests_for_runner(runner, 0.3)


def test_fallback_executor():
    runner = get_executor(None, None)
    assert str(runner).find('Serial') >= 0

    run_tests_for_runner(runner, 0)

"""
Tests for MultiprocessingExecutor
"""

from datacube.executor import get_executor
from time import sleep
import pytest

DATA = [1, 2, 3, 4]
RETRIES = 5


def _echo(x, please_fail=False):
    if please_fail or x == 'please fail':
        raise IOError('Fake I/O error, because you asked')
    return x


def run_executor_tests(executor, sleep_time=1):
    # get_ready: mostly pending
    futures = executor.map(_echo, DATA)
    assert len(futures) == len(DATA)
    completed, failed, pending = executor.get_ready(futures)
    assert len(failed) == 0
    assert len(completed) + len(pending) == len(DATA)

    # get_ready: processed + failure
    data = ['please fail'] + DATA
    futures = executor.map(_echo, data)
    assert len(futures) == len(data)

    for _ in range(RETRIES):
        sleep(sleep_time)  # give it "enough" time to finish
        completed, failed, pending = executor.get_ready(futures)
        if len(pending) == 0:
            break

    if sleep_time:
        assert len(failed) == 1
    else:
        assert len(failed) in [0, 1]

    assert len(completed) + len(pending) + len(failed) == len(data)

    # test results
    futures = executor.map(_echo, DATA)
    results = executor.results(futures)

    assert len(results) == len(DATA)
    assert set(results) == set(DATA)

    # Test failure pass-through
    future = executor.submit(_echo, "", please_fail=True)

    for ff in executor.as_completed([future]):
        with pytest.raises(IOError):
            executor.result(ff)

    # Next completed with data
    future = executor.submit(_echo, 'tt')
    futures = [future]
    result, futures = executor.next_completed(futures, 'default')
    assert len(futures) == 0
    print(type(result), result)

    # Next completed with empty list
    result, futures = executor.next_completed([], 'default')
    assert result == 'default'
    assert len(futures) == 0

    executor.release(future)


def test_concurrent_executor():
    executor = get_executor(None, 2)
    assert 'Multiproc' in str(executor)
    run_executor_tests(executor)

    executor = get_executor(None, 2, use_cloud_pickle=False)
    assert 'Multiproc' in str(executor)
    run_executor_tests(executor)


def test_fallback_executor():
    executor = get_executor(None, None)
    assert 'Serial' in str(executor)

    run_executor_tests(executor, sleep_time=0)

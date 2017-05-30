"""
Tests for MultiprocessingExecutor
"""

from datacube.executor import get_executor
import pytest

DATA = [1, 2, 3, 4]


def _echo(x, please_fail=False):
    if please_fail:
        raise IOError('Fake I/O error, cause you asked')
    return x


def test_concurrent_executor():
    runner = get_executor(None, 2)

    futures = runner.map(_echo, DATA)
    assert len(futures) == len(DATA)

    completed, failed, pending = runner.get_ready(futures)

    assert len(failed) == 0
    assert len(completed) + len(pending) == len(DATA)

    futures = runner.map(_echo, DATA)
    results = runner.results(futures)

    assert len(results) == len(DATA)
    assert set(results) == set(DATA)

    # Test failure pass-through
    future = runner.submit(_echo, "", please_fail=True)

    for ff in runner.as_completed([future]):
        with pytest.raises(IOError):
            runner.result(ff)

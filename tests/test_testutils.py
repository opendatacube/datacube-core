import pytest
from datacube.testutils.threads import FakeThreadPoolExecutor


def test_fakethreadpool():

    def tfunc(a: int, b: int = 0, please_fail=False) -> int:
        if please_fail:
            raise ValueError('as you wish')
        if a == 13:
            raise ValueError('13')
        return a + b

    pool = FakeThreadPoolExecutor()

    assert pool.submit(tfunc, 1).result() == 1
    assert pool.submit(tfunc, 1, 2).result() == 3

    fut = pool.submit(tfunc, 1, please_fail=True)
    assert fut.done()
    assert fut.exception() is not None

    with pytest.raises(ValueError):
        fut.result()

    ff = list(pool.map(tfunc, range(14)))
    assert len(ff) == 14
    assert [f.result() for f in ff[:13]] == list(range(13))
    assert ff[13].exception() is not None

    aa = list(range(10))
    bb = aa[::-1]
    ff = list(pool.map(tfunc, aa, bb))
    assert len(ff) == 10
    assert [f.result() for f in ff[:13]] == [a+b for a, b in zip(aa, bb)]

    pool.shutdown()

import pytest
from datacube.testutils.threads import FakeThreadPoolExecutor
from datacube.testutils import mk_sample_xr_dataset


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


def test_mk_sample_xr():
    xx = mk_sample_xr_dataset()
    assert 'band' in xx.data_vars
    assert list(xx.coords) == ['time', 'y', 'x', 'spatial_ref']
    assert xx.band.dims == ('time', 'y', 'x')
    assert xx.geobox is not None

    assert mk_sample_xr_dataset(name='xx', shape=(3, 7)).xx.shape == (1, 3, 7)
    assert mk_sample_xr_dataset(name='xx', time=None, shape=(3, 7)).xx.shape == (3, 7)
    assert mk_sample_xr_dataset(name='xx', time=None).xx.dims == ('y', 'x')

    assert mk_sample_xr_dataset(resolution=(1, 100)).geobox.resolution == (1, 100)
    assert mk_sample_xr_dataset(resolution=(1, 100), xy=(3, 55)).geobox.transform*(0, 0) == (3, 55)
    assert mk_sample_xr_dataset(crs=None).geobox is None

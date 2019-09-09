import dask
import dask.delayed

from datacube.utils.dask import (
    start_local_dask,
    compute_tasks,
    pmap,
    partition_map,
)


def test_compute_tasks():
    client = start_local_dask(threads_per_worker=1,
                              dashboard_address=None)

    tasks = (dask.delayed(x) for x in range(100))
    xx = [x for x in compute_tasks(tasks, client)]
    assert xx == [x for x in range(100)]

    client.close()
    del client


def test_partition_map():
    tasks = partition_map(10, str, range(101))
    tt = [t for t in tasks]
    assert len(tt) == 11
    lump = tt[0].compute()
    assert len(lump) == 10
    assert lump == [str(x) for x in range(10)]

    lump = tt[-1].compute()
    assert len(lump) == 1


def test_pmap():
    client = start_local_dask(threads_per_worker=1,
                              dashboard_address=None)

    xx_it = pmap(str, range(101), client=client)
    xx = [x for x in xx_it]

    assert xx == [str(x) for x in range(101)]

    client.close()
    del client

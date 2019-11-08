import pytest
import moto
from pathlib import Path
import dask
import dask.delayed

from datacube.utils.io import slurp

from datacube.utils.dask import (
    start_local_dask,
    get_total_available_memory,
    compute_memory_per_worker,
    compute_tasks,
    pmap,
    partition_map,
    save_blob_to_file,
    save_blob_to_s3,
    _save_blob_to_file,
    _save_blob_to_s3,
)

from datacube.utils.aws import (
    s3_url_parse,
    s3_fetch,
    s3_client,
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


@pytest.mark.parametrize("blob", [
    "some utf8 string",
    b"raw bytes",
])
def test_save_blob_file_direct(tmpdir, blob):
    tmpdir = Path(str(tmpdir))
    fname = str(tmpdir/"file.txt")
    mode = "rt" if isinstance(blob, str) else "rb"

    assert _save_blob_to_file(blob, fname) == (fname, True)
    assert slurp(fname, mode=mode) == blob

    fname = str(tmpdir/"missing"/"file.txt")
    assert _save_blob_to_file(blob, fname) == (fname, False)


@pytest.mark.parametrize("blob", [
    "some utf8 string",
    b"raw bytes",
])
def test_save_blob_file(tmpdir, blob, dask_client):
    tmpdir = Path(str(tmpdir))
    fname = str(tmpdir/"file.txt")
    dask_blob = dask.delayed(blob)
    mode = "rt" if isinstance(blob, str) else "rb"

    rr = save_blob_to_file(dask_blob, fname)
    assert dask_client.compute(rr).result() == (fname, True)
    assert slurp(fname, mode=mode) == blob

    fname = str(tmpdir/"missing"/"file.txt")
    rr = save_blob_to_file(dask_blob, fname)
    assert dask_client.compute(rr).result() == (fname, False)


@pytest.mark.parametrize("blob", [
    "some utf8 string",
    b"raw bytes",
])
def test_save_blob_s3_direct(blob, monkeypatch):
    region_name = "us-west-2"
    blob2 = blob + blob

    url = "s3://bucket/file.txt"
    url2 = "s3://bucket/file-2.txt"

    bucket, _ = s3_url_parse(url)
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "fake-key-id")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "fake-secret")

    with moto.mock_s3():
        s3 = s3_client(region_name=region_name)
        s3.create_bucket(Bucket=bucket)

        assert _save_blob_to_s3(blob, url, region_name=region_name) == (url, True)
        assert _save_blob_to_s3(blob2, url2, region_name=region_name) == (url2, True)

        bb1 = s3_fetch(url, s3=s3)
        bb2 = s3_fetch(url2, s3=s3)
        if isinstance(blob, str):
            bb1 = bb1.decode("utf8")
            bb2 = bb2.decode("utf8")

        assert bb1 == blob
        assert bb2 == blob2

        assert _save_blob_to_s3("", "s3://not-a-bucket/f.txt") == ("s3://not-a-bucket/f.txt", False)


@pytest.mark.parametrize("blob", [
    "some utf8 string",
    b"raw bytes",
])
def test_save_blob_s3(blob, monkeypatch, dask_client):
    region_name = "us-west-2"

    blob2 = blob + blob

    dask_blob = dask.delayed(blob)
    dask_blob2 = dask.delayed(blob2)

    url = "s3://bucket/file.txt"
    url2 = "s3://bucket/file-2.txt"

    bucket, _ = s3_url_parse(url)
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "fake-key-id")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "fake-secret")

    with moto.mock_s3():
        s3 = s3_client(region_name=region_name)
        s3.create_bucket(Bucket=bucket)

        rr = save_blob_to_s3(dask_blob, url, region_name=region_name)
        assert rr.compute() == (url, True)

        rr = save_blob_to_s3(dask_blob2, url2, region_name=region_name)
        assert dask_client.compute(rr).result() == (url2, True)

        bb1 = s3_fetch(url, s3=s3)
        bb2 = s3_fetch(url2, s3=s3)
        if isinstance(blob, str):
            bb1 = bb1.decode("utf8")
            bb2 = bb2.decode("utf8")

        assert bb1 == blob
        assert bb2 == blob2


def test_memory_functions(monkeypatch):
    gig = 10**9

    total_mem = get_total_available_memory()
    default_safety = min(500*(1 << 20), total_mem//2)

    assert total_mem - compute_memory_per_worker() == default_safety
    assert total_mem - compute_memory_per_worker(2)*2 == default_safety

    assert compute_memory_per_worker(mem_safety_margin=1) == total_mem - 1
    assert compute_memory_per_worker(memory_limit='4G') == 4*gig
    assert compute_memory_per_worker(2, memory_limit='4G') == 2*gig
    assert compute_memory_per_worker(memory_limit='4G',
                                     mem_safety_margin='1G') == 3*gig

    total_mem = 1*gig
    monkeypatch.setenv('MEM_LIMIT', str(total_mem))
    assert get_total_available_memory() == 1*gig
    assert compute_memory_per_worker(mem_safety_margin=1) == total_mem - 1

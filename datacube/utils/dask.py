""" Dask Distributed Tools

"""
from typing import Any, Iterable, Optional, Union, Tuple
from random import randint
import toolz
import queue
from dask.distributed import Client
import dask
import threading
import logging
import os
from botocore.credentials import ReadOnlyCredentials
from botocore.exceptions import BotoCoreError
from .aws import s3_dump, s3_client


__all__ = (
    "start_local_dask",
    "pmap",
    "compute_tasks",
    "partition_map",
    "save_blob_to_file",
    "save_blob_to_s3",
)

_LOG = logging.getLogger(__name__)


def get_total_available_memory(check_jupyter_hub=True):
    """ Figure out how much memory is available
        1. Check MEM_LIMIT environment variable, set by jupyterhub
        2. Use hardware information if that not set
    """
    if check_jupyter_hub:
        mem_limit = os.environ.get('MEM_LIMIT', None)
        if mem_limit is not None:
            return int(mem_limit)

    from psutil import virtual_memory
    return virtual_memory().total


def compute_memory_per_worker(n_workers: int = 1,
                              mem_safety_margin: Optional[Union[str, int]] = None,
                              memory_limit: Optional[Union[str, int]] = None) -> int:
    """ Figure out how much memory to assign per worker.

        result can be passed into `memory_limit=` parameter of dask worker/cluster/client
    """
    from dask.utils import parse_bytes

    if isinstance(memory_limit, str):
        memory_limit = parse_bytes(memory_limit)

    if isinstance(mem_safety_margin, str):
        mem_safety_margin = parse_bytes(mem_safety_margin)

    if memory_limit is None and mem_safety_margin is None:
        total_bytes = get_total_available_memory()
        # leave 500Mb or half of all memory if RAM is less than 1 Gb
        mem_safety_margin = min(500*(1024*1024), total_bytes//2)
    elif memory_limit is None:
        total_bytes = get_total_available_memory()
    elif mem_safety_margin is None:
        total_bytes = memory_limit
        mem_safety_margin = 0
    else:
        total_bytes = memory_limit

    return (total_bytes - mem_safety_margin)//n_workers


def start_local_dask(n_workers: int = 1,
                     threads_per_worker: Optional[int] = None,
                     mem_safety_margin: Optional[Union[str, int]] = None,
                     memory_limit: Optional[Union[str, int]] = None,
                     **kw):
    """Wrapper around `distributed.Client(..)` constructor that deals with memory better.

    :param n_workers: number of worker processes to launch
    :param threads_per_worker: number of threads per worker, default is as many as there are CPUs
    :param mem_safety_margin: bytes to reserve for the rest of the system, only applicable
                              if `memory_limit=` is not supplied.

    NOTE: if `memory_limit` is supplied, it will be parsed and divided equally between workers.
    """
    memory_limit = compute_memory_per_worker(n_workers=n_workers,
                                             memory_limit=memory_limit,
                                             mem_safety_margin=mem_safety_margin)

    client = Client(n_workers=n_workers,
                    threads_per_worker=threads_per_worker,
                    memory_limit=memory_limit,
                    **kw)

    return client


def _randomize(prefix):
    return '{}-{:08x}'.format(prefix, randint(0, 0xFFFFFFFF))


def partition_map(n: int, func: Any, its: Iterable[Any],
                  name: str = 'compute') -> Iterable[Any]:
    """ Partition sequence into lumps of size `n`, then construct dask delayed computation evaluating to:

    [func(x) for x in its[0:1n]],
    [func(x) for x in its[n:2n]],
    ...
    [func(x) for x in its[]],

    :param n: number of elements to process in one go
    :param func: Function to apply (non-dask)
    :param its:  Values to feed to fun
    :param name: How the computation should be named in dask visualizations
    """
    def lump_proc(dd):
        return [func(d) for d in dd]

    proc = dask.delayed(lump_proc, nout=1, pure=True)
    data_name = _randomize('data_' + name)
    name = _randomize(name)

    for i, dd in enumerate(toolz.partition_all(n, its)):
        lump = dask.delayed(dd,
                            pure=True,
                            traverse=False,
                            name=data_name + str(i))
        yield proc(lump, dask_key_name=name + str(i))


def compute_tasks(tasks: Iterable[Any], client: Client,
                  max_in_flight: int = 3) -> Iterable[Any]:
    """ Parallel compute stream with back pressure.

        Equivalent to:

        (client.compute(task).result()
          for task in tasks)

        but with up to `max_in_flight` tasks being processed at the same time.
        Input/Output order is preserved, so there is a possibility of head of
        line blocking.

        NOTE: lower limit is 3 concurrent tasks to simplify implementation,
              there is no point calling this function if you want one active
              task and supporting exactly 2 active tasks is not worth the complexity,
              for now. We might special-case `2` at some point.

    """
    # New thread:
    #    1. Take dask task from iterator
    #    2. Submit to client for processing
    #    3. Send it of to wrk_q
    #
    # Calling thread:
    #    1. Pull scheduled future from wrk_q
    #    2. Wait for result of the future
    #    3. yield result to calling code
    from .generic import it2q, qmap

    # (max_in_flight - 2) -- one on each side of queue
    wrk_q = queue.Queue(maxsize=max(1, max_in_flight - 2))  # type: queue.Queue

    # fifo_timeout='0ms' ensures that priority of later tasks is lower
    futures = (client.compute(task, fifo_timeout='0ms') for task in tasks)

    in_thread = threading.Thread(target=it2q, args=(futures, wrk_q))
    in_thread.start()

    yield from qmap(lambda f: f.result(), wrk_q)

    in_thread.join()


def pmap(func: Any,
         its: Iterable[Any],
         client: Client,
         lump: int = 1,
         max_in_flight: int = 3,
         name: str = 'compute') -> Iterable[Any]:
    """ Parallel map with back pressure.

    Equivalent to this:

       (func(x) for x in its)

    Except that ``func(x)`` runs concurrently on dask cluster.

    :param func:   Method that will be applied concurrently to data from ``its``
    :param its:    Iterator of input values
    :param client: Connected dask client
    :param lump:   Group this many datasets into one task
    :param max_in_flight: Maximum number of active tasks to submit
    :param name:   Dask name for computation
    """
    max_in_flight = max_in_flight // lump

    tasks = partition_map(lump, func, its, name=name)

    for xx in compute_tasks(tasks, client=client, max_in_flight=max_in_flight):
        yield from xx


def _save_blob_to_file(data: Union[bytes, str],
                       fname: str) -> Tuple[str, bool]:
    if isinstance(data, str):
        data = data.encode('utf8')

    try:
        with open(fname, 'wb') as f:
            f.write(data)
    except IOError:
        return (fname, False)

    return (fname, True)


@dask.delayed(name='save-to-disk', pure=False)
def save_blob_to_file(data,
                      fname,
                      with_deps=None):
    """ Dump from memory to local filesystem as a dask delayed operation.

    NOTE: dask workers better be local or have network filesystem mounted in
    the same path as calling code.

    :param data     : Data blob to save to file (have to fit into memory all at once),
                      strings will be saved in UTF8 format.
    :param fname    : Path to file
    :param with_deps: Useful for introducing dependencies into dask graph,
                      for example save yaml file after saving all tiff files.

    Returns
    -------
    (File Path, True) tuple on success
    (File Path, False) on any error
    """
    return _save_blob_to_file(data, fname)


def _save_blob_to_s3(data: Union[bytes, str],
                     url: str,
                     profile: Optional[str] = None,
                     creds: Optional[ReadOnlyCredentials] = None,
                     region_name: Optional[str] = None,
                     **kw) -> Tuple[str, bool]:
    """ Dump from memory to S3 as a dask delayed operation.

    :param data       : Data blob to save to file (have to fit into memory all at once)
    :param url        : Url in a form s3://bucket/path/to/file

    :param profile    : Profile name to lookup (only used if session is not supplied)
    :param creds      : Override credentials with supplied data
    :param region_name: Region name to use, overrides session setting

    Returns
    -------
    (url, True) tuple on success
    (url, False) on any error
    """
    from botocore.errorfactory import ClientError
    try:
        s3 = s3_client(profile=profile,
                       creds=creds,
                       region_name=region_name,
                       cache=True)

        result = s3_dump(data, url, s3=s3, **kw)
    except (IOError, BotoCoreError, ClientError):
        result = False

    return url, result


@dask.delayed(name='save-to-s3', pure=False)
def save_blob_to_s3(data,
                    url,
                    profile=None,
                    creds=None,
                    region_name=None,
                    with_deps=None,
                    **kw):
    """ Dump from memory to S3 as a dask delayed operation.

    :param data       : Data blob to save to file (have to fit into memory all at once)
    :param url        : Url in a form s3://bucket/path/to/file

    :param profile    : Profile name to lookup (only used if session is not supplied)
    :param creds      : Override credentials with supplied data
    :param region_name: Region name to use, overrides session setting

    :param with_deps  : Useful for introducing dependencies into dask graph,
                        for example save yaml file after saving all tiff files.

    Returns
    -------
    (url, True) tuple on success
    (url, False) on any error
    """
    return _save_blob_to_s3(data, url,
                            profile=profile,
                            creds=creds,
                            region_name=region_name,
                            **kw)

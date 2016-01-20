# coding=utf-8
"""
Ingest datasets into the AGDC.
"""
from __future__ import absolute_import

import os
import sys
import signal
import logging
from multiprocessing import Pool

from datacube import ui, storage
from datacube.index import index_connect

_LOG = logging.getLogger(__name__)


def index_datasets(path, index=None):
    """
    Discover datasets in path and add them to the index

    :type path: pathlib.Path
    :type index: datacube.index._api.Index
    :rtype: list[datacube.model.Dataset]
    """
    index = index or index_connect()

    metadata_path = ui.get_metadata_path(path)
    if not metadata_path or not metadata_path.exists():
        raise ValueError('No supported metadata docs found for dataset {}'.format(path))

    datasets = [index.datasets.add(metadata_doc, metadata_path)
                for metadata_path, metadata_doc
                in ui.read_documents(metadata_path)]
    _LOG.info('Indexed datasets %s', path)
    return datasets


def store_datasets(datasets, index=None, workers=0):
    """
    Create any necessary storage units for the given datasets.

    Find matching storage_types for datasets
    Create storage units for datasets according to the storage_type
    Add storage units to the index

    :type datasets: list[datacube.model.Dataset]
    :type index: datacube.index._api.Index
    """
    index = index or index_connect()

    storage_types = find_storage_types_for_datasets(datasets, index)

    for storage_type_id, datasets in storage_types.items():
        storage_type = index.storage.types.get(storage_type_id)
        _LOG.info('Using %s to store %s datasets', storage_type, datasets)
        storage_units = create_storage_units(datasets, storage_type, workers=workers)
        index.storage.add_many(storage_units)


def find_storage_types_for_datasets(datasets, index=None):
    """
    Find matching storage_types for datasets

    :type datasets: list[datacube.model.Dataset]
    :type index: datacube.index._api.Index
    :rtype dict[int, list[datacube.model.Dataset]]
    """
    index = index or index_connect()

    storage_types = {}
    for dataset in datasets:
        matching_types = index.storage.types.get_for_dataset(dataset)
        if not matching_types:
            _LOG.warning('No storage types found for %s dataset', dataset)
        for storage_type in matching_types:
            storage_types.setdefault(storage_type.id_, []).append(dataset)
    return storage_types


def create_storage_units(datasets, storage_type, workers=0):
    """
    Create storage units for datasets using storage_type
    Add storage units to the index

    :type datasets: list[datacube.model.Dataset]
    :type storage_type: datacube.model.StorageType
    """
    # :type tile_index: (x,y)
    # Each task is an entire storage unit, safe to run tasks in parallel

    tasks = [(tile_index, storage_type, datasets) for
             tile_index, datasets in storage.tile_datasets_with_storage_type(datasets, storage_type).items()]

    try:
        if workers:
            storage_units = _run_parallel_tasks(_create_storage_unit, tasks, workers)
        else:
            storage_units = [_create_storage_unit(task) for task in tasks]

        return storage_units
    except:
        for task in tasks:
            _remove_storage_unit(task)
        raise


def _create_storage_unit(task):
    tile_index, storage_type, datasets = task
    filename = storage.generate_filename(tile_index, datasets, storage_type)
    return storage.create_storage_unit(tile_index, datasets, storage_type, filename)


def _run_parallel_tasks(func, tasks, workers):
    pool = Pool(processes=workers, initializer=_init_worker)
    try:
        result = list(pool.imap_unordered(func, tasks))
    except:
        pool.terminate()
        raise
    else:
        pool.close()
    finally:
        pool.join()

    return result


def _remove_storage_unit(task):
    tile_index, storage_type, datasets = task
    filename = storage.generate_filename(tile_index, datasets, storage_type)
    try:
        os.unlink(filename)
    except OSError:
        pass


def _init_worker(*args):
    signal.signal(signal.SIGTERM, lambda s, f: sys.exit(0))

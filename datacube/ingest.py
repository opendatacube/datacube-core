# coding=utf-8
"""
Ingest datasets into the AGDC.
"""
from __future__ import absolute_import

from collections import defaultdict
import os
import logging

from datacube import ui, storage
from datacube.executor import SerialExecutor
from datacube.index import index_connect
from datacube.model import _uri_to_local_path

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


def store_datasets(datasets, index=None, executor=SerialExecutor()):
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

    storage_units = []
    for storage_type_id, datasets in storage_types.items():
        storage_type = index.storage.types.get(storage_type_id)
        _LOG.info('Storing %s datasets using %s', datasets, storage_type)
        storage_units += create_storage_units(datasets, storage_type, executor=executor)

    index.storage.add_many(executor.result(value) for value in storage_units)


def find_storage_types_for_datasets(datasets, index=None):
    """
    Find matching storage_types for datasets

    Return a dictionary, keys are storage_type_ids, values are a list of datasets

    :type datasets: list[datacube.model.Dataset]
    :type index: datacube.index._api.Index
    :rtype dict[int, list[datacube.model.Dataset]]
    """
    # TODO: Move to storage-types/storage-mappings
    index = index or index_connect()

    storage_types = defaultdict(list)
    for dataset in datasets:
        dataset_storage_types = index.storage.types.get_for_dataset(dataset)
        if not dataset_storage_types:
            raise RuntimeError('No storage types found for %s dataset', dataset)
        for storage_type in dataset_storage_types:
            storage_types[storage_type.id_].append(dataset)
    return storage_types


def create_storage_units(datasets, storage_type, executor=SerialExecutor()):
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
        storage_units = executor.map(_create_storage_unit, tasks)
        return storage_units
    except:
        for task in tasks:
            _remove_storage_unit(task)
        raise


def _create_storage_unit(task):
    tile_index, storage_type, datasets = task
    filename = storage.generate_filename(tile_index, datasets, storage_type)
    return storage.create_storage_unit_from_datasets(tile_index, datasets, storage_type, filename)


def _remove_storage_unit(task):
    tile_index, storage_type, datasets = task
    filename = storage.generate_filename(tile_index, datasets, storage_type)
    try:
        os.unlink(str(_uri_to_local_path(filename)))
    except OSError:
        pass

# coding=utf-8
"""
Ingest datasets into the agdc.
"""
from __future__ import absolute_import

import logging

from . import model, storage
from .index import index_connect

_LOG = logging.getLogger(__name__)


def ingest(dataset_path):
    dataset = model.Dataset.from_path(dataset_path)

    index = index_connect()

    if not index.datasets.has(dataset):
        was_indexed = index.datasets.add(dataset)
        if was_indexed:
            _LOG.info('Indexed %s', dataset_path)

    _write_missing_storage_units(index, dataset)

    _LOG.info('Completed dataset %s', dataset_path)


def _write_missing_storage_units(index, dataset):
    """
    Ensure all storage units have been written for the dataset.
    :type index: datacube.index._api.Index
    :type dataset: datacube.model.Dataset
    """
    # TODO: Query for missing storage units, not all storage units.
    storage_mappings = index.mappings.get_for_dataset(dataset)
    _LOG.debug('Storage mappings: %s', storage_mappings)
    if storage_mappings:
        storage_units = storage.store(storage_mappings, dataset)
        index.storage.add_many(storage_units)

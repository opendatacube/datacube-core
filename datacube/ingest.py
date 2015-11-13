# coding=utf-8
"""
Ingest datasets into the agdc.
"""
from __future__ import absolute_import

import logging

from . import model, storage
from .index import data_index_connect, data_management_connect

_LOG = logging.getLogger(__name__)


def ingest(dataset_path):

    dataset = model.Dataset.from_path(dataset_path)

    index = data_index_connect()
    config = data_management_connect()

    if not index.contains_dataset(dataset):
        was_indexed = index.ensure_dataset(dataset)
        if was_indexed:
            _LOG.info('Indexed %s', dataset_path)

    _write_missing_storage_units(config, index, dataset)

    _LOG.info('Completed dataset %s', dataset_path)


def _write_missing_storage_units(config, index, dataset):
    """
    Ensure all storage units have been written for the dataset.
    """
    # TODO: Query for missing storage units, not all storage units.
    storage_mappings = config.get_storage_mappings_for_dataset(dataset.metadata_doc)
    _LOG.debug('Storage mappings: %s', storage_mappings)
    if storage_mappings:
        storage_units = storage.store(storage_mappings, dataset)
        index.add_storage_units(storage_units)

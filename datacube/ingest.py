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
            storage_mappings = config.get_storage_mappings_for_dataset(dataset.metadata_doc)
            _LOG.debug('Storage mappings: %s', storage_mappings)

            storage_segments = storage.store(storage_mappings, dataset)

            # index.add_storage_segments(dataset, storage_segments)

            _LOG.info('Ingested %s', dataset_path)
        else:
            _LOG.info('Skipping just-ingested dataset %s', dataset_path)
    else:
        # TODO: Check/write any missing storage records for the dataset?
        _LOG.info('Skipping already-ingested dataset %s', dataset_path)

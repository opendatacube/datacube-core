# coding=utf-8
"""
Ingest datasets into the agdc.
"""
from __future__ import absolute_import

import logging

from .index import data_index_connect, data_management_connect
from . import model

_LOG = logging.getLogger(__name__)


def ingest(dataset_path):
    # config = load_config()

    dataset = model.Dataset.from_path(dataset_path)

    index = data_index_connect()
    config = data_management_connect()

    if not index.contains_dataset(dataset):
        was_indexed = index.ensure_dataset(dataset)

        storage_mappings = config.get_storage_mappings_for_dataset(dataset.metadata_doc)

        for mapping in storage_mappings:
            storage_type = mapping.storage_type
            measurements = mapping.measurements

            # storage_records = storage.store(storage_type, measurements, dataset)

        # index.add_storage_records(dataset, storage_records)
        if was_indexed:
            _LOG.info('Ingested %s', dataset_path)
    else:
        # TODO: Check/write any missing storage records for the dataset?
        _LOG.info('Skipping already-ingested dataset %s', dataset_path)

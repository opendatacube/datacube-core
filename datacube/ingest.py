# coding=utf-8
"""
Ingest datasets into the agdc.
"""
from __future__ import absolute_import

import logging

from . import index
from . import model

_LOG = logging.getLogger(__name__)


def ingest(dataset_path):
    # config = load_config()

    dataset = model.Dataset.from_path(dataset_path)

    if not index.contains_dataset(dataset):
        index.add_dataset_simple(dataset)

        # dataset_storage_config = config.storage_config(dataset)

        # storage_records = storage.store(config.ingest_config, dataset_storage_config, dataset)
        # index.add_storage_records(dataset, storage_records)
        _LOG.info('Ingested %r', dataset_path)
    else:
        # TODO: Check/write any missing storage records for the dataset?
        _LOG.info('Skipping already-ingested dataset %r', dataset_path)

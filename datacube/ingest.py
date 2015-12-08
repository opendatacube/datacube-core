# coding=utf-8
"""
Ingest datasets into the agdc.
"""
from __future__ import absolute_import

import logging

from datacube import ui
from . import storage
from .index import index_connect

_LOG = logging.getLogger(__name__)


def _expected_metadata_path(dataset_path):
    """
    Get the path where we expect a metadata file for this dataset.

    (only supports eo metadata docs at the moment)

    :type dataset_path: pathlib.Path
    :rtype: Path
    """

    # - A dataset directory expects file 'agdc-metadata.yaml'.
    # - A dataset file expects a sibling file with suffix '.agdc-md.yaml'.
    # - Otherwise they gave us the metadata file directly.

    if dataset_path.is_file():
        if ui.is_supported_document_type(dataset_path):
            return dataset_path

        return dataset_path.parent.joinpath('{}.agdc-md.yaml'.format(dataset_path.name))

    elif dataset_path.is_dir():
        return dataset_path.joinpath('agdc-metadata.yaml')

    raise ValueError('Unhandled path type for %r' % dataset_path)


def index_datasets(path, index=None):
    """
    Discover datasets in path and add them to the index

    :type path: pathlib.Path
    :type index: datacube.index._api.Index
    :rtype: list[datacube.model.Dataset]
    """
    index = index or index_connect()

    metadata_path = _expected_metadata_path(path)
    if not metadata_path or not metadata_path.exists():
        raise ValueError('No supported metadata docs found for dataset {}'.format(path))

    datasets = [index.datasets.add(metadata_doc, metadata_path)
                for metadata_path, metadata_doc
                in ui.read_documents(metadata_path)]
    _LOG.info('Indexed datasets %s', path)
    return datasets


def store_datasets(datasets, index=None):
    """
    Find matching mappings for datasets
    Create storage units for datasets as per the mappings
    Add storage units to the index

    :type datasets: list[datacube.model.Dataset]
    :type index: datacube.index._api.Index
    """
    index = index or index_connect()

    storage_mappings = {}
    for dataset in datasets:
        for storage_mapping in index.mappings.get_for_dataset(dataset):
            storage_mappings.setdefault(storage_mapping.id_, []).append(dataset)

    for storage_mapping_id, datasets in storage_mappings.items():
        storage_mapping = index.mappings.get(storage_mapping_id)
        _LOG.info('Using %s to store %s datasets', storage_mapping, datasets)
        store_datasets_with_mapping(datasets, storage_mapping, index)


def store_datasets_with_mapping(datasets, storage_mapping, index=None):
    """
    Create storage units for datasets using storage_mapping
    Add storage units to the index

    :type datasets: list[datacube.model.Dataset]
    :type storage_mapping: datacube.model.StorageMapping
    :type index: datacube.index._api.Index
    """
    index = index or index_connect()

    storage_units = storage.store_datasets_with_mapping(datasets, storage_mapping)
    index.storage.add_many(storage_units)

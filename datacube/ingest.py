# coding=utf-8
"""
Ingest datasets into the agdc.
"""
from __future__ import absolute_import

import logging

import yaml

from . import model, storage
from .index import index_connect

_LOG = logging.getLogger(__name__)


def _expected_metadata_path(dataset_path):
    """
    Get the path where we expect a metadata file for this dataset.

    (only supports eo metadata docs at the moment)

    :type dataset_path: pathlib.Path
    :rtype: Path
    """

    # - A dataset directory expects file 'ga-metadata.yaml'.
    # - A dataset file expects a sibling file with suffix '.ga-md.yaml'.
    # - Otherwise they gave us the metadata file directly.

    if dataset_path.is_file():
        if dataset_path.suffix in ('.yaml', '.yml', '.json'):
            return dataset_path

        return dataset_path.parent.joinpath('{}.ga-md.yaml'.format(dataset_path.name))

    elif dataset_path.is_dir():
        return dataset_path.joinpath('ga-metadata.yaml')

    raise ValueError('Unhandled path type for %r' % dataset_path)


def _dataset_from_path(index, path):
    """
    :type index: datacube.index._api.Index
    :type path: pathlib.Path
    :rtype: datacube.model.Dataset
    """
    metadata_path = _expected_metadata_path(path)
    if not metadata_path or not metadata_path.exists():
        raise ValueError('No supported metadata docs found for dataset {}'.format(path))

    if metadata_path.suffix in ('.yaml', '.yml'):
        metadata_doc = yaml.load(open(str(metadata_path), 'r'))
    else:
        raise ValueError('Only yaml metadata is supported at the moment (provided {})'.format(metadata_path.suffix))

    collection = index.collections.get_for_dataset_doc(metadata_doc)
    if not collection:
        _LOG.debug('Failed match on dataset doc %r', metadata_doc)
        raise ValueError('No collection matched for dataset.')

    _LOG.info('Matched collection %r (%s)', collection.name, collection.id_)
    return model.Dataset(collection, metadata_doc, metadata_path)


def ingest(path, index=None):
    """
    :type index: datacube.index._api.Index
    :type path: pathlib.Path
    :rtype: datacube.model.Dataset
    """
    index = index or index_connect()

    dataset = _dataset_from_path(index, path)

    if not index.datasets.has(dataset):
        was_indexed = index.datasets.add(dataset)
        if was_indexed:
            _LOG.info('Indexed %s', path)

    _write_missing_storage_units(index, dataset)

    _LOG.info('Completed dataset %s', path)


def _write_missing_storage_units(index, dataset):
    """
    Ensure all storage units have been written for the dataset.
    :type index: datacube.index._api.Index
    :type dataset: datacube.model.Dataset
    """
    # TODO: Query for missing storage units, not all storage units.
    storage_mappings = index.mappings.get_for_dataset(dataset)
    _LOG.info('%s applicable storage mapping(s)', len(storage_mappings))
    _LOG.debug('Storage mappings: %s', storage_mappings)
    if storage_mappings:
        storage_units = storage.store(storage_mappings, dataset)
        index.storage.add_many(storage_units)

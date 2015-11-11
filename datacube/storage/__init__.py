# coding=utf-8
"""
Storage of datasets
"""
from __future__ import absolute_import

import logging
import os

from pathlib import Path

from datacube.model import StorageSegment

_LOG = logging.getLogger(__name__)


# Dummy, incorrect placeholder implementations added by Jeremy.
# This should instead be an import from other modules in this package...
def store(storage_mappings, dataset):
    """

    :type storage_mappings: list[datacube.model.StorageMapping]
    :type dataset: datacube.model.Dataset
    :return:
    """
    _LOG.info('%s mappings for dataset %s', len(storage_mappings), dataset.id)

    for mapping in storage_mappings:
        storage_type = mapping.storage_type
        if storage_type.driver != 'NetCDF CF':
            raise RuntimeError('Unknown storage driver')

        # dict of measurements ('bands') and their settings for the storage driver.
        # eg.
        #  '10':
        #     dtype: int16
        #     fill_value: -999
        #     resampling_method: cubic
        #     varname: band_10
        # '20':
        #     dtype: int16
        #     fill_value: -999
        #     resampling_method: cubic
        #     varname: band_20
        _LOG.debug('Measurements: %r', mapping.measurements)

        # Loop through band paths as an example.
        dataset_measurements = _get_doc_offset(mapping.dataset_measurements_offset, dataset.metadata_doc)
        for band_id, band_descriptor in dataset_measurements.items():
            # The path of a band is relative to the dataset path.
            dataset_path = dataset.metadata_path.parent
            band_path = dataset_path.joinpath(band_descriptor['path'])

            _LOG.debug('Band path: %s', band_path)
            assert os.path.exists(band_path)

        _LOG.debug('Storage type description: %r', storage_type.descriptor)

        # Return descriptions of written 'tiles'/'segments'.
        # We don't have a representation of a storage unit (just file path). Is that a problem?

        # Two segments inside one storage unit.
        yield StorageSegment(
            dataset.id,
            storage_type,
            {'something': {'x': 234}},
            Path('/tmp/something1.nc')
        )
        yield StorageSegment(
            dataset.id,
            storage_type,
            {'something': {'x': 235}},
            Path('/tmp/something1.nc')
        )


def _get_doc_offset(offset, document):
    """
    :type offset: list[str]
    :type document: dict

    >>> _get_doc_offset(['a'], {'a': 4})
    4
    >>> _get_doc_offset(['a', 'b'], {'a': {'b': 4}})
    4
    >>> _get_doc_offset(['a'], {})
    Traceback (most recent call last):
    ...
    KeyError: 'a'
    """
    value = document
    for key in offset:
        value = value[key]
    return value


__all__ = ['store']

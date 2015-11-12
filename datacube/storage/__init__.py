# coding=utf-8
"""
Storage of datasets
"""
from __future__ import absolute_import

import logging

from pathlib import Path

from datacube.model import StorageUnit
from datacube.storage.ingester import SimpleObject  # TODO: Use actual classes
from datacube.storage.ingester import crazy_band_tiler

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

        # TODO: sort the location thing out
        storage_type.descriptor["filename_format"] = '/short/u46/gxr547/gregcube/'+mapping.location_offset
        dataset_measurements = _get_doc_offset(mapping.dataset_measurements_offset, dataset.metadata_doc)
        for measurement_id, measurement_descriptor in mapping.measurements.items():
            # Get the corresponding measurement/band from the dataset.
            band_descriptor = dataset_measurements[measurement_id]

            # The path of a band is relative to the dataset path.
            dataset_path = dataset.metadata_path.parent
            band_path = dataset_path.joinpath(band_descriptor['path'])

            _LOG.debug('Band path: %s', band_path)
            assert band_path.exists()

            # How to store this band/measurement:
            _LOG.debug('Measurement descriptor: %r', measurement_descriptor)
            band_info = SimpleObject(**measurement_descriptor)  # TODO: Use actual classes
            input_filename = str(band_path)
            dataset_metadata = dataset.metadata_doc

            time_value = dataset_metadata['extent']['center_dt']

            for filename, metadata in crazy_band_tiler(band_info, input_filename, storage_type.descriptor,
                                                       time_value, dataset_metadata):
                yield StorageUnit(
                    dataset.id,
                    storage_type,
                    metadata,
                    Path(filename)
                )

        _LOG.debug('Storage type description: %r', storage_type.descriptor)

        # Return descriptions of written 'tiles'/'segments'.
        # We don't have a representation of a storage unit (just file path). Is that a problem?

        # Two segments inside one storage unit.


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

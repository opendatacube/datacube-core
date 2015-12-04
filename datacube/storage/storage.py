# coding=utf-8
"""
Create/store dataset data into storage units based on the provided storage mappings
"""
from __future__ import absolute_import

import logging

import dateutil.parser

from datacube import compat
from datacube.model import StorageUnit
from datacube.storage.ingester import storage_unit_tiler, SimpleObject
from datacube.storage.netcdf_indexer import index_netcdfs

_LOG = logging.getLogger(__name__)


def store(storage_mappings, dataset):
    """

    :type storage_mappings: list[datacube.model.StorageMapping]
    :type dataset: datacube.model.Dataset
    :return:
    """
    _LOG.info('%s mappings for dataset %s', len(storage_mappings), dataset.id)
    collection = dataset.collection

    result = []
    for mapping in storage_mappings:
        storage_unit_filenames = set()
        storage_type = mapping.storage_type
        if mapping.storage_type.driver != 'NetCDF CF':
            raise RuntimeError('Unknown storage driver')

        # TODO: hack? tiler takes way too many params as it is...
        if not mapping.storage_pattern.startswith('file://'):
            raise RuntimeError('URI protocol is not supported (yet): %s' % mapping.storage_pattern)
        storage_type.descriptor["filename_format"] = mapping.storage_pattern[7:]

        dataset_measurements = collection.dataset_reader(dataset.metadata_doc).measurements_dict
        for measurement_id, measurement_descriptor in mapping.measurements.items():
            # Get the corresponding measurement/band from the dataset.
            dataset_measurement_descriptor = dataset_measurements[measurement_id]

            # The path of an input file is relative to the dataset path.
            input_filename = dataset.metadata_path.parent.joinpath(dataset_measurement_descriptor['path'])

            _LOG.debug('Input filename: %s', input_filename)
            assert input_filename.exists()

            # How to store this band/measurement:
            _LOG.debug('Measurement descriptor: %r', measurement_descriptor)
            for filename in storage_unit_tiler(SimpleObject(**measurement_descriptor),  # TODO: Use actual classes
                                               input_filename=str(input_filename),
                                               storage_type=storage_type,
                                               # TODO: Use doc fields, rather than parsing manually.
                                               time_value=_as_datetime(
                                                   dataset.metadata_doc['extent']['center_dt']
                                               ),
                                               dataset_metadata=dataset.metadata_doc):  # Just for making out filename
                storage_unit_filenames.add(filename)

        _LOG.debug('Storage type description: %r', storage_type.descriptor)

        created_storage_units = index_netcdfs(storage_unit_filenames)
        _LOG.debug('Wrote storage units: %s', created_storage_units)
        result += [
            StorageUnit(
                [dataset.id],
                mapping,
                unit_descriptor,
                mapping.local_path_to_location_offset('file://' + path)
            )
            for path, unit_descriptor in created_storage_units.items()
            ]

    return result


def _as_datetime(field):
    if isinstance(field, compat.string_types):
        return dateutil.parser.parse(field)
    return field

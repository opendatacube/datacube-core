# coding=utf-8
"""
Module
"""
from __future__ import absolute_import

import os
import cachetools

from datacube.config import SystemConfig
from datacube.index._core_db import Db
from datacube.model import StorageMapping, StorageType, DatasetMatcher


def connect(config=SystemConfig.find()):
    """
    Connect to the .
    :type config: datacube.config.SystemConfig
    :rtype: DataManagement
    """
    return DataManagement(Db.connect(config.db_hostname, config.db_database, config.db_username, config.db_port),
                          config)


class DataManagement(object):
    def __init__(self, db, config):
        """
        :type db: datacube.index._core_db.Db
        :type config: datacube.config.SystemConfig
        """
        self.db = db
        self.config = config
        self._cached_storage_types = {}

    @cachetools.cached(cachetools.TTLCache(100, 60))
    def _get_storage_type(self, id_):
        _storage_type = self.db.get_storage_type(id_)
        return StorageType(_storage_type['driver'], _storage_type['name'], _storage_type['descriptor'])

    def get_storage_mappings_for_dataset(self, dataset_metadata):
        mappings = self.db.get_storage_mappings(dataset_metadata)
        
        def resolve_location(location, offset):
            return os.path.join(self.config.location_mappings[location], offset)
        return [
            StorageMapping(
                self._get_storage_type(mapping['storage_type_ref']),
                mapping['name'],
                DatasetMatcher(mapping['dataset_metadata']),
                mapping['measurements'],
                mapping['dataset_measurements_key'],
                resolve_location(mapping['location_name'], mapping['location_offset'])
            )
            for mapping in mappings
        ]

    def ensure_storage_type(self, descriptor):
        """
        Ensure a storage type is in the index (add it if needed).

        :return:
        """
        # TODO: Validate (Against JSON Schema?)
        name = descriptor['name']
        driver = descriptor['driver']
        self.db.ensure_storage_type(driver, name, descriptor)

    def ensure_storage_mapping(self, descriptor):
        """
        Take a parsed storage mapping file and ensure it's in the index.
        (update if needed)

        :return:
        """
        # TODO: Validate doc (Against JSON Schema?)
        name = descriptor['name']
        driver = descriptor['driver']
        dataset_metadata = descriptor['match']['metadata']
        storage_mappings = descriptor['storage']
        with self.db.begin() as transaction:
            for mapping in storage_mappings:
                storage_type_name = mapping['name']
                location_name = mapping['location_name']
                location_offset = mapping['location_offset']
                measurements = mapping['measurements']
                self.db.ensure_storage_mapping(
                    driver,
                    storage_type_name,
                    name,
                    location_name,
                    location_offset,
                    dataset_metadata,
                    # The offset within an eodataset to find a band list.
                    ['image', 'bands'],
                    measurements
                )

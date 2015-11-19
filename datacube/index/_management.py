# coding=utf-8
"""
Access methods for storing dataset management & configuration.
"""
from __future__ import absolute_import

import cachetools

from datacube.config import SystemConfig
from datacube.model import StorageMapping, StorageType, DatasetMatcher, StorageUnit
from .db import Db


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
        return StorageType(_storage_type['driver'],
                           _storage_type['name'],
                           _storage_type['descriptor'],
                           id_=_storage_type['id'])

    def _make_storage_mapping(self, mapping):
        return StorageMapping(
            self._get_storage_type(mapping['storage_type_ref']),
            mapping['name'],
            DatasetMatcher(mapping['dataset_metadata']),
            mapping['measurements'],
            mapping['dataset_measurements_key'],
            self.config.location_mappings[mapping['location_name']],
            mapping['location_offset'],
            id_=mapping['id']
        )

    @cachetools.cached(cachetools.TTLCache(100, 60))
    def get_storage_mapping(self, id_):
        mapping = self.db.get_storage_mapping(id_)
        return self._make_storage_mapping(mapping)

    def get_storage_mappings_for_dataset(self, dataset_metadata):
        mappings = self.db.get_storage_mappings(dataset_metadata)
        return [self._make_storage_mapping(mapping) for mapping in mappings]

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
                self.db.ensure_storage_mapping(
                    driver,
                    mapping['name'],
                    name,
                    mapping['location_name'],
                    mapping['location_offset'],
                    dataset_metadata,
                    # The offset within an eodataset to find a band list.
                    ['image', 'bands'],
                    mapping['measurements']
                )

    # TODO: it's crazy for it to be here, but I need get_storage_mapping,
    # TODO: which needs _get_storage_type, which needs config...
    def get_storage_units(self):
        """
        :rtype: list[datacube.model.StorageUnit]
        """
        return [StorageUnit([], self.get_storage_mapping(su['storage_mapping_ref']), su['descriptor'], su['path'])
                for su in self.db.get_storage_units()]

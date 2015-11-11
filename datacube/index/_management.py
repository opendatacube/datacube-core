# coding=utf-8
"""
Module
"""
from __future__ import absolute_import

import cachetools

from datacube.config import UserConfig
from datacube.index._core_db import Db
from datacube.model import StorageMapping, StorageType, DatasetMatcher


def connect(config=UserConfig.find()):
    """
    Connect to the .
    :type config: datacube.config.UserConfig
    :rtype: DataManagement
    """
    return DataManagement(Db.connect(config.db_hostname, config.db_database))


class DataManagement(object):
    def __init__(self, db):
        """
        :type db: datacube.index._core_db.Db
        """
        self.db = db
        self._cached_storage_types = {}

    @cachetools.cached(cachetools.TTLCache(100, 60))
    def _get_storage_type(self, id_):
        _storage_type = self.db.get_storage_type(id_)
        return StorageType(_storage_type['driver'], _storage_type['name'], _storage_type['descriptor'])

    def get_storage_mappings_for_dataset(self, dataset_metadata):
        mappings = self.db.get_storage_mappings(dataset_metadata)
        return [
            StorageMapping(
                self._get_storage_type(mapping['storage_type_ref']),
                mapping['name'],
                DatasetMatcher(mapping['datasets_matching']),
                mapping['measurements'],
                mapping['dataset_measurements_key']
            )
            for mapping in mappings
        ]

    def ensure_storage_type(self, descriptor, driver):
        """
        Ensure a storage type is in the index (add it if needed).

        :return:
        """
        name = descriptor['name']
        self.db.ensure_storage_type(driver, name, descriptor)

    def ensure_storage_mapping(self, descriptor, driver):
        """
        Take a parsed storage mapping file and ensure it's in the index.
        (update if needed)

        :return:
        """
        name = descriptor['name']
        datasets_matching = descriptor['match']['metadata']
        storage_mappings = descriptor['storage']
        with self.db.begin() as transaction:
            for mapping in storage_mappings:
                storage_type_name = mapping['name']
                measurements = mapping['measurements']
                self.db.ensure_storage_mapping(
                    driver,
                    storage_type_name,
                    name,
                    datasets_matching,
                    # The offset within an eodataset to find a band list.
                    ['image', 'bands'],
                    measurements
                )

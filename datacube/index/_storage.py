# coding=utf-8
"""
Module
"""
from __future__ import absolute_import

import logging

import cachetools

from datacube.index.fields import _build_expressions
from datacube.model import StorageUnit, StorageType, DatasetMatcher, StorageMapping

_LOG = logging.getLogger(__name__)


class StorageUnitResource(object):
    def __init__(self, db, storage_mapping_resource):
        """
        :type db: datacube.index.postgres._api.PostgresDb
        :type storage_mapping_resource:
        """
        self._db = db
        self.storage_mappings = storage_mapping_resource

    def get(self, id_):
        raise RuntimeError('TODO: implement')

    def add_many(self, storage_units):
        """
        :type storage_units: list[datacube.model.StorageUnit]
        """
        for unit in storage_units:
            with self._db.begin() as transaction:
                unit_id = self._db.add_storage_unit(
                    unit.path,
                    unit.dataset_ids,
                    unit.descriptor,
                    unit.storage_mapping.id_,
                )
                _LOG.debug('Indexed unit %s @ %s', unit_id, unit.path)

    def add(self, storage_unit):
        """
        :type storage_unit: datacube.model.StorageUnit
        """
        return self.add_many([storage_unit])

    def get_field(self, name):
        """
        :type name: str
        :rtype: datacube.index.fields.Field
        """
        return self._db.get_storage_field('eo', name)

    def get_storage_field_with_fallback(self, name):
        """
        :type name: str
        :rtype: datacube.index.fields.Field
        """
        val = self.get_field(name)
        return val if val is not None else self._db.get_dataset_field('eo', name)

    def search(self, *expressions, **query):
        """
        TODO: Return objects
        :type expressions: list[datacube.index.fields.Expression]
        """
        query_exprs = tuple(_build_expressions(self.get_field, **query))
        return self._db.search_storage_units((expressions + query_exprs))

    def search_eager(self, *expressions, **query):
        """
        :type expressions: list[datacube.index.fields.Expression]
        """
        return list(self.search(*expressions, **query))

    def _make(self, query_results):
        """
        :rtype: list[datacube.model.StorageUnit]
        """
        return (StorageUnit(
            # TODO: move dataset ids out of this class?
            [],
            self.storage_mappings.get(su['storage_mapping_ref']),
            su['descriptor'],
            su['path']
        ) for su in self._db.get_storage_units())


class StorageTypeResource(object):
    def __init__(self, db):
        """
        :type db: datacube.index.postgres._api.PostgresDb
        """
        self._db = db

    @cachetools.cached(cachetools.TTLCache(100, 60))
    def get(self, id_):
        storage_type = self._db.get_storage_type(id_)
        return StorageType(
            storage_type['driver'],
            storage_type['name'],
            storage_type['descriptor'],
            id_=storage_type['id']
        )

    def add(self, descriptor):
        """
        Ensure a storage type is in the index (add it if needed).

        :return:
        """
        # TODO: Validate (Against JSON Schema?)
        name = descriptor['name']
        driver = descriptor['driver']
        self._db.ensure_storage_type(driver, name, descriptor)


class StorageMappingResource(object):
    def __init__(self, db, storage_type_resource, host_config):
        """
        :type db: datacube.index.postgres._api.PostgresDb
        """
        self._db = db
        self._storage_types = storage_type_resource
        self._host_config = host_config

    def add(self, descriptor):
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
        with self._db.begin() as transaction:
            for mapping in storage_mappings:
                self._db.ensure_storage_mapping(
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

    def _make_storage_mapping(self, mapping):
        return StorageMapping(
            self._storage_types.get(mapping['storage_type_ref']),
            mapping['name'],
            DatasetMatcher(mapping['dataset_metadata']),
            mapping['measurements'],
            mapping['dataset_measurements_key'],
            self._host_config.location_mappings[mapping['location_name']],
            mapping['location_offset'],
            id_=mapping['id']
        )

    @cachetools.cached(cachetools.TTLCache(100, 60))
    def get(self, id_):
        mapping = self._db.get_storage_mapping(id_)
        return self._make_storage_mapping(mapping)

    def get_for_dataset(self, dataset):
        mappings = self._db.get_storage_mappings(dataset.metadata_doc)
        return [self._make_storage_mapping(mapping) for mapping in mappings]

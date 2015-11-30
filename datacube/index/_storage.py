# coding=utf-8
"""
API for storage indexing, access and search.
"""
from __future__ import absolute_import

import logging

import cachetools

from datacube.index.fields import to_expressions
from datacube.model import StorageUnit, StorageType, DatasetMatcher, StorageMapping

_LOG = logging.getLogger(__name__)


class StorageUnitResource(object):
    def __init__(self, db, storage_mapping_resource, collection_resource, local_config):
        """
        :type db: datacube.index.postgres._api.PostgresDb
        :type storage_mapping_resource: StorageMappingResource
        :type collection_resource: CollectionResource
        :type local_config: datacube.config.LocalConfig
        """
        self._db = db
        self._storage_mapping_resource = storage_mapping_resource
        self._collection_resource = collection_resource

        self._config = local_config

    def get(self, id_):
        raise RuntimeError('TODO: implement')

    def add_many(self, storage_units):
        """
        :type storage_units: list[datacube.model.StorageUnit]
        """
        with self._db.begin() as transaction:
            for unit in storage_units:
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

    def get_field(self, name, collection_name=None):
        """
        :type name: str
        :param collection_name: Collection to search, or None for default
        :rtype: datacube.index.fields.Field
        """
        if collection_name is None:
            collection_name = self._config.default_collection_name

        collection = self._collection_resource.get_by_name(collection_name)
        return collection.storage_unit_search_fields.get(name)

    def get_field_with_fallback(self, name, collection_name=None):
        """
        :type name: str
        :rtype: datacube.index.fields.Field
        :param collection_name: Collection to search, or None for default
        """
        if collection_name is None:
            collection_name = self._config.default_collection_name

        collection = self._collection_resource.get_by_name(collection_name)
        val = collection.storage_unit_search_fields.get(name)

        return val if val is not None else collection.dataset_search_fields.get(name)

    def search(self, *expressions, **query):
        """
        :type expressions: tuple[datacube.index.fields.PgExpression]
        :type query: dict[str,str|float|datacube.model.Range]
        :rtype list[datacube.model.StorageUnit]
        """
        query_exprs = tuple(to_expressions(self.get_field_with_fallback, **query))
        return self._make(self._db.search_storage_units((expressions + query_exprs)))

    def search_eager(self, *expressions, **query):
        """
        :type expressions: list[datacube.index.fields.Expression]
        :type query: dict[str,str|float|datacube.model.Range]
        :rtype list[datacube.model.StorageUnit]
        """
        return list(self.search(*expressions, **query))

    def _make(self, query_results):
        """
        :rtype: list[datacube.model.StorageUnit]
        """
        return (StorageUnit(
            # TODO: move dataset ids out of this class?
            [],
            self._storage_mapping_resource.get(su['storage_mapping_ref']),
            su['descriptor'],
            su['path']
        ) for su in query_results)


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

        :type descriptor: dict
        """
        # TODO: Validate (Against JSON Schema?)
        name = descriptor['name']
        driver = descriptor['driver']
        self._db.ensure_storage_type(driver, name, descriptor)


class StorageMappingResource(object):
    def __init__(self, db, storage_type_resource, host_config):
        """
        :type db: datacube.index.postgres._api.PostgresDb
        :type storage_type_resource: StorageTypeResource
        :type host_config: datacube.config.LocalConfig
        """
        self._db = db
        self._storage_types = storage_type_resource
        self._host_config = host_config

    def add(self, descriptor):
        """
        Take a parsed storage mapping file and ensure it's in the index.
        (update if needed)

        :type descriptor: dict
        """
        # TODO: Validate doc (Against JSON Schema?)
        name = descriptor['name']
        driver = descriptor['driver']
        dataset_metadata = descriptor['match']['metadata']
        storage_mappings = descriptor['storage']
        with self._db.begin() as transaction:
            for mapping in storage_mappings:
                self._db.ensure_storage_mapping(
                    mapping['name'],
                    name,
                    mapping['location_name'],
                    mapping['file_path_template'],
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
            mapping['file_path_template'],
            id_=mapping['id']
        )

    @cachetools.cached(cachetools.TTLCache(100, 60))
    def get(self, id_):
        mapping = self._db.get_storage_mapping(id_)
        return self._make_storage_mapping(mapping)

    def get_for_dataset(self, dataset):
        mappings = self._db.get_storage_mappings(dataset.metadata_doc)
        return [self._make_storage_mapping(mapping) for mapping in mappings]

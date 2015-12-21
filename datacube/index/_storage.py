# coding=utf-8
"""
API for storage indexing, access and search.
"""
from __future__ import absolute_import

import copy
import logging

import cachetools

from datacube.model import StorageUnit, StorageType, DatasetMatcher, StorageMapping
from . import fields

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
        return self.get_fields().get(name)

    def get_field_with_fallback(self, name, collection_name=None):
        """
        :type name: str
        :rtype: datacube.index.fields.Field
        :param collection_name: Collection to search, or None for default
        """
        if collection_name is None:
            collection_name = self._config.default_collection_name

        collection = self._collection_resource.get_by_name(collection_name)
        val = collection.storage_fields.get(name)

        return val if val is not None else collection.dataset_fields.get(name)

    def get_fields(self, collection_name=None):
        """
        :type collection_name: str
        :rtype: dict[str, datacube.index.fields.Field]
        """
        if collection_name is None:
            collection_name = self._config.default_collection_name
        collection = self._collection_resource.get_by_name(collection_name)
        return collection.storage_fields

    def search(self, *expressions, **query):
        """
        :type expressions: tuple[datacube.index.fields.PgExpression]
        :type query: dict[str,str|float|datacube.model.Range]
        :rtype list[datacube.model.StorageUnit]
        """
        query_exprs = tuple(fields.to_expressions(self.get_field_with_fallback, **query))
        return self._make(self._db.search_storage_units((expressions + query_exprs)))

    def search_summaries(self, *expressions, **query):
        query_exprs = tuple(fields.to_expressions(self.get_field_with_fallback, **query))

        return (
            dict(fs) for fs in
            self._db.search_storage_units(
                (expressions + query_exprs),
                select_fields=tuple(self.get_fields().values())
            )
        )

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
            # An offset from the location (ie. a URL fragment):
            su['path'],
            id_=su['id']
        ) for su in query_results)


class StorageMappingResource(object):
    def __init__(self, db, host_config):
        """
        :type db: datacube.index.postgres._api.PostgresDb
        :type storage_type_resource: StorageTypeResource
        :type host_config: datacube.config.LocalConfig
        """
        self._db = db
        # self._storage_types = storage_type_resource
        self._host_config = host_config

    def add(self, descriptor):
        """
        Take a parsed storage mapping file and ensure it's in the index.
        (update if needed)

        :type descriptor: dict
        """
        # TODO: Validate doc (Against JSON Schema?)
        name = descriptor['name']
        dataset_metadata = descriptor['match']['metadata']
        description = descriptor.get('description')
        roi = descriptor.get('roi')

        with self._db.begin() as transaction:
            location_name = descriptor['location_name']
            file_path_template = descriptor['file_path_template']
            measurements_doc = descriptor['measurements']
            storage_type = descriptor['storage']

            existing = self._db.get_storage_mapping_by_name(name)
            if existing:
                # They've passed us the same storage mapping again. Make sure it matches what we have:
                fields.check_field_equivalence(
                    [
                        ('location_name', location_name, existing.location_name),
                        ('file_path_template', file_path_template, existing.file_path_template),
                        ('measurements', measurements_doc, existing.measurements),
                        ('storage_type', storage_type, existing.storage_type),
                        ('roi', roi, existing.roi)
                    ],
                    'Storage mapping {}'.format(name)
                )
            else:
                self._db.ensure_storage_mapping(
                    name,
                    location_name,
                    file_path_template,
                    dataset_metadata,
                    measurements_doc,
                    storage_type,
                    description=description,
                    roi=roi
                )

    def _make(self, mapping):
        """
        :rtype: datacube.model.StorageMapping
        """
        return StorageMapping(
            storage_type=StorageType(mapping['storage_type']),
            name=mapping['name'],
            description=mapping['description'],
            match=DatasetMatcher(mapping['dataset_metadata']),
            measurements=mapping['measurements'],
            location=self._host_config.location_mappings[mapping['location_name']],
            filename_pattern=mapping['file_path_template'],
            roi=mapping['roi'],
            id_=mapping['id']
        )

    @cachetools.cached(cachetools.TTLCache(100, 60))
    def get(self, id_):
        """
        :rtype: datacube.model.StorageMapping
        """
        mapping = self._db.get_storage_mapping(id_)
        if not mapping:
            return None
        return self._make(mapping)

    def get_for_dataset(self, dataset):
        """
        :rtype: list[datacube.model.StorageMapping]
        """
        return self.get_for_dataset_doc(dataset.metadata_doc)

    def get_for_dataset_doc(self, dataset_doc):
        """
        :rtype: list[datacube.model.StorageMapping]
        """
        return [self._make(mapping) for mapping in self._db.get_storage_mappings(dataset_doc)]

    def get_by_name(self, name):
        """
        :rtype: datacube.model.StorageMapping
        """
        mapping_res = self._db.get_storage_mapping_by_name(name)
        if not mapping_res:
            return None
        return self._make(mapping_res)

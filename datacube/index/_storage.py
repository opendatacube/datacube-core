# coding=utf-8
"""
API for storage indexing, access and search.
"""
from __future__ import absolute_import

import logging
import pathlib

import cachetools
import jsonschema
import yaml

from datacube.index.fields import InvalidDocException
from datacube.model import StorageUnit, StorageType, DatasetMatcher
from . import fields

MAPPING_SCHEMA_PATH = pathlib.Path(__file__).parent.joinpath('mapping-schema.yaml')

_LOG = logging.getLogger(__name__)


def _ensure_valid(descriptor):
    try:
        jsonschema.validate(
            descriptor,
            yaml.safe_load(MAPPING_SCHEMA_PATH.open('r'))
        )
    except jsonschema.ValidationError as e:
        raise InvalidDocException(e.message)


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
                    unit.storage_type.id_,
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
        metadata_type = collection.metadata_type
        val = metadata_type.storage_fields.get(name)

        return val if val is not None else metadata_type.dataset_fields.get(name)

    def get_fields(self, collection_name=None):
        """
        :type collection_name: str
        :rtype: dict[str, datacube.index.fields.Field]
        """
        if collection_name is None:
            collection_name = self._config.default_collection_name
        collection = self._collection_resource.get_by_name(collection_name)
        return collection.metadata_type.storage_fields

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
        :type host_config: datacube.config.LocalConfig
        """
        self._db = db
        self._host_config = host_config

    def add(self, descriptor):
        """
        Take a parsed storage mapping file and ensure it's in the index.
        (update if needed)

        :type descriptor: dict
        """
        _ensure_valid(descriptor)

        name = descriptor['name']
        dataset_metadata = descriptor['match']['metadata']

        with self._db.begin() as transaction:
            existing = self._db.get_storage_mapping_by_name(name)
            if existing:
                # They've passed us the same storage mapping again. Make sure it matches what is stored.
                fields.check_doc_unchanged(
                    existing.descriptor,
                    descriptor,
                    'Storage mapping {}'.format(name)
                )
            else:
                self._db.ensure_storage_mapping(
                    name,
                    dataset_metadata,
                    descriptor
                )

    def _make(self, mapping_record):
        """
        :rtype: datacube.model.StorageType
        """
        descriptor = mapping_record['descriptor']
        _match = descriptor.get('match')
        if descriptor['location_name'] in self._host_config.location_mappings:
            location = self._host_config.location_mappings[descriptor['location_name']]
        else:
            raise Exception("Invalid configuration, storage mapping '{}' references unknown location '{}'".format(
                mapping_record['name'], descriptor['location_name']))

        return StorageType(
            definition=descriptor['storage'],
            name=mapping_record['name'],
            description=descriptor.get('description'),
            match=DatasetMatcher(mapping_record['dataset_metadata']),
            measurements=descriptor['measurements'],
            location=location,
            filename_pattern=descriptor['file_path_template'],
            roi=_match.get('roi') if _match else None,
            id_=mapping_record['id']
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

    def get_all(self):
        """
        :rtype: list[datacube.model.StorageMapping]
        """
        return [self._make(mapping) for mapping in self._db.get_all_storage_mappings()]

    def count(self):
        return self._db.count_mappings()

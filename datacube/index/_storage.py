# coding=utf-8
"""
API for storage indexing, access and search.
"""
from __future__ import absolute_import

import logging
import uuid

import cachetools
import dateutil.parser
import jsonschema
import pathlib
import yaml

try:
    from yaml import CSafeLoader as SafeLoader
except ImportError:
    from yaml import SafeLoader

from datacube.index.fields import InvalidDocException
from datacube.model import StorageUnit, StorageType
from . import fields

STORAGE_TYPE_SCHEMA_PATH = pathlib.Path(__file__).parent.joinpath('storage-type-schema.yaml')

_LOG = logging.getLogger(__name__)


def _ensure_valid(descriptor):
    try:
        jsonschema.validate(
            descriptor,
            yaml.load(STORAGE_TYPE_SCHEMA_PATH.open('r'), Loader=SafeLoader)
        )
    except jsonschema.ValidationError as e:
        raise InvalidDocException(e.message)


class StorageUnitResource(object):
    def __init__(self, db, storage_type_resource, dataset_types, metadata_types, local_config):
        """
        :type db: datacube.index.postgres._api.PostgresDb
        :type storage_type_resource: StorageTypeResource
        :type dataset_types: datacube.index._datasets.DatasetTypeResource
        :type local_config: datacube.config.LocalConfig
        """
        self._db = db
        self.types = storage_type_resource
        self._dataset_types = dataset_types
        self._metadata_types = metadata_types

        self._config = local_config

    def get(self, id_):
        raise RuntimeError('TODO: implement')

    def add_many(self, storage_units):
        """
        :type storage_units: list[datacube.model.StorageUnit]
        """
        with self._db.begin() as transaction:
            for unit in storage_units:
                self._add_unit(unit)

    def _add_unit(self, unit):
        if unit.id is None:
            unit.id = uuid.uuid4()

        was_newly_inserted = self._db.insert_dataset(
            unit.descriptor,
            unit.id,
            storage_type_id=unit.storage_type.id
        )
        # TODO: unit.size_bytes
        for source_id in unit.dataset_ids:
            source_dataset = self._db.get_dataset(source_id)
            if not source_dataset:
                raise ValueError('Unknown source dataset: %s' % source_id)

            self._db.insert_dataset_source(
                _unit_classifier(source_dataset['metadata']),
                unit.id,
                source_id
            )
        self._db.ensure_dataset_location(
            unit.id,
            pathlib.Path(unit.path).as_uri(), allow_replacement=True)
        _LOG.debug('Indexed unit %s @ %s', unit.id, unit.path)

    def add(self, storage_unit):
        """
        :type storage_unit: datacube.model.StorageUnit
        """
        return self.add_many([storage_unit])

    def replace(self, old_storage_units, new_storage_units):
        """
        :type old_storage_units: list[datacube.model.StorageUnit]
        :type new_storage_units: list[datacube.model.StorageUnit]
        """
        with self._db.begin() as transaction:
            for unit in old_storage_units:
                self._db.archive_storage_unit(unit.id)

            for unit in new_storage_units:
                self._add_unit(unit)
                _LOG.debug('Indexed unit %s @ %s', unit.id, unit.path)

    def get_field(self, name, collection_name=None):
        """
        :type name: str
        :param collection_name: Collection to search, or None for default
        :rtype: datacube.index.fields.Field
        """
        return self.get_fields().get(name)

    def get_overlaps(self, storage_type):
        return (r['id'] for r in self._db.get_storage_unit_overlap(storage_type))

    def get_field_with_fallback(self, name, collection_name=None):
        """
        :type name: str
        :rtype: datacube.index.fields.Field
        :param collection_name: Collection to search, or None for default
        """
        if collection_name is None:
            collection_name = self._config.default_collection_name

        collection = self._dataset_types.get_by_name(collection_name)
        if not collection:
            raise ValueError('Unknown collection: ' + collection_name)

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
        collection = self._dataset_types.get_by_name(collection_name)
        _storage_unit_type = self._dataset_types.get_by_name('storage_unit')
        return _storage_unit_type.dataset_fields

    def search(self, *expressions, **query):
        """
        Perform a search, returning results as StorageUnit objects.
        :type expressions: tuple[datacube.index.fields.PgExpression]
        :type query: dict[str,str|float|datacube.model.Range]
        :rtype list[datacube.model.StorageUnit]
        """
        query_exprs = tuple(fields.to_expressions(self.get_field_with_fallback, **query))
        return self._make(self._db.search_datasets((expressions + query_exprs), with_source_ids=True))

    def search_summaries(self, *expressions, **query):
        """
        Perform a search, returning just the search fields of each storage unit.

        :type query: dict[str,str|float|datacube.model.Range]
        :type expressions: tuple[datacube.index.fields.PgExpression]
        :rtype: dict
        """
        query_exprs = tuple(fields.to_expressions(self.get_field_with_fallback, **query))

        return (
            dict(fs) for fs in
            self._db.search_datasets(
                (expressions + query_exprs),
                select_fields=tuple(self.get_fields().values()),
                with_source_ids=True
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
            su['dataset_refs'],
            self.types.get(su['storage_type_ref']),
            su['descriptor'],
            # An offset from the location (ie. a URL fragment):
            size_bytes=su['size_bytes'],
            relative_path=su['path'],
            id_=su['id']
        ) for su in query_results)


class StorageTypeResource(object):
    def __init__(self, db, host_config):
        """
        :type db: datacube.index.postgres._api.PostgresDb
        :type host_config: datacube.config.LocalConfig
        """
        self._db = db
        self._host_config = host_config

    def add(self, definition):
        """
        Take a parsed storage definition file and ensure it's in the index.
        (update if needed)

        :type definition: dict
        """
        _ensure_valid(definition)

        name = definition['name']
        dataset_metadata = definition['match']['metadata']

        with self._db.begin() as transaction:
            existing = self._db.get_storage_type_by_name(name)
            if existing:
                # They've passed us the same storage mapping again. Make sure it matches what is stored.
                fields.check_doc_unchanged(
                    existing.definition,
                    definition,
                    'Storage type {}'.format(name)
                )
            else:
                self._db.ensure_storage_type(
                    name,
                    dataset_metadata,
                    definition
                )

    def _make(self, record):
        """
        :rtype: datacube.model.StorageType
        """
        definition = record['definition']
        if definition['location_name'] in self._host_config.location_mappings:
            definition['location'] = self._host_config.location_mappings[definition['location_name']]
        else:
            raise Exception("Invalid configuration, storage type '{}' references unknown location '{}'".format(
                record['name'], definition['location_name']))

        return StorageType(definition, id_=record['id'])

    @cachetools.cached(cachetools.TTLCache(100, 60))
    def get(self, id_):
        """
        :rtype: datacube.model.StorageType
        """
        record = self._db.get_storage_type(id_)
        if not record:
            return None
        return self._make(record)

    def get_for_dataset(self, dataset):
        """
        :rtype: list[datacube.model.StorageType]
        """
        return self.get_for_dataset_doc(dataset.metadata_doc)

    def get_for_dataset_doc(self, dataset_doc):
        """
        :rtype: list[datacube.model.StorageType]
        """
        return [self._make(record) for record in self._db.get_storage_types(dataset_doc)]

    def get_by_name(self, name):
        """
        :rtype: datacube.model.StorageType
        """
        record = self._db.get_storage_type_by_name(name)
        if not record:
            return None
        return self._make(record)

    def get_all(self):
        """
        :rtype: list[datacube.model.StorageType]
        """
        return [self._make(record) for record in self._db.get_all_storage_types()]

    def count(self):
        return self._db.count_storage_types()


def _unit_classifier(descriptor):
    """
    Get a classifier for the given source dataset.

    (A classifier is how we distinguish different sources: a storage unit may have many source datasets)

    We currently use the center time without microseconds.

    >>> _unit_classifier({"extent": {"center_dt": "2014-07-26T23:49:00.343853"}})
    '2014-07-26T23:49:00'
    """
    d = dateutil.parser.parse(descriptor['extent']['center_dt'])
    return d.replace(microsecond=0).isoformat()

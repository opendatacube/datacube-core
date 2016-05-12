# coding=utf-8
"""
API for storage indexing, access and search.
"""
from __future__ import absolute_import, division

import copy
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
    # This class is a temporary shim for backwards compatibility: we're ok with using the underlying dataset api.
    # pylint: disable=protected-access

    def __init__(self, db, storage_type_resource, dataset_types, datasets, metadata_types):
        """
        :type db: datacube.index.postgres._api.PostgresDb
        :type storage_type_resource: StorageTypeResource
        :type datasets: datacube.index._datasets.DatasetResource
        :type dataset_types: datacube.index._datasets.DatasetTypeResource
        :type local_config: datacube.config.LocalConfig
        """
        self._db = db
        self.types = storage_type_resource
        self._dataset_types = dataset_types
        self._metadata_types = metadata_types
        self._datasets = datasets

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
            unit.id = str(uuid.uuid4())

        dataset_type = self._dataset_types.get(unit.storage_type.target_dataset_type_id)

        merged_descriptor = {
            # Storage units stored this as a separate column, datasets have it in the metadata.
            'size_bytes': unit.size_bytes,
            'format': {
                'name': unit.storage_type.driver
            }
        }

        # Merge with expected dataset type metadata (old storage unit creation code does not include them)
        merged_descriptor.update(dataset_type.match)
        merged_descriptor.update(unit.descriptor)

        was_newly_inserted = self._db.insert_dataset(
            merged_descriptor,
            unit.id,
            dataset_type_id=unit.storage_type.target_dataset_type_id
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
        self._db.ensure_dataset_location(unit.id, unit.local_uri, allow_replacement=True)
        _LOG.debug('Indexed unit %s @ %s', unit.id, unit.local_uri)

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

    def get_fields(self):
        """
        :rtype: dict[str, datacube.index.fields.Field]
        """
        return self._metadata_types.get_by_name('storage_unit').dataset_fields

    def search(self, **query):
        """
        Perform a search, returning results as StorageUnit objects.
        :type query: dict[str,str|float|datacube.model.Range]
        :rtype list[datacube.model.StorageUnit]
        """
        query.update({'metadata_type': 'storage_unit'})

        return self._make(self._datasets._do_search(query, with_source_ids=True))

    def search_summaries(self, **query):
        """
        Perform a search, returning just the search fields of each storage unit.

        :type query: dict[str,str|float|datacube.model.Range]
        :rtype: dict
        """
        query.update({'metadata_type': 'storage_unit'})
        return (
            dict(fs) for fs in

            self._datasets._do_search(
                query,
                return_fields=True,
                with_source_ids=False
            )
        )

    def search_eager(self, **query):
        """
        :type query: dict[str,str|float|datacube.model.Range]
        :rtype list[datacube.model.StorageUnit]
        """
        return list(self.search(**query))

    def _make(self, query_results):
        """
        :rtype: list[datacube.model.StorageUnit]
        """
        return (StorageUnit(
            su['dataset_refs'],
            self.types.get_for_dataset_type(su['dataset_type_ref']),
            su['metadata'],
            # An offset from the location (ie. a URL fragment):
            size_bytes=su['metadata']['size_bytes'],
            output_uri=su['local_uri'],
            id_=su['id']
        ) for su in query_results)


class StorageTypeResource(object):
    def __init__(self, db, host_config, metadata_type_resource):
        """
        :type db: datacube.index.postgres._api.PostgresDb
        :type host_config: datacube.config.LocalConfig
        :type metadata_type_resource: datacube.index._datasets.MetadataTypeResource
        """
        self._db = db
        self._host_config = host_config
        self._metadata_type_resource = metadata_type_resource

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
                # Add a corresponding dataset type.
                # The duplication is temporary: replacements for storage_types are still being discussed.

                # Our type has the same dataset metadata but in a different file format.
                match_metadata = copy.deepcopy(dataset_metadata)
                match_metadata.update({
                    'format': {
                        'name': definition['storage']['driver']
                    }
                })
                storage_unit_type = self._metadata_type_resource.get_by_name('storage_unit')
                assert storage_unit_type, "No storage_unit type exists: were default metadata types skipped?"
                dataset_type_id = self._db.add_dataset_type(
                    name, match_metadata,
                    storage_unit_type.id,
                    {
                        'name': name,
                        'description': definition.get('description'),
                        'metadata_type': 'storage_unit',
                        'match': {'metadata': match_metadata}
                    }
                )
                self._db.ensure_storage_type(
                    name,
                    dataset_metadata,
                    definition,
                    dataset_type_id
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

        return StorageType(definition, record['target_dataset_type_ref'], id_=record['id'])

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

    def get_for_dataset_type(self, dataset_type_id):
        results = [d for d in self.get_all() if d.target_dataset_type_id == dataset_type_id]
        if not results:
            return None

        return results[0]

    @cachetools.cached(cachetools.TTLCache(100, 60))
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
    >>> _unit_classifier(
    ...     {"extent": {
    ...         "center_dt": "2014-07-26T23:49:00.343853",
    ...         "from_dt": "2014-07-26T23:48:00.343853",
    ...         "to_dt": "2014-07-26T23:56:00.343853"
    ...     }}
    ... )
    '2014-07-26T23:49:00'
    >>> _unit_classifier({"extent": {"from_dt": "2014-07-26T23:49:00.343853", "to_dt": "2014-07-26T23:51:00.343853"}})
    '2014-07-26T23:50:00'
    """
    extent_ = descriptor['extent']
    if 'center_dt' in extent_:
        d = dateutil.parser.parse(extent_.get('center_dt'))
    elif 'from_dt' in extent_:
        # No center: calculate from start/stop
        start = dateutil.parser.parse(extent_.get('to_dt'))
        end = dateutil.parser.parse(extent_.get('from_dt'))
        interval = (end - start) // 2
        d = start + interval
    else:
        raise ValueError('No usable time information in dataset metadata: %r ' % descriptor)

    return d.replace(microsecond=0).isoformat()

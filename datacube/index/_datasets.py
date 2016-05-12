# coding=utf-8
"""
API for dataset indexing, access and search.
"""
from __future__ import absolute_import

import copy
import logging

import cachetools

from datacube import compat
from datacube.index.fields import InvalidDocException
from datacube.model import Dataset, DatasetType, DatasetMatcher, DatasetOffsets, MetadataType
from . import fields

_LOG = logging.getLogger(__name__)


class MetadataTypeResource(object):
    def __init__(self, db):
        """
        :type db: datacube.index.postgres._api.PostgresDb
        """
        self._db = db

    def add(self, definition, allow_table_lock=False):
        """
        :type definition: dict
        :param allow_table_lock:
            Allow an exclusive lock to be taken on the table while creating the indexes.
            This will halt other user's requests until completed.

            If false, creation will be slightly slower and cannot be done in a transaction.
        :rtype: datacube.model.MetadataType
        """
        # This column duplication is getting out of hand:
        name = definition['name']

        existing = self._db.get_metadata_type_by_name(name)
        if existing:
            # They've passed us the same one again. Make sure it matches what is stored.
            # TODO: Support for adding/updating search fields?
            fields.check_doc_unchanged(
                existing.definition,
                definition,
                'Metadata Type {}'.format(name)
            )
        else:
            self._db.add_metadata_type(
                name=name,
                definition=definition,
                concurrently=not allow_table_lock
            )
        return self.get_by_name(name)

    @cachetools.cached(cachetools.TTLCache(100, 60))
    def get(self, id_):
        """
        :rtype datacube.model.MetadataType
        """
        return self._make(self._db.get_metadata_type(id_))

    @cachetools.cached(cachetools.TTLCache(100, 60))
    def get_by_name(self, name):
        """
        :rtype datacube.model.MetadataType
        """
        record = self._db.get_metadata_type_by_name(name)
        if not record:
            return None
        return self._make(record)

    def _make_many(self, query_rows):
        return (self._make(c) for c in query_rows)

    def _make(self, query_row):
        """
        :rtype list[datacube.model.MetadataType]
        """
        definition = query_row['definition']
        dataset_ = definition['dataset']
        return MetadataType(
            query_row['name'],
            DatasetOffsets(
                uuid_field=dataset_.get('id_offset'),
                label_field=dataset_.get('label_offset'),
                creation_time_field=dataset_.get('creation_dt_offset'),
                measurements_dict=dataset_.get('measurements_offset'),
                sources=dataset_.get('sources_offset'),
            ),
            dataset_search_fields=self._db.get_dataset_fields(query_row),
            id_=query_row['id']
        )


class DatasetTypeResource(object):
    """
    :type _db: datacube.index.postgres._api.PostgresDb
    :type metadata_type_resource: MetadataTypeResource
    """
    def __init__(self, db, metadata_type_resource):
        """
        :type db: datacube.index.postgres._api.PostgresDb
        :type metadata_type_resource: MetadataTypeResource
        """
        self._db = db
        self.metadata_type_resource = metadata_type_resource

    def from_doc(self, definition):
        """
        :type definition: dict
        :rtype: datacube.model.DatasetType
        """
        # This column duplication is getting out of hand:
        name = definition['name']
        dataset_metadata = definition['match']['metadata']
        metadata_type = definition['metadata_type']

        # They either specified the name of a metadata type, or specified a metadata type.
        # Is it a name?
        if isinstance(metadata_type, compat.string_types):
            metadata_type = self.metadata_type_resource.get_by_name(metadata_type)
        else:
            # Otherwise they embedded a document, add it if needed:
            metadata_type = self.metadata_type_resource.add(metadata_type, allow_table_lock=False)

        if not metadata_type:
            raise InvalidDocException('Unknown metadata type: %r' % definition['metadata_type'])

        return DatasetType(metadata_type, definition)

    def add(self, type_):
        """
        :type type_: datacube.model.DatasetType
        :rtype: datacube.model.DatasetType
        """
        existing = self._db.get_dataset_type_by_name(type_.name)
        if existing:
            # TODO: Support for adding/updating match rules?
            # They've passed us the same collection again. Make sure it matches what is stored.
            fields.check_doc_unchanged(
                existing.definition,
                type_.definition,
                'Dataset type {}'.format(type_.name)
            )
        else:
            self._db.add_dataset_type(
                name=type_.name,
                metadata=type_.match,
                metadata_type_id=type_.metadata_type.id,
                definition=type_.definition
            )
        return self.get_by_name(type_.name)

    def add_document(self, definition):
        """
        :type definition: dict
        :rtype: datacube.model.DatasetType
        """
        type_ = self.from_doc(definition)
        return self.add(type_)

    def add_many(self, definitions):
        """
        :type definitions: list[dict]
        """
        for definition in definitions:
            self.add_document(definition)

    @cachetools.cached(cachetools.TTLCache(100, 60))
    def get(self, id_):
        """
        :rtype datacube.model.DatasetType
        """
        return self._make(self._db.get_dataset_type(id_))

    @cachetools.cached(cachetools.TTLCache(100, 60))
    def get_by_name(self, name):
        """
        :rtype datacube.model.DatasetType
        """
        result = self._db.get_dataset_type_by_name(name)
        if not result:
            return None
        return self._make(result)

    def get_for_dataset_doc(self, metadata_doc):
        """
        :type metadata_doc: dict
        :rtype: datacube.model.DatasetType or None
        """
        result = self._db.determine_dataset_type_for_doc(metadata_doc)
        if result is None:
            return None

        return self._make(result)

    def get_with_fields(self, field_names):
        """
        Return dataset types that have all the given fields.
        :type field_names: tuple[str]
        :rtype: __generator[DatasetType]
        """
        for type_ in self.get_all():
            for name in field_names:
                if name not in type_.metadata_type.dataset_fields:
                    break
            else:
                yield type_

    def get_all(self):
        """
        :rtype: iter[datacube.model.DatasetType]
        """
        return (self._make(record) for record in self._db.get_all_dataset_types())

    def _make_many(self, query_rows):
        return (self._make(c) for c in query_rows)

    def _make(self, query_row):
        """
        :rtype datacube.model.DatasetType
        """
        return DatasetType(
            definition=query_row['definition'],
            metadata_type=self.metadata_type_resource.get(query_row['metadata_type_ref']),
            id_=query_row['id'],
        )


class DatasetResource(object):
    """
    :type _db: datacube.index.postgres._api.PostgresDb
    :type types: datacube.index._datasets.DatasetTypeResource
    """
    def __init__(self, db, dataset_type_resource):
        """
        :type db: datacube.index.postgres._api.PostgresDb
        :type dataset_type_resource: datacube.index._datasets.DatasetTypeResource
        """
        self._db = db
        self.types = dataset_type_resource

    def get(self, id_, include_sources=False):
        """
        Get dataset by id

        :param include_sources: get the full provenance graph?
        :rtype datacube.model.Dataset
        """
        if not include_sources:
            return self._make(self._db.get_dataset(id_))

        datasets = {result['id']: (self._make(result), result) for result in self._db.get_dataset_sources(id_)}
        for dataset, result in datasets.values():
            dataset.metadata_doc['lineage']['source_datasets'] = {
                classifier: datasets[str(source)][0].metadata_doc
                for source, classifier in zip(result['sources'], result['classes']) if source
                }
            dataset.sources = {
                classifier: datasets[str(source)][0]
                for source, classifier in zip(result['sources'], result['classes']) if source
            }
        return datasets[id_][0]

    def has(self, dataset):
        """
        Have we already indexed this dataset?

        :type dataset: datacube.model.Dataset
        :rtype: bool
        """
        return self._db.contains_dataset(dataset.id)

    def from_doc(self, dataset_doc, metadata_path=None, uri=None, allow_replacement=False):
        """
        Build a dataset from its document

        A file path or URI should be specified if available.

        :type metadata_doc: dict
        :type metadata_path: pathlib.Path
        :type uri: str
        :rtype: datacube.model.Dataset
        """
        type_ = self.types.get_for_dataset_doc(dataset_doc)
        if not type_:
            _LOG.debug('Failed match on dataset doc %r', dataset_doc)
            raise ValueError('No types match the dataset.')
        _LOG.info('Matched type %r (%s)', type_.name, type_.id)

        indexable_doc = copy.deepcopy(dataset_doc)
        dataset = type_.metadata_type.dataset_reader(indexable_doc)

        source_datasets = {classifier: self.from_doc(source_dataset_doc)
                           for classifier, source_dataset_doc in dataset.sources.items()}

        if metadata_path and not uri:
            uri = metadata_path.absolute().as_uri()

        return Dataset(type_, indexable_doc, uri, source_datasets, managed=allow_replacement)

    def add(self, dataset):
        """
        Ensure a dataset is in the index. Add it if not present.

        :type dataset: datacube.model.Dataset
        :rtype: datacube.model.Dataset
        """
        for source in dataset.sources.values():
            self.add(source)

        _LOG.info('Indexing %s', dataset.id)
        with self._db.begin() as transaction:
            was_inserted = self._db.insert_dataset(dataset.metadata_doc, dataset.id, dataset.type.id)

            if was_inserted:
                for classifier, source_dataset in dataset.sources.items():
                    self._db.insert_dataset_source(classifier, dataset.id, source_dataset.id)

        if dataset.local_uri:
            self._db.ensure_dataset_location(dataset.id, dataset.local_uri, dataset.managed)

        return dataset

    def add_document(self, metadata_doc, metadata_path=None, uri=None, allow_replacement=False):
        """
        Ensure a dataset is in the index. Add it if not present.

        A file path or URI should be specified if available.

        :type metadata_doc: dict
        :type metadata_path: pathlib.Path
        :type uri: str
        :rtype: datacube.model.Dataset
        """
        dataset = self.from_doc(metadata_doc, metadata_path, uri, allow_replacement)
        return self.add(dataset)

    def replace(self, old_datasets, new_datasets):
        """
        :type old_datasets: list[datacube.model.Dataset]
        :type new_datasets: list[datacube.model.Dataset]
        """
        with self._db.begin() as transaction:
            for unit in old_datasets:
                self._db.archive_storage_unit(unit.id)

            for unit in new_datasets:
                unit = self.add(unit)
                _LOG.debug('Indexed dataset %s @ %s', unit.id, unit.local_uri)

    def get_field(self, name, type_name=None):
        """
        :type name: str
        :rtype: datacube.index.fields.Field
        """
        return self.get_fields(type_name).get(name)

    def get_field_names(self, type_name=None):
        """
        :type type_name: str
        :rtype: __generator[str]
        """
        if type_name is None:
            types = self.types.get_all()
        else:
            types = [self.types.get_by_name(type_name)]

        for type_ in types:
            for name in type_.metadata_type.dataset_fields:
                yield name

    def get_locations(self, dataset):
        """
        :type dataset: datacube.model.Dataset
        :rtype: list[str]
        """
        return self._db.get_locations(dataset.id)

    def _make(self, dataset_res):
        """
        :rtype datacube.model.Dataset
        """
        return Dataset(
            self.types.get(dataset_res.dataset_type_ref),
            dataset_res.metadata,
            dataset_res.local_uri,
            managed=dataset_res.managed
        )

    def _make_many(self, query_result):
        """
        :rtype list[datacube.model.Dataset]
        """
        return (self._make(dataset) for dataset in query_result)

    def search_by_metadata(self, metadata):
        """
        Perform a search using arbitrary metadata, returning results as Dataset objects.

        Caution – slow! This will usually not use indexes.

        :type metadata: dict
        :rtype list[datacube.model.Dataset]
        """
        return self._make_many(self._db.search_datasets_by_metadata(metadata))

    def search(self, **query):
        """
        Perform a search, returning results as Dataset objects.
        :type query: dict[str,str|float|datacube.model.Range]
        :rtype: __generator[datacube.model.Dataset]
        """
        return self._make_many(self._do_search(query))

    def _do_search(self, query, return_fields=False, with_source_ids=False):
        q = dict(query)
        metadata_types = set()
        if 'type' in q.keys():
            metadata_types.add(self.types.get_by_name(q['type']).metadata_type)

        # If they specified a metadata type, search using it.
        if 'metadata_type' in q.keys():
            metadata_types.add(self.types.metadata_type_resource.get_by_name(q['metadata_type']))

        if len(metadata_types) > 1:
            _LOG.warning(
                "Both a dataset type and metadata type were specified, but they're not compatible: %r, %r.",
                query['type'], query['metadata_type']
            )
            # No datasets of this type can have the given metadata type.
            # No results.
            return

        if not metadata_types:
            # Otherwise search any metadata type that has all the given search fields.
            applicable_dataset_types = self.types.get_with_fields(q.keys())
            if not applicable_dataset_types:
                raise ValueError('No type of dataset has fields: %r', tuple(q.keys()))
            # Unique metadata types we're searching.
            metadata_types = set(d.metadata_type for d in applicable_dataset_types)

        # Perform one search per metadata type.
        for metadata_type in metadata_types:
            q['metadata_type'] = metadata_type.name
            query_exprs = tuple(fields.to_expressions(metadata_type.dataset_fields.get, **q))
            select_fields = None
            if return_fields:
                select_fields = tuple(metadata_type.dataset_fields.values())
            for dataset in self._db.search_datasets(query_exprs,
                                                    select_fields=select_fields,
                                                    with_source_ids=with_source_ids):
                yield dataset

    def search_summaries(self, **query):
        """
        Perform a search, returning just the search fields of each dataset.

        :type query: dict[str,str|float|datacube.model.Range]
        :rtype: dict
        """
        return (
            dict(fs) for fs in
            self._do_search(
                query,
                return_fields=True
            )
        )

    def search_eager(self, **query):
        """
        :type query: dict[str,str|float|datacube.model.Range]
        :rtype list[datacube.model.Dataset]
        """
        return list(self.search(**query))

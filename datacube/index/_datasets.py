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


def _ensure_dataset(db, types_resource, dataset_doc):
    """
    Ensure a dataset is in the index (add it if needed).

    :type db: datacube.index.postgres._api.PostgresDb
    :type dataset_doc: dict
    :type types_resource: DatasetTypeResource
    :returns: The dataset_id if we ingested it.
    :rtype: uuid.UUID
    """

    was_inserted, dataset, source_datasets = _prepare_single(types_resource, dataset_doc, db)

    dataset_id = dataset.uuid_field

    if not was_inserted:
        # Already existed.
        _LOG.info('Dataset already in Index. No indexing required.')
        return dataset_id

    if source_datasets:
        # Get source datasets & index them.
        sources = {}
        for classifier, source_dataset in source_datasets.items():
            sources[classifier] = _ensure_dataset(db, types_resource, source_dataset)

        # Link to sources.
        for classifier, source_dataset_id in sources.items():
            db.insert_dataset_source(classifier, dataset_id, source_dataset_id)

    return dataset_id


def _prepare_single(types_resource, dataset_doc, db):
    """
    :type types_resource: DatasetTypeResource
    :type dataset_doc: dict
    :type db: datacube.index.postgres._api.PostgresDb
    """
    type_ = types_resource.get_for_dataset_doc(dataset_doc)
    if not type_:
        _LOG.debug('Failed match on dataset doc %r', dataset_doc)
        raise ValueError('No types match the dataset.')

    _LOG.info('Matched collection %r (%s)', type_.name, type_.id)

    indexable_doc = copy.deepcopy(dataset_doc)
    dataset = type_.metadata_type.dataset_reader(indexable_doc)

    source_datasets = dataset.sources
    # Clear source datasets: We store them separately.
    dataset.sources = None

    dataset_id = dataset.uuid_field

    _LOG.info('Indexing %s', dataset_id)
    was_inserted = db.insert_dataset(indexable_doc, dataset_id, dataset_type_id=type_.id)

    return was_inserted, dataset, source_datasets


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
        return self._make(self._db.get_metadata_type(id_))

    @cachetools.cached(cachetools.TTLCache(100, 60))
    def get_by_name(self, name):
        record = self._db.get_metadata_type_by_name(name)
        if not record:
            return None
        return self._make(record)

    def _make_many(self, query_rows):
        return (self._make(c) for c in query_rows)

    def _make(self, query_row):
        """
        :rtype list[datacube.model.Collection]
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
    def __init__(self, db, metadata_type_resource):
        """
        :type db: datacube.index.postgres._api.PostgresDb
        :type metadata_type_resource: MetadataTypeResource
        """
        self._db = db
        self.metadata_type_resource = metadata_type_resource

    def add(self, definition):
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

        existing = self._db.get_dataset_type_by_name(name)
        if existing:
            # TODO: Support for adding/updating match rules?
            # They've passed us the same collection again. Make sure it matches what is stored.
            fields.check_doc_unchanged(
                existing.definition,
                definition,
                'Dataset type {}'.format(name)
            )
        else:
            self._db.add_dataset_type(
                name=name,
                metadata=dataset_metadata,
                metadata_type_id=metadata_type.id,
                definition=definition
            )
        return self.get_by_name(name)

    def add_many(self, definitions):
        """
        :type definitions: list[dict]
        """
        for definition in definitions:
            self.add(definition)

    @cachetools.cached(cachetools.TTLCache(100, 60))
    def get(self, id_):
        return self._make(self._db.get_dataset_type(id_))

    @cachetools.cached(cachetools.TTLCache(100, 60))
    def get_by_name(self, name):
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

    def get_all(self):
        """
        :rtype: iter[datacube.model.Collection]
        """
        return (self._make(record) for record in self._db.get_all_dataset_types())

    def _make_many(self, query_rows):
        return (self._make(c) for c in query_rows)

    def _make(self, query_row):
        """
        :rtype datacube.model.Collection
        """
        return DatasetType(
            query_row['name'],
            DatasetMatcher(query_row['metadata']),
            metadata_type=self.metadata_type_resource.get(query_row['metadata_type_ref']),
            id_=query_row['id'],
        )


class DatasetResource(object):
    def __init__(self, db, user_config, dataset_type_resource):
        """
        :type db: datacube.index.postgres._api.PostgresDb
        :type user_config: datacube.config.LocalConfig
        :type dataset_type_resource: datacube.index._datasets.DatasetTypeResource
        """
        self._db = db
        self._config = user_config
        self.types = dataset_type_resource

    def get(self, id_, provenance=False):
        """
        Get dataset by id

        :param provenance: get the full provenance graph?
        :rtype datacube.model.Dataset
        """
        if not provenance:
            return self._make(self._db.get_dataset(id_))

        datasets = {result['id']: result for result in self._db.get_dataset_sources(id_)}
        for dataset in datasets.values():
            dataset['metadata']['lineage']['source_datasets'] = {
                classifier: datasets[str(source)]['metadata']
                for source, classifier in zip(dataset['sources'], dataset['classes']) if source
            }
        return self._make(datasets[id_])

    def has(self, dataset):
        """
        Have we already indexed this dataset?

        :type dataset: datacube.model.Dataset
        :rtype: bool
        """
        return self._db.contains_dataset(dataset.id)

    def add(self, metadata_doc, metadata_path=None, uri=None, allow_replacement=False):
        """
        Ensure a dataset is in the index. Add it if not present.

        A file path or URI should be specified if available.

        :type metadata_doc: dict
        :type metadata_path: pathlib.Path
        :type uri: str
        :rtype: datacube.model.Dataset
        """
        with self._db.begin() as transaction:
            dataset_id = self._add_dataset(metadata_doc,
                                           allow_replacement=allow_replacement,
                                           metadata_path=metadata_path,
                                           uri=uri)

        if not dataset_id:
            return None

        return self.get(dataset_id)

    def _add_dataset(self, metadata_doc, allow_replacement, metadata_path=None, uri=None):
        dataset_id = _ensure_dataset(self._db, self.types, metadata_doc)
        if metadata_path or uri:
            if uri is None:
                uri = metadata_path.absolute().as_uri()
            self._db.ensure_dataset_location(dataset_id, uri, allow_replacement)
        return dataset_id

    def replace(self, old_datasets, new_datasets):
        """
        :type old_datasets: list[datacube.model.Dataset]
        :type new_datasets: list[datacube.model.Dataset]
        """
        with self._db.begin() as transaction:
            for unit in old_datasets:
                self._db.archive_storage_unit(unit.id)

            for unit in new_datasets:
                dataset_id = self._add_dataset(unit.metadata_doc, allow_replacement=True, uri=unit.local_uri)
                _LOG.debug('Indexed dataset %s @ %s', dataset_id, unit.local_uri)

    def get_field(self, name, type_name=None):
        """
        :type name: str
        :rtype: datacube.index.fields.Field
        """
        return self.get_fields(type_name).get(name)

    def get_fields(self, type_name=None):
        """
        :type type_name: str
        :rtype: dict[str, datacube.index.fields.Field]
        """
        if type_name is None:
            type_name = self._config.default_collection_name
        collection = self.types.get_by_name(type_name)
        return collection.metadata_type.dataset_fields

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
            dataset_res.local_uri
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

    def search(self, *expressions, **query):
        """
        Perform a search, returning results as Dataset objects.
        :type query: dict[str,str|float|datacube.model.Range]
        :type expressions: tuple[datacube.index.fields.PgExpression]
        :rtype list[datacube.model.Dataset]
        """
        query_exprs = tuple(fields.to_expressions(self.get_field, **query))
        return self._make_many(self._db.search_datasets((expressions + query_exprs)))

    def search_summaries(self, *expressions, **query):
        """
        Perform a search, returning just the search fields of each dataset.

        :type query: dict[str,str|float|datacube.model.Range]
        :type expressions: tuple[datacube.index.fields.PgExpression]
        :rtype: dict
        """
        query_exprs = tuple(fields.to_expressions(self.get_field, **query))

        return (
            dict(fs) for fs in
            self._db.search_datasets(
                (expressions + query_exprs),
                select_fields=tuple(self.get_fields().values())
            )
        )

    def search_eager(self, *expressions, **query):
        """
        :type expressions: list[datacube.index.fields.Expression]
        :type query: dict[str,str|float|datacube.model.Range]
        :rtype list[datacube.model.Dataset]
        """
        return list(self.search(*expressions, **query))

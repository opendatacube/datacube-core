# coding=utf-8
"""
API for dataset indexing, access and search.
"""
from __future__ import absolute_import

import copy
import logging
from pathlib import Path

import cachetools

from datacube.model import Dataset, Collection, DatasetMatcher, DatasetOffsets
from . import fields

_LOG = logging.getLogger(__name__)


def _ensure_dataset(db, collection_resource, dataset_doc, path=None):
    """
    Ensure a dataset is in the index (add it if needed).

    :type db: datacube.index.postgres._api.PostgresDb
    :type dataset_doc: dict
    :type collection_resource: CollectionResource
    :type path: pathlib.Path
    :returns: The dataset_id if we ingested it.
    :rtype: uuid.UUID
    """

    was_inserted, dataset, source_datasets = _prepare_single(collection_resource, dataset_doc, db, path)

    dataset_id = dataset.uuid_field

    if not was_inserted:
        # Already existed.
        return dataset_id

    if source_datasets:
        # Get source datasets & index them.
        sources = {}
        for classifier, source_dataset in source_datasets.items():
            sources[classifier] = _ensure_dataset(db, collection_resource, source_dataset)

        # Link to sources.
        for classifier, source_dataset_id in sources.items():
            db.insert_dataset_source(classifier, dataset_id, source_dataset_id)

    return dataset_id


def _prepare_single(collection_resource, dataset_doc, db, path):
    collection = collection_resource.get_for_dataset_doc(dataset_doc)
    if not collection:
        _LOG.debug('Failed match on dataset doc %r', dataset_doc)
        raise ValueError('No collection matched for dataset.')

    _LOG.info('Matched collection %r (%s)', collection.name, collection.id_)

    indexable_doc = copy.deepcopy(dataset_doc)
    dataset = collection.dataset_reader(indexable_doc)

    source_datasets = dataset.sources
    # Clear source datasets: We store them separately.
    dataset.sources = None

    dataset_id = dataset.uuid_field

    _LOG.info('Indexing %s @ %s', dataset_id, path)
    was_inserted = db.insert_dataset(indexable_doc, dataset_id, path)

    return was_inserted, dataset, source_datasets


class CollectionResource(object):
    def __init__(self, db, user_config):
        """
        :type db: datacube.index.postgres._api.PostgresDb
        """
        self._db = db

    def add(self, descriptor):
        """
        :type descriptor: dict
        :rtype: datacube.model.Collection
        """
        # This column duplication is getting out of hand:
        name = descriptor['name']
        description = descriptor['description']
        dataset_metadata = descriptor['match']['metadata']
        match_priority = int(descriptor['match']['priority'])
        d_id_offset = descriptor['dataset']['id_offset']
        d_label_offset = descriptor['dataset']['label_offset']
        d_creation_offset = descriptor['dataset']['creation_dt_offset']
        d_measurements_offset = descriptor['dataset']['measurements_offset']
        d_sources_offset = descriptor['dataset']['sources_offset']
        d_search_fields = descriptor['dataset']['search_fields']
        s_search_fields = descriptor['storage_unit']['search_fields']

        existing = self._db.get_collection_by_name(name)
        if existing:
            # They've passed us the same collection again. Make sure it matches what we have:
            # TODO: Support for adding/updating search fields?
            fields.check_field_equivalence(
                [
                    ('description', existing.description, description),
                    ('match.metadata', existing.dataset_metadata, dataset_metadata),
                    ('match.priority', existing.match_priority, match_priority),
                    ('dataset.id_offset', existing.dataset_id_offset, d_id_offset),
                    ('dataset.label_offset', existing.dataset_label_offset, d_label_offset),
                    ('dataset.creation_dt_offset', existing.dataset_creation_dt_offset, d_creation_offset),
                    ('dataset.measurements_offset', existing.dataset_measurements_offset, d_measurements_offset),
                    ('dataset.sources_offset', existing.dataset_sources_offset, d_sources_offset),
                    ('dataset.search_offset', existing.dataset_search_fields, d_search_fields),
                    ('storage_unit.search_offset', existing.storage_unit_search_fields, s_search_fields)
                ],
                'Collection {}'.format(name)
            )
        else:
            self._db.add_collection(
                name=name,
                description=description,
                dataset_metadata=dataset_metadata,
                match_priority=match_priority,
                dataset_id_offset=d_id_offset,
                dataset_label_offset=d_label_offset,
                dataset_creation_dt_offset=d_creation_offset,
                dataset_measurements_offset=d_measurements_offset,
                dataset_sources_offset=d_sources_offset,
                # TODO: Validate
                dataset_search_fields=d_search_fields,
                # TODO: Validate
                storage_unit_search_fields=s_search_fields
            )
        return self.get_by_name(name)

    @cachetools.cached(cachetools.TTLCache(100, 60))
    def get(self, id_):
        return self._make(self._db.get_collection(id_))

    @cachetools.cached(cachetools.TTLCache(100, 60))
    def get_by_name(self, name):
        collection = self._db.get_collection_by_name(name)
        if not collection:
            return None
        return self._make(collection)

    def get_for_dataset_doc(self, metadata_doc):
        """
        :type metadata_doc: dict
        :rtype: datacube.model.Collection or None
        """
        collection_res = self._db.get_collection_for_doc(metadata_doc)
        if collection_res is None:
            return None

        return self._make(collection_res)

    def _make_many(self, query_rows):
        return (self._make(c) for c in query_rows)

    def _make(self, query_row):
        """
        :rtype list[datacube.model.Collection]
        """
        return Collection(
            query_row['name'],
            query_row['description'],
            DatasetMatcher(query_row['dataset_metadata']),
            DatasetOffsets(
                uuid_field=query_row['dataset_id_offset'],
                label_field=query_row['dataset_label_offset'],
                creation_time_field=query_row['dataset_creation_dt_offset'],
                measurements_dict=query_row['dataset_measurements_offset'],
                sources=query_row['dataset_sources_offset'],
            ),
            dataset_search_fields=self._db.get_dataset_fields(query_row),
            storage_unit_search_fields=self._db.get_storage_unit_fields(query_row),
            id_=query_row['id'],
        )


class DatasetResource(object):
    def __init__(self, db, user_config, collection_resource):
        """
        :type db: datacube.index.postgres._api.PostgresDb
        :type user_config: datacube.config.LocalConfig
        :type collection_resource: CollectionResource
        """
        self._db = db
        self._config = user_config
        self._collection_resource = collection_resource

    def get(self, id_):
        """
        :rtype datacube.model.Dataset
        """
        return self._make(self._db.get_dataset(id_))

    def has(self, dataset):
        """
        Have we already indexed this dataset?

        :type dataset: datacube.model.Dataset
        :rtype: bool
        """
        return self._db.contains_dataset(dataset.id)

    def add(self, metadata_doc, metadata_path):
        """
        Ensure a dataset is in the index. Add it if not present.
        :type metadata_doc: dict
        :type metadata_path: pathlib.Path
        :rtype: datacube.model.Dataset
        """
        with self._db.begin() as transaction:
            dataset_id = _ensure_dataset(self._db, self._collection_resource, metadata_doc, path=metadata_path)

        if not dataset_id:
            return None

        return self.get(dataset_id)

    def get_field(self, name, collection_name=None):
        """
        :type name: str
        :rtype: datacube.index.fields.Field
        """
        return self.get_fields(collection_name).get(name)

    def get_fields(self, collection_name=None):
        """
        :type collection_name: str
        :rtype: dict[str, datacube.index.fields.Field]
        """
        if collection_name is None:
            collection_name = self._config.default_collection_name
        collection = self._collection_resource.get_by_name(collection_name)
        return collection.dataset_fields

    def _make(self, dataset_res):
        """
        :rtype datacube.model.Dataset
        """
        return Dataset(
            self._collection_resource.get(dataset_res.collection_ref),
            dataset_res.metadata,
            Path(dataset_res.metadata_path) if dataset_res.metadata_path else None
        )

    def _make_many(self, query_result):
        """
        :rtype list[datacube.model.Dataset]
        """
        return (self._make(dataset) for dataset in query_result)

    def search(self, *expressions, **query):
        """
        :type query: dict[str,str|float|datacube.model.Range]
        :type expressions: tuple[datacube.index.fields.PgExpression]
        :rtype list[datacube.model.Dataset]
        """
        query_exprs = tuple(fields.to_expressions(self.get_field, **query))
        return self._make_many(self._db.search_datasets((expressions + query_exprs)))

    def search_summaries(self, *expressions, **query):
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

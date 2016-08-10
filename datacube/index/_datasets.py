# coding=utf-8
"""
API for dataset indexing, access and search.
"""
from __future__ import absolute_import

import logging

from cachetools import lru_cache

from datacube import compat
from datacube.model import Dataset, DatasetType, MetadataType
from datacube.utils import InvalidDocException, check_doc_unchanged, jsonify_document
from . import fields
from .exceptions import DuplicateRecordError

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
        MetadataType.validate(definition)

        name = definition['name']

        existing = self._db.get_metadata_type_by_name(name)
        if existing:
            # They've passed us the same one again. Make sure it matches what is stored.
            # TODO: Support for adding/updating search fields?
            check_doc_unchanged(
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

    @lru_cache()
    def get(self, id_):
        """
        :rtype datacube.model.MetadataType
        """
        return self._make(self._db.get_metadata_type(id_))

    @lru_cache()
    def get_by_name(self, name):
        """
        :rtype datacube.model.MetadataType
        """
        record = self._db.get_metadata_type_by_name(name)
        if not record:
            return None
        return self._make(record)

    def check_field_indexes(self, allow_table_lock=False, rebuild_all=False):
        """
        Create or replace per-field indexes and views.
        :param allow_table_lock:
            Allow an exclusive lock to be taken on the table while creating the indexes.
            This will halt other user's requests until completed.

            If false, creation will be slightly slower and cannot be done in a transaction.
        """
        self._db.check_dynamic_fields(concurrently=not allow_table_lock, rebuild_all=rebuild_all)

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
            dataset_,
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
        Create a Product from its definitions

        :param dict definition: product definition document
        :rtype: datacube.model.DatasetType
        """
        # This column duplication is getting out of hand:
        DatasetType.validate(definition)

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
        Add a Product

        :param datacube.model.DatasetType type_: Product to add
        :rtype: datacube.model.DatasetType
        """
        DatasetType.validate(type_.definition)

        existing = self._db.get_dataset_type_by_name(type_.name)
        if existing:
            # TODO: Support for adding/updating match rules?
            # They've passed us the same collection again. Make sure it matches what is stored.
            check_doc_unchanged(
                existing.definition,
                jsonify_document(type_.definition),
                'Dataset type {}'.format(type_.name)
            )
        else:
            self._db.add_dataset_type(
                name=type_.name,
                metadata=type_.metadata_doc,
                metadata_type_id=type_.metadata_type.id,
                definition=type_.definition
            )
        return self.get_by_name(type_.name)

    def add_document(self, definition):
        """
        Add a Product using its difinition

        :param dict definition: product definition document
        :rtype: datacube.model.DatasetType
        """
        type_ = self.from_doc(definition)
        return self.add(type_)

    @lru_cache()
    def get(self, id_):
        """
        Retrieve Product by id

        :param int id_: id of the Product
        :rtype datacube.model.DatasetType
        """
        return self._make(self._db.get_dataset_type(id_))

    @lru_cache()
    def get_by_name(self, name):
        """
        Retrieve Product by name

        :param str name: name of the Product
        :rtype datacube.model.DatasetType
        """
        result = self._db.get_dataset_type_by_name(name)
        if not result:
            return None
        return self._make(result)

    def get_with_fields(self, field_names):
        """
        Return dataset types that have all the given fields.

        :param tuple[str] field_names:
        :rtype: __generator[DatasetType]
        """
        for type_ in self.get_all():
            for name in field_names:
                if name not in type_.metadata_type.dataset_fields:
                    break
            else:
                yield type_

    def search(self, **query):
        """
        Return dataset types that have all the given fields.

        :param dict query:
        :rtype: __generator[DatasetType]
        """
        for type_, q in self.search_robust(**query):
            if not q:
                yield type_

    def search_robust(self, **query):
        """
        Return dataset types that match match-able fields and dict of remaining un-matchable fields.

        :param dict query:
        :rtype: __generator[(DatasetType, dict)]
        """
        for type_ in self.get_all():
            q = query.copy()
            if q.pop('product', type_.name) != type_.name:
                continue
            if q.pop('metadata_type', type_.metadata_type.name) != type_.metadata_type.name:
                continue

            for key, value in list(q.items()):
                try:
                    exprs = fields.to_expressions(type_.metadata_type.dataset_fields.get, **{key: value})
                except RuntimeError:
                    break

                try:
                    if all(expr.evaluate(type_.metadata_doc) for expr in exprs):
                        q.pop(key)
                    else:
                        break
                except (AttributeError, KeyError, ValueError) as e:
                    continue
            else:
                yield type_, q

    def get_all(self):
        """
        Retrieve all Products

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

        :param uuid id_: id of the dataset to retrieve
        :param bool include_sources: get the full provenance graph?
        :rtype: datacube.model.Dataset
        """
        if not include_sources:
            return self._make(self._db.get_dataset(id_), full_info=True)

        datasets = {result['id']: (self._make(result, full_info=True), result)
                    for result in self._db.get_dataset_sources(id_)}
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

    def get_derived(self, id_):
        """
        Get drived datasets

        :param uuid id_: dataset id
        :rtype: list[datacube.model.Dataset]
        """
        return [self._make(result) for result in self._db.get_derived_datasets(id_)]

    def has(self, dataset):
        """
        Have we already indexed this dataset?

        :param datacube.model.Dataset dataset: dataset to check
        :rtype: bool
        """
        return self._db.contains_dataset(dataset.id)

    def add(self, dataset, skip_sources=False):
        """
        Ensure a dataset is in the index. Add it if not present.

        :param datacube.model.Dataset dataset: dataset to add
        :param bool skip_sources: don't attempt to index source (use when sources are already indexed)
        :rtype: datacube.model.Dataset
        """
        if not skip_sources:
            for source in dataset.sources.values():
                self.add(source)

        was_inserted = False
        sources_tmp = dataset.type.dataset_reader(dataset.metadata_doc).sources
        dataset.type.dataset_reader(dataset.metadata_doc).sources = {}
        try:
            _LOG.info('Indexing %s', dataset.id)
            with self._db.begin() as transaction:
                try:
                    was_inserted = transaction.insert_dataset(dataset.metadata_doc, dataset.id, dataset.type.id)
                    for classifier, source_dataset in dataset.sources.items():
                        transaction.insert_dataset_source(classifier, dataset.id, source_dataset.id)

                    # try to update location in the same transaction as insertion.
                    # if insertion fails we'll try updating location later
                    # if insertion succeeds the location bit can't possibly fail
                    if dataset.local_uri:
                        transaction.ensure_dataset_location(dataset.id, dataset.local_uri)
                except DuplicateRecordError as e:
                    _LOG.warning(str(e))

            if not was_inserted:
                existing = self.get(dataset.id)
                if existing:
                    check_doc_unchanged(
                        existing.metadata_doc,
                        jsonify_document(dataset.metadata_doc),
                        'Dataset {}'.format(dataset.id)
                    )

                # reinsert attempt? try updating the location
                if dataset.local_uri:
                    try:
                        self._db.ensure_dataset_location(dataset.id, dataset.local_uri)
                    except DuplicateRecordError as e:
                        _LOG.warning(str(e))
        finally:
            dataset.type.dataset_reader(dataset.metadata_doc).sources = sources_tmp

        return dataset

    def archive(self, ids):
        """
        Mark datasets as archived

        :param list[uuid] ids: list of dataset ids to archive
        """
        with self._db.begin() as transaction:
            for id_ in ids:
                transaction.archive_dataset(id_)

    def restore(self, ids):
        """
        Mark datasets as not archived

        :param list[uuid] ids: list of dataset ids to restore
        """
        with self._db.begin() as transaction:
            for id_ in ids:
                transaction.restore_dataset(id_)

    def get_field_names(self, type_name=None):
        """
        :param str type_name:
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
        :param datacube.model.Dataset dataset: dataset
        :rtype: list[str]
        """
        return self._db.get_locations(dataset.id)

    def _make(self, dataset_res, full_info=False):
        """
        :rtype datacube.model.Dataset

        :param bool full_info: Include all available fields
        """
        return Dataset(
            self.types.get(dataset_res.dataset_type_ref),
            dataset_res.metadata,
            dataset_res.local_uri,
            indexed_by=dataset_res.added_by if full_info else None,
            indexed_time=dataset_res.added if full_info else None
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

        :param dict metadata:
        :rtype: list[datacube.model.Dataset]
        """
        return self._make_many(self._db.search_datasets_by_metadata(metadata))

    def search(self, **query):
        """
        Perform a search, returning results as Dataset objects.

        :param dict[str,str|float|datacube.model.Range] query:
        :rtype: __generator[datacube.model.Dataset]
        """
        for dataset_type, datasets in self._do_search_by_product(query):
            for dataset in self._make_many(datasets):
                yield dataset

    def search_by_product(self, **query):
        """
        Perform a search, returning datasets grouped by product type.

        :param dict[str,str|float|datacube.model.Range] query:
        :rtype: __generator[(datacube.model.DatasetType,  __generator[datacube.model.Dataset])]]
        """
        for dataset_type, datasets in self._do_search_by_product(query):
            yield dataset_type, self._make_many(datasets)

    def count(self, **query):
        """
        Perform a search, returning count of results.

        :param dict[str,str|float|datacube.model.Range] query:
        :rtype: int
        """
        # This may be optimised into one query in the future.
        result = 0
        for product_type, count in self._do_count_by_product(query):
            result += count

        return result

    def count_by_product(self, **query):
        """
        Perform a search, returning a count of for each matching product type.

        :param dict[str,str|float|datacube.model.Range] query:
        :returns: Sequence of (product, count)
        :rtype: __generator[(datacube.model.DatasetType,  int)]]
        """
        return self._do_count_by_product(query)

    def count_by_product_through_time(self, period, **query):
        """
        Perform a search, returning counts for each product grouped in time slices
        of the given period.

        :param dict[str,str|float|datacube.model.Range] query:
        :param str period: Time range for each slice: '1 month', '1 day' etc.
        :returns: For each matching product type, a list of time ranges and their count.
        :rtype: __generator[(datacube.model.DatasetType, list[(datetime.datetime, datetime.datetime), int)]]
        """
        return self._do_time_count(period, query)

    def count_product_through_time(self, period, **query):
        """
        Perform a search, returning counts for a single product grouped in time slices
        of the given period.

        Will raise an error if the search terms match more than one product.

        :param dict[str,str|float|datacube.model.Range] query:
        :param str period: Time range for each slice: '1 month', '1 day' etc.
        :returns: For each matching product type, a list of time ranges and their count.
        :rtype: list[(str, list[(datetime.datetime, datetime.datetime), int)]]
        """
        return next(self._do_time_count(period, query, ensure_single=True))[1]

    def _get_dataset_types(self, q):
        types = set()
        if 'product' in q.keys():
            types.add(self.types.get_by_name(q['product']))
        else:
            # Otherwise search any metadata type that has all the given search fields.
            types = self.types.get_with_fields(tuple(q.keys()))
            if not types:
                raise ValueError('No type of dataset has fields: %r', tuple(q.keys()))

        return types

    def _get_product_queries(self, query):
        for dataset_type, q in self.types.search_robust(**query):
            q['dataset_type_id'] = dataset_type.id
            yield q, dataset_type

    def _do_search_by_product(self, query, return_fields=False, with_source_ids=False):
        for q, dataset_type in self._get_product_queries(query):
            dataset_fields = dataset_type.metadata_type.dataset_fields
            query_exprs = tuple(fields.to_expressions(dataset_fields.get, **q))
            select_fields = None
            if return_fields:
                select_fields = tuple(dataset_fields.values())
            yield (dataset_type,
                   self._db.search_datasets(
                       query_exprs,
                       select_fields=select_fields,
                       with_source_ids=with_source_ids
                   ))

    def _do_count_by_product(self, query):
        for q, dataset_type in self._get_product_queries(query):
            dataset_fields = dataset_type.metadata_type.dataset_fields
            query_exprs = tuple(fields.to_expressions(dataset_fields.get, **q))
            count = self._db.count_datasets(query_exprs)
            if count > 0:
                yield dataset_type, count

    def _do_time_count(self, period, query, ensure_single=False):
        if 'time' not in query:
            raise ValueError('Counting through time requires a "time" range query argument')

        query = dict(query)

        start, end = query['time']
        del query['time']

        product_quries = list(self._get_product_queries(query))
        if ensure_single:
            if len(product_quries) == 0:
                raise ValueError('No products match search terms: %r' % query)
            if len(product_quries) > 1:
                raise ValueError('Multiple products match single query search: %r' %
                                 ([dt.name for q, dt in product_quries],))

        for q, dataset_type in product_quries:
            dataset_fields = dataset_type.metadata_type.dataset_fields
            query_exprs = tuple(fields.to_expressions(dataset_fields.get, **q))
            yield dataset_type, list(self._db.count_datasets_through_time(
                start,
                end,
                period,
                dataset_fields.get('time'),
                query_exprs
            ))

    def search_summaries(self, **query):
        """
        Perform a search, returning just the search fields of each dataset.

        :param dict[str,str|float|datacube.model.Range] query:
        :rtype: dict
        """
        for dataset_type, results in self._do_search_by_product(query, return_fields=True):
            for columns in results:
                yield dict(columns)

    def search_eager(self, **query):
        """
        Perform a search, returning results as Dataset objects.

        :param dict[str,str|float|datacube.model.Range] query:
        :rtype: list[datacube.model.Dataset]
        """
        return list(self.search(**query))

# coding=utf-8
"""
API for dataset indexing, access and search.
"""
from __future__ import absolute_import

import logging
import warnings
from collections import namedtuple
from uuid import UUID
from typing import Any, Iterable, Mapping, Set, Tuple, Union

from datacube import compat
from datacube.model import Dataset, DatasetType
from datacube.model.utils import flatten_datasets
from datacube.utils import jsonify_document, changes
from datacube.utils.changes import get_doc_changes, check_doc_unchanged
from . import fields
from .exceptions import DuplicateRecordError

_LOG = logging.getLogger(__name__)

# It's a public api, so we can't reorganise old methods.
# pylint: disable=too-many-public-methods, too-many-lines


class DatasetResource(object):
    """
    :type _db: datacube.drivers.postgres._connections.PostgresDb
    :type types: datacube.index._products.ProductResource
    """

    def __init__(self, db, dataset_type_resource):
        """
        :type db: datacube.drivers.postgres._connections.PostgresDb
        :type dataset_type_resource: datacube.index._products.ProductResource
        """
        self._db = db
        self.types = dataset_type_resource

    def get(self, id_, include_sources=False):
        """
        Get dataset by id

        :param UUID id_: id of the dataset to retrieve
        :param bool include_sources: get the full provenance graph?
        :rtype: Dataset
        """
        if isinstance(id_, compat.string_types):
            id_ = UUID(id_)

        with self._db.connect() as connection:
            if not include_sources:
                dataset = connection.get_dataset(id_)
                return self._make(dataset, full_info=True) if dataset else None

            datasets = {result['id']: (self._make(result, full_info=True), result)
                        for result in connection.get_dataset_sources(id_)}

        if not datasets:
            # No dataset found
            return None

        for dataset, result in datasets.values():
            dataset.metadata.sources = {
                classifier: datasets[source][0].metadata_doc
                for source, classifier in zip(result['sources'], result['classes']) if source
            }
            dataset.sources = {
                classifier: datasets[source][0]
                for source, classifier in zip(result['sources'], result['classes']) if source
            }
        return datasets[id_][0]

    def bulk_get(self, ids):
        def to_uuid(x):
            return x if isinstance(x, UUID) else UUID(x)

        ids = [to_uuid(i) for i in ids]

        with self._db.connect() as connection:
            rows = connection.get_datasets(ids)
            return [self._make(r, full_info=True) for r in rows]

    def get_derived(self, id_):
        """
        Get all derived datasets

        :param UUID id_: dataset id
        :rtype: list[Dataset]
        """
        with self._db.connect() as connection:
            return [
                self._make(result, full_info=True)
                for result in connection.get_derived_datasets(id_)
            ]

    def has(self, id_):
        """
        Have we already indexed this dataset?

        :param typing.Union[UUID, str] id_: dataset id
        :rtype: bool
        """
        with self._db.connect() as connection:
            return connection.contains_dataset(id_)

    def bulk_has(self, ids_):
        """
        Like `has` but operates on a list of ids.

        For every supplied id check if database contains a dataset with that id.

        :param [typing.Union[UUID, str]] ids_: list of dataset ids

        :rtype: [bool]
        """
        with self._db.connect() as connection:
            existing = set(connection.datasets_intersection(ids_))

        return [x in existing for x in
                map((lambda x: UUID(x) if isinstance(x, str) else x), ids_)]

    def add(self, dataset, with_lineage=None, **kwargs):
        """
        Add ``dataset`` to the index. No-op if it is already present.

        :param Dataset dataset: dataset to add
        :param bool with_lineage: True -- attempt adding lineage if it's missing, False don't
        :rtype: Dataset
        """

        def process_bunch(dss, main_ds, transaction):
            edges = []

            # First insert all new datasets
            for ds in dss:
                is_new = transaction.insert_dataset(ds.metadata_doc_without_lineage(), ds.id, ds.type.id)
                if is_new:
                    edges.extend((name, ds.id, src.id)
                                 for name, src in ds.sources.items())

            # Second insert lineage graph edges
            for ee in edges:
                transaction.insert_dataset_source(*ee)

            # Finally update location for top-level dataset only
            if main_ds.uris is not None:
                self._ensure_new_locations(main_ds, transaction=transaction)

        if with_lineage is None:
            policy = kwargs.pop('sources_policy', None)
            if policy is not None:
                _LOG.debug('Use of sources_policy is deprecated')
                with_lineage = (policy != "skip")
                if policy == 'verify':
                    _LOG.debug('Verify is no longer done inside add')
            else:
                with_lineage = True

        _LOG.info('Indexing %s', dataset.id)

        if with_lineage:
            ds_by_uuid = flatten_datasets(dataset)
            all_uuids = list(ds_by_uuid)

            present = {k: v for k, v in zip(all_uuids, self.bulk_has(all_uuids))}

            if present[dataset.id]:
                _LOG.warning('Dataset %s is already in the database', dataset.id)
                return dataset

            dss = [ds for ds in [dss[0] for dss in ds_by_uuid.values()] if not present[ds.id]]
        else:
            if self.has(dataset.id):
                _LOG.warning('Dataset %s is already in the database', dataset.id)
                return dataset

            dss = [dataset]

        with self._db.begin() as transaction:
            process_bunch(dss, dataset, transaction)

        return dataset

    def search_product_duplicates(self, product, *group_fields):
        # type: (DatasetType, Iterable[Union[str, fields.Field]]) -> Iterable[tuple, Set[UUID]]
        """
        Find dataset ids who have duplicates of the given set of field names.

        Product is always inserted as the first grouping field.

        Returns each set of those field values and the datasets that have them.
        """

        def load_field(f):
            # type: (Union[str, fields.Field]) -> fields.Field
            if isinstance(f, compat.string_types):
                return product.metadata_type.dataset_fields[f]
            assert isinstance(f, fields.Field), "Not a field: %r" % (f,)
            return f

        group_fields = [load_field(f) for f in group_fields]
        result_type = namedtuple('search_result', (f.name for f in group_fields))

        expressions = [product.metadata_type.dataset_fields.get('product') == product.name]

        with self._db.connect() as connection:
            for record in connection.get_duplicates(group_fields, expressions):
                dataset_ids = set(record[0])
                grouped_fields = tuple(record[1:])
                yield result_type(*grouped_fields), dataset_ids

    def can_update(self, dataset, updates_allowed=None):
        """
        Check if dataset can be updated. Return bool,safe_changes,unsafe_changes

        :param Dataset dataset: Dataset to update
        :param dict updates_allowed: Allowed updates
        :rtype: bool,list[change],list[change]
        """
        need_sources = dataset.sources is not None
        existing = self.get(dataset.id, include_sources=need_sources)
        if not existing:
            raise ValueError('Unknown dataset %s, cannot update – did you intend to add it?' % dataset.id)

        if dataset.type.name != existing.type.name:
            raise ValueError('Changing product is not supported. From %s to %s in %s' % (existing.type.name,
                                                                                         dataset.type.name,
                                                                                         dataset.id))

        # TODO: figure out (un)safe changes from metadata type?
        allowed = {
            # can always add more metadata
            tuple(): changes.allow_extension,
        }
        allowed.update(updates_allowed or {})

        doc_changes = get_doc_changes(existing.metadata_doc, jsonify_document(dataset.metadata_doc))
        good_changes, bad_changes = changes.classify_changes(doc_changes, allowed)

        return not bad_changes, good_changes, bad_changes

    def update(self, dataset, updates_allowed=None):
        """
        Update dataset metadata and location
        :param Dataset dataset: Dataset to update
        :param updates_allowed: Allowed updates
        :rtype: Dataset
        """
        existing = self.get(dataset.id)
        can_update, safe_changes, unsafe_changes = self.can_update(dataset, updates_allowed)

        if not safe_changes and not unsafe_changes:
            self._ensure_new_locations(dataset, existing)
            _LOG.info("No changes detected for dataset %s", dataset.id)
            return dataset

        if not can_update:
            full_message = "Unsafe changes at " + ", ".join(".".join(offset) for offset, _, _ in unsafe_changes)
            raise ValueError(full_message)

        _LOG.info("Updating dataset %s", dataset.id)

        for offset, old_val, new_val in safe_changes:
            _LOG.info("Safe change from %r to %r", old_val, new_val)

        for offset, old_val, new_val in unsafe_changes:
            _LOG.info("Unsafe change from %r to %r", old_val, new_val)

        product = self.types.get_by_name(dataset.type.name)
        with self._db.begin() as transaction:
            if not transaction.update_dataset(dataset.metadata_doc_without_lineage(), dataset.id, product.id):
                raise ValueError("Failed to update dataset %s..." % dataset.id)

        self._ensure_new_locations(dataset, existing)

        return dataset

    def _ensure_new_locations(self, dataset, existing=None, transaction=None):
        skip_set = set([None] + existing.uris if existing is not None else [])
        new_uris = [uri for uri in dataset.uris if uri not in skip_set]

        def insert_one(uri, transaction):
            return transaction.insert_dataset_location(dataset.id, uri)

        # process in reverse order, since every add is essentially append to
        # front of a stack
        for uri in new_uris[::-1]:
            if transaction is None:
                with self._db.begin() as tr:
                    insert_one(uri, tr)
            else:
                insert_one(uri, transaction)

    def archive(self, ids):
        """
        Mark datasets as archived

        :param list[UUID] ids: list of dataset ids to archive
        """
        with self._db.begin() as transaction:
            for id_ in ids:
                transaction.archive_dataset(id_)

    def restore(self, ids):
        """
        Mark datasets as not archived

        :param list[UUID] ids: list of dataset ids to restore
        """
        with self._db.begin() as transaction:
            for id_ in ids:
                transaction.restore_dataset(id_)

    def get_field_names(self, type_name=None):
        """
        :param str type_name:
        :rtype: set[str]
        """
        if type_name is None:
            types = self.types.get_all()
        else:
            types = [self.types.get_by_name(type_name)]

        out = set()
        for type_ in types:
            out.update(type_.metadata_type.dataset_fields)
        return out

    def get_locations(self, id_):
        """
        :param typing.Union[UUID, str] id_: dataset id
        :rtype: list[str]
        """
        if isinstance(id_, Dataset):
            warnings.warn("Passing dataset is deprecated after 1.2.2, pass dataset.id", DeprecationWarning)
            id_ = id_.id

        with self._db.connect() as connection:
            return connection.get_locations(id_)

    def get_archived_locations(self, id_):
        """
        :param typing.Union[UUID, str] id_: dataset id
        :rtype: list[str]
        """
        if isinstance(id_, Dataset):
            warnings.warn("Passing dataset is deprecated after 1.2.2, pass dataset.id", DeprecationWarning)
            id_ = id_.id

        with self._db.connect() as connection:
            return [uri for uri, archived_dt in connection.get_archived_locations(id_)]

    def get_archived_location_times(self, id_):
        """
        Get each archived location along with the time it was archived.

        :param typing.Union[UUID, str] id_: dataset id
        :rtype: List[Tuple[str, datetime.datetime]]
        """
        if isinstance(id_, Dataset):
            raise RuntimeError("Passing a dataset has been deprecated for all index apis, and "
                               "is not supported in new apis. Pass the id of your dataset.")

        with self._db.connect() as connection:
            return list(connection.get_archived_locations(id_))

    def add_location(self, id_, uri):
        """
        Add a location to the dataset if it doesn't already exist.

        :param typing.Union[UUID, str] id_: dataset id
        :param str uri: fully qualified uri
        :returns bool: Was one added?
        """
        if isinstance(id_, Dataset):
            warnings.warn("Passing dataset is deprecated after 1.2.2, pass dataset.id", DeprecationWarning)
            id_ = id_.id

        if not uri:
            warnings.warn("Cannot add empty uri. (dataset %s)" % id_)
            return False

        with self._db.connect() as connection:
            return connection.insert_dataset_location(id_, uri)

    def get_datasets_for_location(self, uri, mode=None):
        with self._db.connect() as connection:
            return (self._make(row) for row in connection.get_datasets_for_location(uri, mode=mode))

    def remove_location(self, id_, uri):
        """
        Remove a location from the dataset if it exists.

        :param typing.Union[UUID, str] id_: dataset id
        :param str uri: fully qualified uri
        :returns bool: Was one removed?
        """
        if isinstance(id_, Dataset):
            warnings.warn("Passing dataset is deprecated after 1.2.2, pass dataset.id", DeprecationWarning)
            id_ = id_.id

        with self._db.connect() as connection:
            was_removed = connection.remove_location(id_, uri)
            return was_removed

    def archive_location(self, id_, uri):
        """
        Archive a location of the dataset if it exists.

        :param typing.Union[UUID, str] id_: dataset id
        :param str uri: fully qualified uri
        :return bool: location was able to be archived
        """
        if isinstance(id_, Dataset):
            warnings.warn("Passing dataset is deprecated after 1.2.2, pass dataset.id", DeprecationWarning)
            id_ = id_.id

        with self._db.connect() as connection:
            was_archived = connection.archive_location(id_, uri)
            return was_archived

    def restore_location(self, id_, uri):
        """
        Un-archive a location of the dataset if it exists.

        :param typing.Union[UUID, str] id_: dataset id
        :param str uri: fully qualified uri
        :return bool: location was able to be restored
        """
        if isinstance(id_, Dataset):
            warnings.warn("Passing dataset is deprecated after 1.2.2, pass dataset.id", DeprecationWarning)
            id_ = id_.id

        with self._db.connect() as connection:
            was_restored = connection.restore_location(id_, uri)
            return was_restored

    def _make(self, dataset_res, full_info=False):
        """
        :rtype Dataset

        :param bool full_info: Include all available fields
        """
        if dataset_res.uris:
            uris = [uri for uri in dataset_res.uris if uri]
        else:
            uris = []

        return Dataset(
            type_=self.types.get(dataset_res.dataset_type_ref),
            metadata_doc=dataset_res.metadata,
            uris=uris,
            indexed_by=dataset_res.added_by if full_info else None,
            indexed_time=dataset_res.added if full_info else None,
            archived_time=dataset_res.archived
        )

    def _make_many(self, query_result):
        """
        :rtype list[Dataset]
        """
        return (self._make(dataset) for dataset in query_result)

    def search_by_metadata(self, metadata):
        """
        Perform a search using arbitrary metadata, returning results as Dataset objects.

        Caution – slow! This will usually not use indexes.

        :param dict metadata:
        :rtype: list[Dataset]
        """
        with self._db.connect() as connection:
            for dataset in self._make_many(connection.search_datasets_by_metadata(metadata)):
                yield dataset

    def search(self, limit=None, **query):
        """
        Perform a search, returning results as Dataset objects.

        :param Union[str,float,Range,list] query:
        :param int limit: Limit number of datasets
        :rtype: __generator[Dataset]
        """
        source_filter = query.pop('source_filter', None)
        for _, datasets in self._do_search_by_product(query,
                                                      source_filter=source_filter,
                                                      limit=limit):
            for dataset in self._make_many(datasets):
                yield dataset

    def search_by_product(self, **query):
        """
        Perform a search, returning datasets grouped by product type.

        :param dict[str,str|float|datacube.model.Range] query:
        :rtype: __generator[(DatasetType,  __generator[Dataset])]]
        """
        for product, datasets in self._do_search_by_product(query):
            yield product, self._make_many(datasets)

    def search_returning(self, field_names, limit=None, **query):
        """
        Perform a search, returning only the specified fields.

        This method can be faster than normal search() if you don't need all fields of each dataset.

        It also allows for returning rows other than datasets, such as a row per uri when requesting field 'uri'.

        :param tuple[str] field_names:
        :param Union[str,float,Range,list] query:
        :param int limit: Limit number of datasets
        :returns __generator[tuple]: sequence of results, each result is a namedtuple of your requested fields
        """
        result_type = namedtuple('search_result', field_names)

        for _, results in self._do_search_by_product(query,
                                                     return_fields=True,
                                                     select_field_names=field_names,
                                                     limit=limit):

            for columns in results:
                yield result_type(*columns)

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
        :rtype: __generator[(DatasetType,  int)]]
        """
        return self._do_count_by_product(query)

    def count_by_product_through_time(self, period, **query):
        """
        Perform a search, returning counts for each product grouped in time slices
        of the given period.

        :param dict[str,str|float|datacube.model.Range] query:
        :param str period: Time range for each slice: '1 month', '1 day' etc.
        :returns: For each matching product type, a list of time ranges and their count.
        :rtype: __generator[(DatasetType, list[(datetime.datetime, datetime.datetime), int)]]
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
                raise ValueError('No type of dataset has fields: {}'.format(q.keys()))

        return types

    def _get_product_queries(self, query):
        for product, q in self.types.search_robust(**query):
            q['dataset_type_id'] = product.id
            yield q, product

    # pylint: disable=too-many-locals
    def _do_search_by_product(self, query, return_fields=False, select_field_names=None,
                              with_source_ids=False, source_filter=None,
                              limit=None):
        if source_filter:
            product_queries = list(self._get_product_queries(source_filter))
            if not product_queries:
                # No products match our source filter, so there will be no search results regardless.
                raise ValueError('No products match source filter: ' % source_filter)
            if len(product_queries) > 1:
                raise RuntimeError("Multi-product source filters are not supported. Try adding 'product' field")

            source_queries, source_product = product_queries[0]
            dataset_fields = source_product.metadata_type.dataset_fields
            source_exprs = tuple(fields.to_expressions(dataset_fields.get, **source_queries))
        else:
            source_exprs = None

        product_queries = list(self._get_product_queries(query))
        if not product_queries:
            raise ValueError('No products match search terms: %r' % query)

        for q, product in product_queries:
            dataset_fields = product.metadata_type.dataset_fields
            query_exprs = tuple(fields.to_expressions(dataset_fields.get, **q))
            select_fields = None
            if return_fields:
                # if no fields specified, select all
                if select_field_names is None:
                    select_fields = tuple(field for name, field in dataset_fields.items()
                                          if not field.affects_row_selection)
                else:
                    select_fields = tuple(dataset_fields[field_name]
                                          for field_name in select_field_names)
            with self._db.connect() as connection:
                yield (product,
                       connection.search_datasets(
                           query_exprs,
                           source_exprs,
                           select_fields=select_fields,
                           limit=limit,
                           with_source_ids=with_source_ids
                       ))

    def _do_count_by_product(self, query):
        product_queries = self._get_product_queries(query)

        for q, product in product_queries:
            dataset_fields = product.metadata_type.dataset_fields
            query_exprs = tuple(fields.to_expressions(dataset_fields.get, **q))
            with self._db.connect() as connection:
                count = connection.count_datasets(query_exprs)
            if count > 0:
                yield product, count

    def _do_time_count(self, period, query, ensure_single=False):
        if 'time' not in query:
            raise ValueError('Counting through time requires a "time" range query argument')

        query = dict(query)

        start, end = query['time']
        del query['time']

        product_queries = list(self._get_product_queries(query))
        if ensure_single:
            if len(product_queries) == 0:
                raise ValueError('No products match search terms: %r' % query)
            if len(product_queries) > 1:
                raise ValueError('Multiple products match single query search: %r' %
                                 ([dt.name for q, dt in product_queries],))

        for q, product in product_queries:
            dataset_fields = product.metadata_type.dataset_fields
            query_exprs = tuple(fields.to_expressions(dataset_fields.get, **q))
            with self._db.connect() as connection:
                yield product, list(connection.count_datasets_through_time(
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
        :rtype: __generator[dict]
        """
        for _, results in self._do_search_by_product(query, return_fields=True):
            for columns in results:
                yield dict(columns)

    def search_eager(self, **query):
        """
        Perform a search, returning results as Dataset objects.

        :param dict[str,str|float|datacube.model.Range] query:
        :rtype: list[Dataset]
        """
        return list(self.search(**query))

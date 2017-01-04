# coding=utf-8
"""
API for dataset indexing, access and search.
"""
from __future__ import absolute_import

import logging

from cachetools.func import lru_cache
from datacube import compat
from datacube.model import Dataset, DatasetType, MetadataType
from datacube.utils import InvalidDocException, jsonify_document, changes
from datacube.utils.changes import get_doc_changes, check_doc_unchanged

from . import fields
from .exceptions import DuplicateRecordError, UnknownFieldError

_LOG = logging.getLogger(__name__)

# It's a public api, so we can't reorganise old methods.
# pylint: disable=too-many-public-methods


class MetadataTypeResource(object):
    def __init__(self, db):
        """
        :type db: datacube.index.postgres._api.PostgresDb
        """
        self._db = db

    def from_doc(self, definition):
        """
        :param dict definition:
        :rtype: datacube.model.MetadataType
        """
        MetadataType.validate(definition)
        return self._make(definition)

    def add(self, metadata_type, allow_table_lock=False):
        """
        :param datacube.model.MetadataType metadata_type:
        :param allow_table_lock:
            Allow an exclusive lock to be taken on the table while creating the indexes.
            This will halt other user's requests until completed.

            If false, creation will be slightly slower and cannot be done in a transaction.
        :rtype: datacube.model.MetadataType
        """
        # This column duplication is getting out of hand:
        MetadataType.validate(metadata_type.definition)

        existing = self.get_by_name(metadata_type.name)
        if existing:
            # They've passed us the same one again. Make sure it matches what is stored.
            check_doc_unchanged(
                existing.definition,
                jsonify_document(metadata_type.definition),
                'Metadata Type {}'.format(metadata_type.name)
            )
        else:
            with self._db.connect() as connection:
                connection.add_metadata_type(
                    name=metadata_type.name,
                    definition=metadata_type.definition,
                    concurrently=not allow_table_lock
                )
        return self.get_by_name(metadata_type.name)

    def can_update(self, metadata_type, allow_unsafe_updates=False):
        """
        Check if metadata type can be updated. Return bool,safe_changes,unsafe_changes

        Safe updates currently allow new search fields to be added, description to be changed.

        :param datacube.model.MetadataType metadata_type: updated MetadataType
        :param bool allow_unsafe_updates: Allow unsafe changes. Use with caution.
        :rtype: bool,list[change],list[change]
        """
        MetadataType.validate(metadata_type.definition)

        existing = self.get_by_name(metadata_type.name)
        if not existing:
            raise ValueError('Unknown metadata type %s, cannot update – did you intend to add it?' % metadata_type.name)

        updates_allowed = {
            ('description',): changes.allow_any,
            # You can add new fields safely but not modify existing ones.
            ('dataset',): changes.allow_extension,
            ('dataset', 'search_fields'): changes.allow_extension
        }

        doc_changes = get_doc_changes(existing.definition, jsonify_document(metadata_type.definition))
        good_changes, bad_changes = changes.classify_changes(doc_changes, updates_allowed)

        return allow_unsafe_updates or not bad_changes, good_changes, bad_changes

    def update(self, metadata_type, allow_unsafe_updates=False):
        """
        Update a metadata type from the document. Unsafe changes will throw a ValueError by default.

        Safe updates currently allow new search fields to be added, description to be changed.

        :param datacube.model.MetadataType metadata_type: updated MetadataType
        :param bool allow_unsafe_updates: Allow unsafe changes. Use with caution.
        :rtype: datacube.model.MetadataType
        """
        can_update, safe_changes, unsafe_changes = self.can_update(metadata_type, allow_unsafe_updates)

        if not safe_changes and not unsafe_changes:
            _LOG.info("No changes detected for metadata type %s", metadata_type.name)
            return

        if not can_update:
            full_message = "Unsafe changes at " + ", ".join(".".join(offset) for offset, _, _ in unsafe_changes)
            raise ValueError(full_message)

        _LOG.info("Updating metadata type %s", metadata_type.name)

        for offset, old_val, new_val in safe_changes:
            _LOG.info("Safe change from %r to %r", old_val, new_val)

        for offset, old_val, new_val in unsafe_changes:
            _LOG.info("Unsafe change from %r to %r", old_val, new_val)

        with self._db.connect() as connection:
            connection.update_metadata_type(
                name=metadata_type.name,
                definition=metadata_type.definition,
                concurrently=True
            )

        self.get_by_name_unsafe.cache_clear()
        self.get_unsafe.cache_clear()

    def update_document(self, definition, allow_unsafe_updates=False):
        """
        Update a metadata type from the document. Unsafe changes will throw a ValueError by default.

        Safe updates currently allow new search fields to be added, description to be changed.

        :param dict definition: Updated definition
        :param bool allow_unsafe_updates: Allow unsafe changes. Use with caution.
        :rtype: datacube.model.MetadataType
        """
        return self.update(self.from_doc(definition), allow_unsafe_updates=allow_unsafe_updates)

    def get(self, id_):
        """
        :rtype: datacube.model.MetadataType
        """
        try:
            return self.get_unsafe(id_)
        except KeyError:
            return None

    def get_by_name(self, name):
        """
        :rtype: datacube.model.MetadataType
        """
        try:
            return self.get_by_name_unsafe(name)
        except KeyError:
            return None

    @lru_cache()
    def get_unsafe(self, id_):
        with self._db.connect() as connection:
            record = connection.get_metadata_type(id_)
        if record is None:
            raise KeyError('%s is not a valid MetadataType id')
        return self._make_from_query_row(record)

    @lru_cache()
    def get_by_name_unsafe(self, name):
        with self._db.connect() as connection:
            record = connection.get_metadata_type_by_name(name)
        if not record:
            raise KeyError('%s is not a valid MetadataType name' % name)
        return self._make_from_query_row(record)

    def check_field_indexes(self, allow_table_lock=False, rebuild_all=False):
        """
        Create or replace per-field indexes and views.
        :param allow_table_lock:
            Allow an exclusive lock to be taken on the table while creating the indexes.
            This will halt other user's requests until completed.

            If false, creation will be slightly slower and cannot be done in a transaction.
        """
        with self._db.connect() as connection:
            connection.check_dynamic_fields(concurrently=not allow_table_lock, rebuild_all=rebuild_all)

    def get_all(self):
        """
        Retrieve all Metadata Types

        :rtype: iter[datacube.model.MetadataType]
        """
        with self._db.connect() as connection:
            return self._make_many(connection.get_all_metadata_types())

    def _make_many(self, query_rows):
        """
        :rtype: list[datacube.model.MetadataType]
        """
        return (self._make_from_query_row(c) for c in query_rows)

    def _make_from_query_row(self, query_row):
        """
        :rtype: datacube.model.MetadataType
        """
        return self._make(query_row['definition'], query_row['id'])

    def _make(self, definition, id_=None):
        """
        :param dict definition:
        :param int id_:
        :rtype: datacube.model.MetadataType
        """
        return MetadataType(
            definition,
            dataset_search_fields=self._db.get_dataset_fields(definition['dataset']['search_fields']),
            id_=id_
        )


class ProductResource(object):
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
            metadata_type = self.metadata_type_resource.from_doc(metadata_type)
            definition = definition.copy()
            definition['metadata_type'] = metadata_type.name

        if not metadata_type:
            raise InvalidDocException('Unknown metadata type: %r' % definition['metadata_type'])

        return DatasetType(metadata_type, definition)

    def add(self, type_):
        """
        Add a Product.

        :param datacube.model.DatasetType type_: Product to add
        :rtype: datacube.model.DatasetType
        """
        DatasetType.validate(type_.definition)

        existing = self.get_by_name(type_.name)
        if existing:
            check_doc_unchanged(
                existing.definition,
                jsonify_document(type_.definition),
                'Metadata Type {}'.format(type_.name)
            )
        else:
            metadata_type = self.metadata_type_resource.get_by_name(type_.metadata_type.name)
            if metadata_type is None:
                _LOG.warning('Adding metadata_type "%s" as it doesn\'t exist.', type_.metadata_type.name)
                metadata_type = self.metadata_type_resource.add(type_.metadata_type)
            with self._db.connect() as connection:
                connection.add_dataset_type(
                    name=type_.name,
                    metadata=type_.metadata_doc,
                    metadata_type_id=metadata_type.id,
                    search_fields=metadata_type.dataset_fields,
                    definition=type_.definition
                )
        return self.get_by_name(type_.name)

    def can_update(self, product, allow_unsafe_updates=False):
        """
        Check if product can be updated. Return bool,safe_changes,unsafe_changes

        (An unsafe change is anything that may potentially make the product
        incompatible with existing datasets of that type)

        :param datacube.model.DatasetType product: Product to update
        :param bool allow_unsafe_updates: Allow unsafe changes. Use with caution.
            :rtype: bool,list[change],list[change]
        """
        DatasetType.validate(product.definition)

        existing = self.get_by_name(product.name)
        if not existing:
            raise ValueError('Unknown product %s, cannot update – did you intend to add it?' % product.name)

        updates_allowed = {
            ('description',): changes.allow_any,
            ('metadata_type',): changes.allow_any,

            # You can safely make the match rules looser but not tighter.
            # Tightening them could exclude datasets already matched to the product.
            # (which would make search results wrong)
            ('metadata',): changes.allow_truncation
        }

        doc_changes = get_doc_changes(existing.definition, jsonify_document(product.definition))
        good_changes, bad_changes = changes.classify_changes(doc_changes, updates_allowed)

        return allow_unsafe_updates or not bad_changes, good_changes, bad_changes

    def update(self, product, allow_unsafe_updates=False):
        """
        Update a product. Unsafe changes will throw a ValueError by default.

        (An unsafe change is anything that may potentially make the product
        incompatible with existing datasets of that type)

        :param datacube.model.DatasetType product: Product to update
        :param bool allow_unsafe_updates: Allow unsafe changes. Use with caution.
        :rtype: datacube.model.DatasetType
        """

        can_update, safe_changes, unsafe_changes = self.can_update(product, allow_unsafe_updates)

        if not safe_changes and not unsafe_changes:
            _LOG.info("No changes detected for product %s", product.name)
            return

        if not can_update:
            full_message = "Unsafe changes at " + ", ".join(".".join(offset) for offset, _, _ in unsafe_changes)
            raise ValueError(full_message)

        _LOG.info("Updating product %s", product.name)

        for offset, old_val, new_val in safe_changes:
            _LOG.info("Safe change from %r to %r", old_val, new_val)

        for offset, old_val, new_val in unsafe_changes:
            _LOG.info("Unsafe change from %r to %r", old_val, new_val)

        existing = self.get_by_name(product.name)
        changing_metadata_type = product.metadata_type.name != existing.metadata_type.name
        if changing_metadata_type:
            assert False, "TODO: Ask Jeremy WTF is going on here"
            # TODO: Ask Jeremy WTF is going on here
            # If the two metadata types declare the same field with different postgres expressions
            # we can't safely change it.
            # (Replacing the index would cause all existing users to have no effective index)
            # for name, field in existing.metadata_type.dataset_fields.items():
            #     new_field = type_.metadata_type.dataset_fields.get(name)
            #     if new_field and (new_field.sql_expression != field.sql_expression):
            #         declare_unsafe(
            #             ('metadata_type',),
            #             'Metadata type change results in incompatible index '
            #             'for {!r} ({!r} → {!r})'.format(
            #                 name, field.sql_expression, new_field.sql_expression
            #             )
            #         )
        metadata_type = self.metadata_type_resource.get_by_name(product.metadata_type.name)
        # TODO: should we add metadata type here?
        assert metadata_type, "TODO: should we add metadata type here?"
        with self._db.begin() as trans:
            trans.update_dataset_type(
                name=product.name,
                metadata=product.metadata_doc,
                metadata_type_id=metadata_type.id,
                search_fields=metadata_type.dataset_fields,
                definition=product.definition,
                update_metadata_type=changing_metadata_type
            )

        self.get_by_name_unsafe.cache_clear()
        self.get_unsafe.cache_clear()

    def update_document(self, definition, allow_unsafe_updates=False):
        """
        Update a Product using its difinition

        :param bool allow_unsafe_updates: Allow unsafe changes. Use with caution.
        :param dict definition: product definition document
        :rtype: datacube.model.DatasetType
        """
        type_ = self.from_doc(definition)
        return self.update(type_, allow_unsafe_updates=allow_unsafe_updates)

    def add_document(self, definition):
        """
        Add a Product using its difinition

        :param dict definition: product definition document
        :rtype: datacube.model.DatasetType
        """
        type_ = self.from_doc(definition)
        return self.add(type_)

    def get(self, id_):
        """
        Retrieve Product by id

        :param int id_: id of the Product
        :rtype: datacube.model.DatasetType
        """
        try:
            return self.get_unsafe(id_)
        except KeyError:
            return None

    def get_by_name(self, name):
        """
        Retrieve Product by name

        :param str name: name of the Product
        :rtype: datacube.model.DatasetType
        """
        try:
            return self.get_by_name_unsafe(name)
        except KeyError:
            return None

    @lru_cache()
    def get_unsafe(self, id_):
        with self._db.connect() as connection:
            result = connection.get_dataset_type(id_)
        if not result:
            raise KeyError('"%s" is not a valid Product id' % id_)
        return self._make(result)

    @lru_cache()
    def get_by_name_unsafe(self, name):
        with self._db.connect() as connection:
            result = connection.get_dataset_type_by_name(name)
        if not result:
            raise KeyError('"%s" is not a valid Product name' % name)
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
                except UnknownFieldError as e:
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
        with self._db.connect() as connection:
            return (self._make(record) for record in connection.get_all_dataset_types())

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
    :type types: datacube.index._datasets.ProductResource
    """

    def __init__(self, db, dataset_type_resource):
        """
        :type db: datacube.index.postgres._api.PostgresDb
        :type dataset_type_resource: datacube.index._datasets.ProductResource
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
        Get all derived datasets

        :param uuid id_: dataset id
        :rtype: list[datacube.model.Dataset]
        """
        with self._db.connect() as connection:
            return [self._make(result, full_info=True)
                    for result in connection.get_derived_datasets(id_)]

    def has(self, id_):
        """
        Have we already indexed this dataset?

        :param uuid id_: dataset id
        :rtype: bool
        """
        with self._db.connect() as connection:
            return connection.contains_dataset(id_)

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
            product = self.types.get_by_name(dataset.type.name)
            if product is None:
                _LOG.warning('Adding product "%s" as it doesn\'t exist.', dataset.type.name)
                product = self.types.add(dataset.type)
            with self._db.begin() as transaction:
                try:
                    was_inserted = transaction.insert_dataset(dataset.metadata_doc, dataset.id, product.id)
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
                        with self._db.connect() as connection:
                            connection.ensure_dataset_location(dataset.id, dataset.local_uri)
                    except DuplicateRecordError as e:
                        _LOG.warning(str(e))
        finally:
            dataset.type.dataset_reader(dataset.metadata_doc).sources = sources_tmp

        return dataset

    def can_update(self, dataset, updates_allowed=None):
        """
        Check if dataset can be updated. Return bool,safe_changes,unsafe_changes

        :param datacube.model.Dataset dataset: Dataset to update
        :param dict updates_allowed: Allowed updates
            :rtype: bool,list[change],list[change]
        """
        existing = self.get(dataset.id, include_sources=True)
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
        :param datacube.model.Dataset dataset: Dataset to update
        :param updates_allowed: Allowed updates
        :return:
        """
        existing = self.get(dataset.id)
        can_update, safe_changes, unsafe_changes = self.can_update(dataset, updates_allowed)

        if not safe_changes and not unsafe_changes:
            if dataset.local_uri != existing.local_uri:
                with self._db.begin() as transaction:
                    transaction.ensure_dataset_location(dataset.id, dataset.local_uri)
            _LOG.info("No changes detected for dataset %s", dataset.id)
            return

        if not can_update:
            full_message = "Unsafe changes at " + ", ".join(".".join(offset) for offset, _, _ in unsafe_changes)
            raise ValueError(full_message)

        _LOG.info("Updating dataset %s", dataset.id)

        for offset, old_val, new_val in safe_changes:
            _LOG.info("Safe change from %r to %r", old_val, new_val)

        for offset, old_val, new_val in unsafe_changes:
            _LOG.info("Unsafe change from %r to %r", old_val, new_val)

        sources_tmp = dataset.type.dataset_reader(dataset.metadata_doc).sources
        dataset.type.dataset_reader(dataset.metadata_doc).sources = {}
        try:
            product = self.types.get_by_name(dataset.type.name)
            with self._db.begin() as transaction:
                if not transaction.update_dataset(dataset.metadata_doc, dataset.id, product.id):
                    raise ValueError("Failed to update dataset %s..." % dataset.id)

                if dataset.local_uri != existing.local_uri:
                    transaction.ensure_dataset_location(dataset.id, dataset.local_uri)
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

    def get_locations(self, dataset):
        """
        :param datacube.model.Dataset dataset: dataset
        :rtype: list[str]
        """
        with self._db.connect() as connection:
            return connection.get_locations(dataset.id)

    def add_location(self, dataset, uri):
        """
        Add a location to the dataset.
        :param datacube.model.Dataset dataset: dataset
        :param str uri: fully qualified uri
        """
        with self._db.connect() as connection:
            return connection.ensure_dataset_location(dataset.id, uri)

    def remove_location(self, dataset, uri):
        """
        Remove a location from the dataset if it exists.
        :param datacube.model.Dataset dataset: dataset
        :param str uri: fully qualified uri
        :returns: True if a matching one was found
        """
        with self._db.connect() as connection:
            was_removed = connection.remove_location(dataset.id, uri)
            return was_removed

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
            indexed_time=dataset_res.added if full_info else None,
            archived_time=dataset_res.archived
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
        with self._db.connect() as connection:
            for dataset in self._make_many(connection.search_datasets_by_metadata(metadata)):
                yield dataset

    def search(self, **query):
        """
        Perform a search, returning results as Dataset objects.

        :param dict[str,str|float|datacube.model.Range] query:
        :rtype: __generator[datacube.model.Dataset]
        """
        source_filter = query.pop('source_filter', None)
        for _, datasets in self._do_search_by_product(query, source_filter=source_filter):
            for dataset in self._make_many(datasets):
                yield dataset

    def search_by_product(self, **query):
        """
        Perform a search, returning datasets grouped by product type.

        :param dict[str,str|float|datacube.model.Range] query:
        :rtype: __generator[(datacube.model.DatasetType,  __generator[datacube.model.Dataset])]]
        """
        for product, datasets in self._do_search_by_product(query):
            yield product, self._make_many(datasets)

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
        for product, q in self.types.search_robust(**query):
            q['dataset_type_id'] = product.id
            yield q, product

    def _do_search_by_product(self, query, return_fields=False, with_source_ids=False, source_filter=None):
        if source_filter:
            product_queries = list(self._get_product_queries(source_filter))
            if len(product_queries) != 1:
                raise RuntimeError("Multi-product source filters are not supported. Try adding 'product' field")
            source_queries, source_product = product_queries[0]
            dataset_fields = source_product.metadata_type.dataset_fields
            source_exprs = tuple(fields.to_expressions(dataset_fields.get, **source_queries))
        else:
            source_exprs = None

        product_queries = list(self._get_product_queries(query))
        with self._db.connect() as connection:
            for q, product in product_queries:
                dataset_fields = product.metadata_type.dataset_fields
                query_exprs = tuple(fields.to_expressions(dataset_fields.get, **q))
                select_fields = None
                if return_fields:
                    select_fields = tuple(dataset_fields.values())
                yield (product,
                       connection.search_datasets(
                           query_exprs,
                           source_exprs,
                           select_fields=select_fields,
                           with_source_ids=with_source_ids
                       ))

    def _do_count_by_product(self, query):
        product_queries = self._get_product_queries(query)
        with self._db.connect() as connection:
            for q, product in product_queries:
                dataset_fields = product.metadata_type.dataset_fields
                query_exprs = tuple(fields.to_expressions(dataset_fields.get, **q))
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

        with self._db.connect() as connection:
            for q, product in product_queries:
                dataset_fields = product.metadata_type.dataset_fields
                query_exprs = tuple(fields.to_expressions(dataset_fields.get, **q))
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
        :rtype: dict
        """
        for _, results in self._do_search_by_product(query, return_fields=True):
            for columns in results:
                yield dict(columns)

    def search_eager(self, **query):
        """
        Perform a search, returning results as Dataset objects.

        :param dict[str,str|float|datacube.model.Range] query:
        :rtype: list[datacube.model.Dataset]
        """
        return list(self.search(**query))

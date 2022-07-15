# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2020 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
import logging

from cachetools.func import lru_cache

from datacube.index import fields
from datacube.index.abstract import AbstractProductResource
from datacube.model import DatasetType, MetadataType
from datacube.utils import jsonify_document, changes, _readable_offset
from datacube.utils.changes import check_doc_unchanged, get_doc_changes

from typing import Iterable, cast

_LOG = logging.getLogger(__name__)


class ProductResource(AbstractProductResource):
    """
    :type _db: datacube.drivers.postgres._connections.PostgresDb
    :type metadata_type_resource: datacube.index._metadata_types.MetadataTypeResource
    """

    def __init__(self, db, metadata_type_resource):
        """
        :type db: datacube.drivers.postgres._connections.PostgresDb
        :type metadata_type_resource: datacube.index._metadata_types.MetadataTypeResource
        """
        self._db = db
        self.metadata_type_resource = metadata_type_resource

        self.get_unsafe = lru_cache()(self.get_unsafe)
        self.get_by_name_unsafe = lru_cache()(self.get_by_name_unsafe)

    def __getstate__(self):
        """
        We define getstate/setstate to avoid pickling the caches
        """
        return self._db, self.metadata_type_resource

    def __setstate__(self, state):
        """
        We define getstate/setstate to avoid pickling the caches
        """
        self.__init__(*state)

    def add(self, product, allow_table_lock=False):
        """
        Add a Product.

        :param allow_table_lock:
            Allow an exclusive lock to be taken on the table while creating the indexes.
            This will halt other user's requests until completed.

            If false, creation will be slightly slower and cannot be done in a transaction.
        :param DatasetType product: Product to add
        :rtype: DatasetType
        """
        DatasetType.validate(product.definition)

        existing = self.get_by_name(product.name)
        if existing:
            _LOG.warning(f"Product {product.name} is already in the database, checking for differences")
            check_doc_unchanged(
                existing.definition,
                jsonify_document(product.definition),
                'Metadata Type {}'.format(product.name)
            )
        else:
            metadata_type = self.metadata_type_resource.get_by_name(product.metadata_type.name)
            if metadata_type is None:
                _LOG.warning('Adding metadata_type "%s" as it doesn\'t exist.', product.metadata_type.name)
                metadata_type = self.metadata_type_resource.add(product.metadata_type,
                                                                allow_table_lock=allow_table_lock)
            with self._db.connect() as connection:
                connection.insert_product(
                    name=product.name,
                    metadata=product.metadata_doc,
                    metadata_type_id=metadata_type.id,
                    search_fields=metadata_type.dataset_fields,
                    definition=product.definition,
                    concurrently=not allow_table_lock,
                )
        return self.get_by_name(product.name)

    def can_update(self, product, allow_unsafe_updates=False):
        """
        Check if product can be updated. Return bool,safe_changes,unsafe_changes

        (An unsafe change is anything that may potentially make the product
        incompatible with existing datasets of that type)

        :param DatasetType product: Product to update
        :param bool allow_unsafe_updates: Allow unsafe changes. Use with caution.
        :rtype: bool,list[change],list[change]
        """
        DatasetType.validate(product.definition)

        existing = self.get_by_name(product.name)
        if not existing:
            raise ValueError('Unknown product %s, cannot update – did you intend to add it?' % product.name)

        updates_allowed = {
            ('description',): changes.allow_any,
            ('license',): changes.allow_any,
            ('metadata_type',): changes.allow_any,

            # You can safely make the match rules looser but not tighter.
            # Tightening them could exclude datasets already matched to the product.
            # (which would make search results wrong)
            ('metadata',): changes.allow_truncation,

            # Some old storage fields should not be in the product definition any more: allow removal.
            ('storage', 'chunking'): changes.allow_removal,
            ('storage', 'driver'): changes.allow_removal,
            ('storage', 'dimension_order'): changes.allow_removal,
        }

        doc_changes = get_doc_changes(existing.definition, jsonify_document(product.definition))
        good_changes, bad_changes = changes.classify_changes(doc_changes, updates_allowed)

        for offset, old_val, new_val in good_changes:
            _LOG.info("Safe change in %s from %r to %r", _readable_offset(offset), old_val, new_val)

        for offset, old_val, new_val in bad_changes:
            _LOG.warning("Unsafe change in %s from %r to %r", _readable_offset(offset), old_val, new_val)

        return allow_unsafe_updates or not bad_changes, good_changes, bad_changes

    def update(self, product: DatasetType, allow_unsafe_updates=False, allow_table_lock=False):
        """
        Update a product. Unsafe changes will throw a ValueError by default.

        (An unsafe change is anything that may potentially make the product
        incompatible with existing datasets of that type)

        :param DatasetType product: Product to update
        :param bool allow_unsafe_updates: Allow unsafe changes. Use with caution.
        :param allow_table_lock:
            Allow an exclusive lock to be taken on the table while creating the indexes.
            This will halt other user's requests until completed.

            If false, creation will be slower and cannot be done in a transaction.
        :rtype: DatasetType
        """

        can_update, safe_changes, unsafe_changes = self.can_update(product, allow_unsafe_updates)

        if not safe_changes and not unsafe_changes:
            _LOG.warning("No changes detected for product %s", product.name)
            return self.get_by_name(product.name)

        if not can_update:
            raise ValueError(f"Unsafe changes in {product.name}: " + (
                ", ".join(
                    _readable_offset(offset)
                    for offset, _, _ in unsafe_changes
                )
            ))

        _LOG.info("Updating product %s", product.name)

        existing = cast(DatasetType, self.get_by_name(product.name))
        changing_metadata_type = product.metadata_type.name != existing.metadata_type.name
        if changing_metadata_type:
            raise ValueError("Unsafe change: cannot (currently) switch metadata types for a product")
            #  In the past, an effort was made to allow changing metadata types where the new
            #  type extends the old type without breaking it.  Banning all metadata type changes
            #  is safer and simpler.
            #
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
        metadata_type = cast(MetadataType, self.metadata_type_resource.get_by_name(product.metadata_type.name))
        #     Given we cannot change metadata type because of the check above, and this is an
        #     update method, the metadata type is guaranteed to already exist.
        with self._db.connect() as conn:
            conn.update_product(
                name=product.name,
                metadata=product.metadata_doc,
                metadata_type_id=metadata_type.id,
                search_fields=metadata_type.dataset_fields,
                definition=product.definition,
                update_metadata_type=changing_metadata_type,
                concurrently=not allow_table_lock
            )

        self.get_by_name_unsafe.cache_clear()  # type: ignore[attr-defined]
        self.get_unsafe.cache_clear()          # type: ignore[attr-defined]
        return self.get_by_name(product.name)

    def update_document(self, definition, allow_unsafe_updates=False, allow_table_lock=False):
        """
        Update a Product using its definition

        :param bool allow_unsafe_updates: Allow unsafe changes. Use with caution.
        :param dict definition: product definition document
        :param allow_table_lock:
            Allow an exclusive lock to be taken on the table while creating the indexes.
            This will halt other user's requests until completed.

            If false, creation will be slower and cannot be done in a transaction.
        :rtype: DatasetType
        """
        type_ = self.from_doc(definition)
        return self.update(
            type_,
            allow_unsafe_updates=allow_unsafe_updates,
            allow_table_lock=allow_table_lock,
        )

    # This is memoized in the constructor
    # pylint: disable=method-hidden
    def get_unsafe(self, id_):  # type: ignore
        with self._db.connect() as connection:
            result = connection.get_product(id_)
        if not result:
            raise KeyError('"%s" is not a valid Product id' % id_)
        return self._make(result)

    # This is memoized in the constructor
    # pylint: disable=method-hidden
    def get_by_name_unsafe(self, name):  # type: ignore
        with self._db.connect() as connection:
            result = connection.get_product_by_name(name)
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

    def search_robust(self, **query):
        """
        Return dataset types that match match-able fields and dict of remaining un-matchable fields.

        :param dict query:
        :rtype: __generator[(DatasetType, dict)]
        """

        def _listify(v):
            if isinstance(v, tuple):
                return list(v)
            elif isinstance(v, list):
                return v
            else:
                return [v]

        for type_ in self.get_all():
            remaining_matchable = query.copy()
            # If they specified specific product/metadata-types, we can quickly skip non-matches.
            if type_.name not in _listify(remaining_matchable.pop('product', type_.name)):
                continue
            if type_.metadata_type.name not in _listify(remaining_matchable.pop('metadata_type',
                                                                                type_.metadata_type.name)):
                continue

            # Check that all the keys they specified match this product.
            for key, value in list(remaining_matchable.items()):
                field = type_.metadata_type.dataset_fields.get(key)
                if not field:
                    # This type doesn't have that field, so it cannot match.
                    break
                if not hasattr(field, 'extract'):
                    # non-document/native field
                    continue
                if field.extract(type_.metadata_doc) is None:
                    # It has this field but it's not defined in the type doc, so it's unmatchable.
                    continue

                expr = fields.as_expression(field, value)
                if expr.evaluate(type_.metadata_doc):
                    remaining_matchable.pop(key)
                else:
                    # A property doesn't match this type, skip to next type.
                    break

            else:
                yield type_, remaining_matchable

    def get_all(self) -> Iterable[DatasetType]:
        """
        Retrieve all Products
        """
        with self._db.connect() as connection:
            return (self._make(record) for record in connection.get_all_products())

    def _make_many(self, query_rows):
        return (self._make(c) for c in query_rows)

    def _make(self, query_row) -> DatasetType:
        return DatasetType(
            definition=query_row['definition'],
            metadata_type=cast(MetadataType, self.metadata_type_resource.get(query_row['metadata_type_ref'])),
            id_=query_row['id'],
        )

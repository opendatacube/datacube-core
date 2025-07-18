# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2025 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import datetime
import logging
from collections.abc import Generator, Iterable, Mapping, Sequence
from time import monotonic
from typing import TYPE_CHECKING, cast

from cachetools.func import lru_cache
from odc.geo.crs import CRS
from odc.geo.geom import Geometry
from typing_extensions import override

from datacube.drivers.postgis import PostGisDb
from datacube.index import fields
from datacube.index.abstract import AbstractProductResource, BatchStatus
from datacube.index.postgis._transaction import IndexResourceAddIn
from datacube.model import MetadataType, Product, QueryDict, QueryField
from datacube.utils import _readable_offset, changes, jsonify_document
from datacube.utils.changes import check_doc_unchanged, get_doc_changes
from datacube.utils.documents import JsonDict

if TYPE_CHECKING:
    from datacube.index.postgis.index import Index


_LOG: logging.Logger = logging.getLogger(__name__)


class ProductResource(AbstractProductResource, IndexResourceAddIn):
    """
    Postgis driver product resource implementation
    """

    def __init__(self, db: PostGisDb, index: Index) -> None:
        super().__init__(index)
        self._db = db
        self.get_unsafe = lru_cache()(self.get_unsafe)  # type: ignore[method-assign]
        self.get_by_name_unsafe = lru_cache()(self.get_by_name_unsafe)  # type: ignore[method-assign]

    def __getstate__(self):
        """
        We define getstate/setstate to avoid pickling the caches
        """
        return self._db, self._index.metadata_types

    def __setstate__(self, state):
        """
        We define getstate/setstate to avoid pickling the caches
        """
        self.__init__(*state)

    @override
    def add(self, product: Product, allow_table_lock: bool = False) -> Product | None:
        """
        Add a Product.

        :param allow_table_lock:
            Allow an exclusive lock to be taken on the table while creating the indexes.
            This will halt other user's requests until completed.

            If false, creation will be slightly slower and cannot be done in a transaction.
        :param product: Product to add
        """
        Product.validate(product.definition)  # type: ignore[attr-defined]

        existing = self.get_by_name(product.name)
        if existing:
            _LOG.warning(
                f"Product {product.name} is already in the database, checking for differences"
            )
            check_doc_unchanged(
                existing.definition,
                jsonify_document(product.definition),
                f"Metadata Type {product.name}",
            )
        else:
            metadata_type = self._index.metadata_types.get_by_name(
                product.metadata_type.name
            )
            if metadata_type is None:
                _LOG.warning(
                    'Adding metadata_type "%s" as it doesn\'t exist.',
                    product.metadata_type.name,
                )
                metadata_type = self._index.metadata_types.add(
                    product.metadata_type, allow_table_lock=allow_table_lock
                )
                if metadata_type is None:
                    _LOG.warning(
                        f"Adding metadata_type {product.metadata_type.name} failed"
                    )
                    return None

            with self._db_connection() as connection:
                connection.insert_product(
                    name=product.name,
                    metadata=product.metadata_doc,
                    metadata_type_id=metadata_type.id,
                    definition=product.definition,
                )
        return self.get_by_name(product.name)

    @override
    def _add_batch(self, batch_products: Iterable[Product]) -> BatchStatus:
        # Would be nice to keep this level of internals hidden from this layer,
        # but most efficient to do it before grabbing a connection and keep the implementation
        # as close to SQLAlchemy as possible.
        b_started = monotonic()
        values = [
            {
                "name": p.name,
                "metadata": p.metadata_doc,
                "metadata_type_ref": p.metadata_type.id,
                "definition": p.definition,
            }
            for p in batch_products
        ]
        with self._db_connection() as connection:
            added, skipped = connection.insert_product_bulk(values)
            return BatchStatus(added, skipped, monotonic() - b_started)

    @override
    def can_update(
        self,
        product: Product,
        allow_unsafe_updates: bool = False,
        allow_table_lock: bool = False,
    ) -> tuple[bool, Iterable[changes.Change], Iterable[changes.Change]]:
        """
        Check if product can be updated. Return bool,safe_changes,unsafe_changes

        (An unsafe change is anything that may potentially make the product
        incompatible with existing datasets of that type)

        :param product: Product to update
        :param allow_unsafe_updates: Allow unsafe changes. Use with caution.
        :param allow_table_lock:
            Allow an exclusive lock to be taken on the table while creating the indexes.
            This will halt other user's requests until completed.

            If false, creation will be slower and cannot be done in a transaction.
        """
        Product.validate(product.definition)  # type: ignore[attr-defined]

        existing = self.get_by_name(product.name)
        if not existing:
            raise ValueError(
                f"Unknown product {product.name}, cannot update - did you intend to add it?"
            )

        updates_allowed: Mapping[changes.Offset, changes.AllowPolicy] = {
            ("description",): changes.allow_any,
            ("license",): changes.allow_any,
            ("metadata_type",): changes.allow_any,
            # You can safely make the match rules looser but not tighter.
            # Tightening them could exclude datasets already matched to the product.
            # (which would make search results wrong)
            ("metadata",): changes.allow_truncation,
            # Some old storage fields should not be in the product definition any more: allow removal.
            ("storage", "chunking"): changes.allow_removal,
            ("storage", "driver"): changes.allow_removal,
            ("storage", "dimension_order"): changes.allow_removal,
        }

        doc_changes = get_doc_changes(
            existing.definition, jsonify_document(product.definition)
        )
        good_changes, bad_changes = changes.classify_changes(
            doc_changes, updates_allowed
        )

        for offset, old_val, new_val in good_changes:
            _LOG.info(
                "Safe change in %s from %r to %r",
                _readable_offset(offset),
                old_val,
                new_val,
            )

        for offset, old_val, new_val in bad_changes:
            _LOG.warning(
                "Unsafe change in %s from %r to %r",
                _readable_offset(offset),
                old_val,
                new_val,
            )

        return allow_unsafe_updates or not bad_changes, good_changes, bad_changes

    @override
    def update(
        self,
        product: Product,
        allow_unsafe_updates: bool = False,
        allow_table_lock: bool = False,
    ) -> Product | None:
        """
        Update a product. Unsafe changes will throw a ValueError by default.

        (An unsafe change is anything that may potentially make the product
        incompatible with existing datasets of that type)

        :param product: Product to update
        :param allow_unsafe_updates: Allow unsafe changes. Use with caution.
        :param allow_table_lock:
            Allow an exclusive lock to be taken on the table while creating the indexes.
            This will halt other user's requests until completed.

            If false, creation will be slower and cannot be done in a transaction.
        """
        can_update, safe_changes, unsafe_changes = self.can_update(
            product, allow_unsafe_updates
        )

        if not safe_changes and not unsafe_changes:
            _LOG.warning("No changes detected for product %s", product.name)
            return self.get_by_name(product.name)

        if not can_update:
            raise ValueError(
                f"Unsafe changes in {product.name}: "
                + (
                    ", ".join(
                        _readable_offset(offset) for offset, _, _ in unsafe_changes
                    )
                )
            )

        _LOG.info("Updating product %s", product.name)

        existing = self.get_by_name_unsafe(product.name)
        changing_metadata_type = (
            product.metadata_type.name != existing.metadata_type.name
        )
        if changing_metadata_type:
            raise ValueError(
                "Unsafe change: cannot (currently) switch metadata types for a product"
            )
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
        metadata_type = self._index.metadata_types.get_by_name(
            product.metadata_type.name
        )
        # TODO: should we add metadata type here?
        assert metadata_type, "TODO: should we add metadata type here?"
        with self._db_connection() as conn:
            conn.update_product(
                name=product.name,
                metadata=product.metadata_doc,
                metadata_type_id=metadata_type.id,
                definition=product.definition,
                update_metadata_type=changing_metadata_type,
            )

        self.get_by_name_unsafe.cache_clear()  # type: ignore[attr-defined]
        self.get_unsafe.cache_clear()  # type: ignore[attr-defined]
        return self.get_by_name(product.name)

    @override
    def update_document(
        self,
        definition: JsonDict,
        allow_unsafe_updates: bool = False,
        allow_table_lock: bool = False,
    ) -> Product | None:
        """
        Update a Product using its definition

        :param allow_unsafe_updates: Allow unsafe changes. Use with caution.
        :param definition: product definition document
        :param allow_table_lock:
            Allow an exclusive lock to be taken on the table while creating the indexes.
            This will halt other user's requests until completed.

            If false, creation will be slower and cannot be done in a transaction.
        """
        type_ = self.from_doc(definition)
        return self.update(
            type_,
            allow_unsafe_updates=allow_unsafe_updates,
            allow_table_lock=allow_table_lock,
        )

    @override
    def delete(
        self, products: Iterable[Product], allow_delete_active: bool = False
    ) -> Sequence[Product]:
        """
        Delete Products, as well as all related datasets

        :param products: the Products to delete
        :param allow_delete_active: Whether to delete active datasets
        :return: list of deleted products
        """
        deleted = []
        for product in products:
            with self._db_connection(transaction=True) as conn:
                # First find and delete all related datasets
                product_datasets = self._index.datasets.search_returning(
                    ("id",), archived=None, product=product.name
                )
                product_datasets = [ds.id for ds in product_datasets]  # type: ignore[attr-defined]
                purged = self._index.datasets.purge(
                    product_datasets, allow_delete_active
                )
                # if not all product datasets are purged, it must be because
                # we're not allowing active datasets to be purged
                if len(purged) != len(product_datasets):
                    _LOG.warning(
                        f"Product {product.name} cannot be deleted because it has active datasets."
                    )
                    continue
                # Now we can safely delete the Product
                conn.delete_product(product.name)
                deleted.append(product)
        return deleted

    # This is memoized in the constructor
    # pylint: disable=method-hidden
    @override
    def get_unsafe(self, id_: int) -> Product:
        with self._db_connection() as connection:
            result = connection.get_product(id_)
        if not result:
            raise KeyError(f'"{id_}" is not a valid Product id')
        return self._make(result)

    # This is memoized in the constructor
    # pylint: disable=method-hidden
    @override
    def get_by_name_unsafe(self, name: str) -> Product:
        with self._db_connection() as connection:
            result = connection.get_product_by_name(name)
        if not result:
            raise KeyError(f'"{name}" is not a valid Product name')
        return self._make(result)

    @override
    def search_robust(
        self, **query: QueryField
    ) -> Generator[tuple[Product, QueryDict]]:
        """
        Return dataset types that match match-able fields and dict of remaining un-matchable fields.
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
            if type_.name not in _listify(
                remaining_matchable.pop("product", type_.name)
            ):
                continue
            if type_.metadata_type.name not in _listify(
                remaining_matchable.pop("metadata_type", type_.metadata_type.name)
            ):
                continue

            # Check that all the keys they specified match this product.
            for key, value in list(remaining_matchable.items()):
                if key == "geopolygon":
                    # Geometry field is handled elsewhere by index drivers that support spatial indexes.
                    continue
                field = type_.metadata_type.dataset_fields.get(key)
                if not field:
                    # This type doesn't have that field, so it cannot match.
                    break
                if not field.can_extract:
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

    @override
    def search_by_metadata(self, metadata: JsonDict) -> Generator[Product]:
        """
        Perform a search using arbitrary metadata, returning results as Product objects.

        Caution - slow! This will usually not use indexes.

        :param metadata:
        """
        with self._db_connection() as connection:
            yield from self._make_many(connection.search_products_by_metadata(metadata))

    @override
    def get_all(self) -> Iterable[Product]:
        """
        Retrieve all Products
        """
        with self._db_connection() as connection:
            return self._make_many(connection.get_all_products())

    @override
    def get_all_docs(self) -> Iterable[JsonDict]:
        with self._db_connection() as connection:
            for row in connection.get_all_product_docs():
                yield row[0]

    def _make_many(self, query_rows):
        return (self._make(c) for c in query_rows)

    def _make(self, query_row) -> Product:
        return Product(
            definition=query_row.definition,
            metadata_type=cast(
                MetadataType,
                self._index.metadata_types.get(query_row.metadata_type_ref),
            ),
            id_=query_row.id,
        )

    @override
    def temporal_extent(
        self, product: str | Product
    ) -> tuple[datetime.datetime, datetime.datetime]:
        """
        Returns the minimum and maximum acquisition time of the product.
        """
        if isinstance(product, str):
            product = self.get_by_name_unsafe(product)
            assert isinstance(product, Product)
        assert product.id is not None  # for type checker
        with self._db_connection() as connection:
            result = connection.temporal_extent_by_prod(product.id)

        return result

    @override
    def spatial_extent(
        self, product: str | Product, crs: CRS = CRS("EPSG:4326")
    ) -> Geometry | None:
        if isinstance(product, str):
            product = self._index.products.get_by_name_unsafe(product)
        ids = [ds.id for ds in self._index.datasets.search(product=product.name)]
        with self._db_connection() as connection:
            return connection.spatial_extent(ids, crs)

    @override
    def most_recent_change(self, product: str | Product) -> datetime.datetime | None:
        if isinstance(product, str):
            product = self._index.products.get_by_name_unsafe(product)
        assert isinstance(product, Product)
        assert product.id is not None
        with self._db_connection() as connection:
            return connection.find_most_recent_change(product.id)

# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2022 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
import logging

from datacube.index.fields import as_expression
from datacube.index.abstract import AbstractProductResource, QueryField
from datacube.index.memory._metadata_types import MetadataTypeResource
from datacube.model import DatasetType as Product
from datacube.utils import changes, jsonify_document, _readable_offset
from datacube.utils.changes import AllowPolicy, Change, Offset, check_doc_unchanged, get_doc_changes, classify_changes
from typing import Iterable, Iterator, Mapping, Tuple, cast

_LOG = logging.getLogger(__name__)


class ProductResource(AbstractProductResource):
    def __init__(self, metadata_type_resource):
        self.metadata_type_resource: MetadataTypeResource = metadata_type_resource
        self.by_id = {}
        self.by_name = {}
        self.next_id = 1

    def add(self, product: Product, allow_table_lock: bool = False) -> Product:
        Product.validate(product.definition)  # type: ignore[attr-defined]
        existing = self.get_by_name(product.name)
        if existing:
            _LOG.warning(f"Product {product.name} is already in the database, checking for differences")
            check_doc_unchanged(
                existing.definition,
                jsonify_document(product.definition),
                f'Metadata Type {product.name}'
            )
        else:
            mdt = self.metadata_type_resource.get_by_name(product.metadata_type.name)
            if mdt is None:
                _LOG.warning(f'Adding metadata_type "{product.metadata_type.name}" as it doesn\'t exist')
                product.metadata_type = self.metadata_type_resource.add(product.metadata_type,
                                                                        allow_table_lock=allow_table_lock)
            clone = self.clone(product)
            clone.id = self.next_id
            self.next_id += 1
            self.by_id[clone.id] = clone
            self.by_name[clone.name] = clone
        return cast(Product, self.get_by_name(product.name))

    def can_update(self, product: Product,
                   allow_unsafe_updates: bool = False,
                   allow_table_lock: bool = False
                  ) -> Tuple[bool, Iterable[Change], Iterable[Change]]:
        Product.validate(product.definition)  # type: ignore[attr-defined]

        existing = self.get_by_name(product.name)
        if not existing:
            raise ValueError(f"Unknown product {product.name}, cannot update - add first")

        updates_allowed: Mapping[Offset, AllowPolicy] = {
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
        good_changes, bad_changes = classify_changes(doc_changes, updates_allowed)

        for offset, old_val, new_val in good_changes:
            _LOG.info("Safe change in %s from %r to %r", _readable_offset(offset), old_val, new_val)

        for offset, old_val, new_val in bad_changes:
            _LOG.warning("Unsafe change in %s from %r to %r", _readable_offset(offset), old_val, new_val)

        return (
            (allow_unsafe_updates or not bad_changes),
            good_changes,
            bad_changes,
        )

    def update(self, product: Product,
               allow_unsafe_updates: bool = False,
               allow_table_lock: bool = False) -> Product:
        can_update, safe_changes, unsafe_changes = self.can_update(product, allow_unsafe_updates)

        if not safe_changes and not unsafe_changes:
            _LOG.warning(f"No changes detected for product {product.name}")
            return cast(Product, self.get_by_name(product.name))

        if not can_update:
            errs = ", ".join(_readable_offset(offset) for offset, _, _ in unsafe_changes)
            raise ValueError(f"Unsafe changes in {product.name}: {errs}")

        existing = cast(Product, self.get_by_name(product.name))
        if product.metadata_type.name != existing.metadata_type.name:
            raise ValueError("Unsafe change: cannot (currently) switch metadata types for a product")

        _LOG.info(f"Updating product {product.name}")

        persisted = self.clone(product)
        persisted.id = existing.id
        self.by_id[persisted.id] = persisted
        self.by_name[persisted.name] = persisted
        return cast(Product, self.get_by_name(product.name))

    def get_unsafe(self, id_: int) -> Product:
        return self.clone(self.by_id[id_])

    def get_by_name_unsafe(self, name: str) -> Product:
        return self.clone(self.by_name[name])

    def get_with_fields(self, field_names: Iterable[str]) -> Iterable[Product]:
        for prod in self.get_all():
            for name in field_names:
                if name not in prod.metadata_type.dataset_fields:
                    break
            else:
                yield prod

    def search_robust(self, **query: QueryField) -> Iterator[Tuple[Product, Mapping[str, QueryField]]]:
        def listify(v):
            if isinstance(v, tuple):
                return list()
            elif isinstance(v, list):
                return v
            else:
                return [v]

        for prod in self.get_all():
            unmatched = query.copy()
            # Skip non-matched if user specified specific products/metadata_types
            if prod.name not in listify(unmatched.pop('product', prod.name)):
                continue
            if prod.metadata_type.name not in listify(unmatched.pop('metadata_type', prod.metadata_type.name)):
                continue
            # Check that all search keys match this product
            for key, value in list(unmatched.items()):
                field = prod.metadata_type.dataset_fields.get(key)
                if not field:
                    # Product doesn't have this field - can't match
                    break
                if not hasattr(field, 'extract'):
                    # non-document/native field (??)
                    continue
                if field.extract(prod.metadata_doc) is None:  # type: ignore[attr-defined]
                    # Product has the field, but not defined in the type doc, so unmatchable
                    continue
                expr = as_expression(field, value)
                if expr.evaluate(prod.metadata_doc):
                    # matches
                    unmatched.pop(key)
                else:
                    # Doesn't match, skip to next product
                    break
            else:
                yield prod, unmatched

    def get_all(self) -> Iterable[Product]:
        return (self.clone(prod) for prod in self.by_id.values())

    def clone(self, orig: Product) -> Product:
        return Product(
            self.metadata_type_resource.clone(orig.metadata_type),
            jsonify_document(orig.definition),
            id_=orig.id
        )

# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2025 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
import datetime
import logging
from abc import ABC, abstractmethod
from collections.abc import Generator, Iterable, Sequence
from time import monotonic
from typing import TYPE_CHECKING, cast

from odc.geo import CRS, Geometry

from datacube.model import MetadataType, Product, QueryDict, QueryField
from datacube.utils import InvalidDocException, jsonify_document
from datacube.utils.changes import Change, DocumentMismatchError, check_doc_unchanged
from datacube.utils.documents import JsonDict, JsonLike, UnknownMetadataType

from ._types import BatchStatus

if TYPE_CHECKING:
    from ._index import AbstractIndex

_LOG: logging.Logger = logging.getLogger(__name__)


class AbstractProductResource(ABC):
    """
    Abstract base class for the Product portion of an index api.

    All ProductResource implementations should inherit from this base
    class and implement all abstract methods.

    (If a particular abstract method is not applicable for a particular implementation
    raise a NotImplementedError)
    """

    def __init__(self, index: "AbstractIndex") -> None:
        self._index = index

    def from_doc(
        self,
        definition: JsonDict,
        metadata_type_cache: dict[str, MetadataType] | None = None,
    ) -> Product:
        """
        Construct unpersisted Product model from product metadata dictionary

        :param definition: a Product metadata dictionary
        :param metadata_type_cache: a dict cache of MetaDataTypes to use in constructing a Product.
                                    MetaDataTypes may come from a different index.
        :return: Unpersisted product model
        """
        # This column duplication is getting out of hand:
        Product.validate(definition)  # type: ignore[attr-defined]   # validate method added by decorator
        # Validate extra dimension metadata
        Product.validate_extra_dims(definition)

        metadata_type_in: str | JsonLike = definition["metadata_type"]

        # They either specified the name of a metadata type, or specified a metadata type.
        # Is it a name?
        if isinstance(metadata_type_in, str):
            if (
                metadata_type_cache is not None
                and metadata_type_in in metadata_type_cache
            ):
                metadata_type: MetadataType | None = metadata_type_cache[
                    metadata_type_in
                ]
            else:
                metadata_type = self._index.metadata_types.get_by_name(metadata_type_in)
                if (
                    metadata_type is not None
                    and metadata_type_cache is not None
                    and metadata_type.name not in metadata_type_cache
                ):
                    metadata_type_cache[metadata_type.name] = metadata_type
        else:
            # Otherwise they embedded a document, add it if needed:
            metadata_type = self._index.metadata_types.from_doc(
                cast(JsonDict, metadata_type_in)
            )
            definition = dict(definition)
            definition["metadata_type"] = metadata_type.name

        if metadata_type is None:
            raise UnknownMetadataType(
                f"Unknown metadata type: {definition['metadata_type']!r}"
            )

        return Product(metadata_type, definition)

    @abstractmethod
    def add(self, product: Product, allow_table_lock: bool = False) -> Product | None:
        """
        Add a product to the index.

        :param product: Unpersisted Product model
        :param allow_table_lock:
            Allow an exclusive lock to be taken on the table while creating the indexes.
            This will halt other user's requests until completed.

            If false, creation will be slightly slower and cannot be done in a transaction.

            raise NotImplementedError if set to True, and this behaviour is not applicable
            for the implementing driver.
        :return: Persisted Product model.
        """

    def _add_batch(self, batch_products: Iterable[Product]) -> BatchStatus:
        """
        Add a single "batch" of products.

        Default implementation is simple loop of add

        API Note: This API method is not finalised and may be subject to change.

        :param batch_products: An iterable of one batch's worth of Product objects to add
        :return: BatchStatus named tuple.
        """
        b_skipped = 0
        b_added = 0
        b_started = monotonic()
        for prod in batch_products:
            try:
                self.add(prod)
                b_added += 1
            except DocumentMismatchError as e:
                _LOG.warning("%s: Skipping", str(e))
                b_skipped += 1
            except Exception as e:
                _LOG.warning("%s: Skipping", str(e))
                b_skipped += 1
        return BatchStatus(b_added, b_skipped, monotonic() - b_started)

    def bulk_add(
        self,
        product_docs: Iterable[JsonDict],
        metadata_types: dict[str, MetadataType] | None = None,
        batch_size: int = 1000,
    ) -> BatchStatus:
        """
        Add a group of product documents in bulk.

        API Note: This API method is not finalised and may be subject to change.

        :param product_docs: An iterable of product metadata docs.
        :param batch_size: Number of products to add per batch (default 1000)
        :param metadata_types: Optional dictionary cache of MetadataType objects.
                               Used for product metadata validation, and for filtering.
                               (Metadata types not in in this list are skipped.)
        :return: BatchStatus named tuple, with `safe` containing a list of
                 product names that are safe to include in a subsequent dataset bulk add.
        """
        n_in_batch = 0
        added = 0
        skipped = 0
        batch = []
        started = monotonic()
        safe = set()
        batched = set()
        existing = {prod.name: prod for prod in self.get_all()}
        for doc in product_docs:
            if metadata_types is not None:
                if doc["metadata_type"] not in metadata_types:
                    skipped += 1
                    continue
            try:
                prod = self.from_doc(doc, metadata_type_cache=metadata_types)
                if prod.name in existing:
                    check_doc_unchanged(
                        prod.definition, jsonify_document(doc), f"Product {prod.name}"
                    )
                    _LOG.warning("%s: skipped (already loaded)", prod.name)
                    skipped += 1
                    safe.add(prod.name)
                else:
                    batch.append(prod)
                    n_in_batch += 1
                    batched.add(prod.name)
            except UnknownMetadataType:
                skipped += 1
            except InvalidDocException as e:
                _LOG.warning("%s: Skipped", str(e))
                skipped += 1
            if n_in_batch >= batch_size:
                batch_results = self._add_batch(batch)
                added += batch_results.completed
                skipped += batch_results.skipped
                if batch_results.safe is not None:
                    safe.update(batch_results.safe)
                else:
                    safe.update(batched)
                batched = set()
                batch = []
                n_in_batch = 0
        if n_in_batch > 0:
            batch_results = self._add_batch(batch)
            added += batch_results.completed
            skipped += batch_results.skipped
            if batch_results.safe is not None:
                safe.update(batch_results.safe)
            else:
                safe.update(batched)

        return BatchStatus(added, skipped, monotonic() - started, safe)

    @abstractmethod
    def can_update(
        self,
        product: Product,
        allow_unsafe_updates: bool = False,
        allow_table_lock: bool = False,
    ) -> tuple[bool, Iterable[Change], Iterable[Change]]:
        """
        Check if product can be updated. Return bool,safe_changes,unsafe_changes

        (An unsafe change is anything that may potentially make the product
        incompatible with existing datasets of that type)

        :param product: product to update
        :param allow_unsafe_updates: Allow unsafe changes. Use with caution.
        :param allow_table_lock:
            Allow an exclusive lock to be taken on the table while creating the indexes.
            This will halt other user's requests until completed.

            If false, creation will be slower and cannot be done in a transaction.
        :return: Tuple of: boolean (can/can't update); safe changes; unsafe changes
        """

    @abstractmethod
    def update(
        self,
        product: Product,
        allow_unsafe_updates: bool = False,
        allow_table_lock: bool = False,
    ) -> Product | None:
        """
        Persist updates to a product. Unsafe changes will throw a ValueError by default.

        (An unsafe change is anything that may potentially make the product
        incompatible with existing datasets of that type)

        :param product: Product model with unpersisted updates
        :param allow_unsafe_updates: Allow unsafe changes. Use with caution.
        :param allow_table_lock:
            Allow an exclusive lock to be taken on the table while creating the indexes.
            This will halt other user's requests until completed.

            If false, creation will be slower and cannot be done in a transaction.
        :return: Persisted updated Product model
        """

    def update_document(
        self,
        definition: JsonDict,
        allow_unsafe_updates: bool = False,
        allow_table_lock: bool = False,
    ) -> Product | None:
        """
        Update a metadata type from a document. Unsafe changes will throw a ValueError by default.

        Safe updates currently allow new search fields to be added, description to be changed.

        :param definition: Updated definition
        :param allow_unsafe_updates: Allow unsafe changes. Use with caution.
        :param allow_table_lock:
            Allow an exclusive lock to be taken on the table while creating the indexes.
            This will halt other user's requests until completed.

            If false, creation will be slower and cannot be done in a transaction.
        :return: Persisted updated Product model
        """
        return self.update(
            self.from_doc(definition),
            allow_unsafe_updates=allow_unsafe_updates,
            allow_table_lock=allow_table_lock,
        )

    def add_document(self, definition: JsonDict) -> Product | None:
        """
        Add a Product using its definition

        :param definition: product definition document
        :return: Persisted Product model
        """
        type_ = self.from_doc(definition)
        return self.add(type_)

    @abstractmethod
    def delete(
        self, products: Iterable[Product], allow_delete_active: bool = False
    ) -> Sequence[Product]:
        """
        Delete the specified products.

        :param products: Products to be deleted
        :param allow_delete_active:
            Whether to allow the deletion of a Product with active datasets
            (and thereby said active datasets). Use with caution.

            If false (default), will error if a Product has active datasets.
        :return: list of deleted Products
        """

    def get(self, id_: int) -> Product | None:
        """
        Fetch product by id.

        :param id_: Id of desired product
        :return: Product model or None if not found
        """
        try:
            return self.get_unsafe(id_)
        except KeyError:
            return None

    def get_by_name(self, name: str) -> Product | None:
        """
        Fetch product by name.

        :param name: Name of desired product
        :return: Product model or None if not found
        """
        try:
            return self.get_by_name_unsafe(name)
        except KeyError:
            return None

    @abstractmethod
    def get_unsafe(self, id_: int) -> Product:
        """
        Fetch product by id

        :param id_: id of desired product
        :return: product model
        :raises KeyError: if not found
        """

    @abstractmethod
    def get_by_name_unsafe(self, name: str) -> Product:
        """
        Fetch product by name

        :param name: name of desired product
        :return: product model
        :raises KeyError: if not found
        """

    def get_with_fields(self, field_names: Iterable[str]) -> Iterable[Product]:
        """
        Return products that have all of the given fields.

        :param field_names: names of fields that returned products must have
        :returns: Matching product models
        """
        return self.get_with_types(
            self._index.metadata_types.get_with_fields(field_names)
        )

    def get_with_types(self, types: Iterable[MetadataType]) -> Iterable[Product]:
        """
        Return all products for given metadata types

        :param types: An iterable of MetadataType models
        :return: An iterable of Product models
        """
        mdts = {mdt.name for mdt in types}
        for prod in self.get_all():
            if prod.metadata_type.name in mdts:
                yield prod

    def get_field_names(self, product: str | Product | None = None) -> Iterable[str]:
        """
        Get the list of possible search fields for a Product (or all products)

        :param product: Name of product, a Product object, or None for all products
        :return: All possible search field names
        """
        if product is None:
            prods = self.get_all()
        else:
            if isinstance(product, str):
                product = self.get_by_name(product)
            if product is None:
                prods = []
            else:
                prods = [product]
        out: set[str] = set()
        for prod in prods:
            out.update(prod.metadata_type.dataset_fields)
        return out

    def search(self, **query: QueryField) -> Generator[Product]:
        """
        Return products that match the supplied query

        :param query: Query parameters
        :return: Generator of product models
        """
        for type_, q in self.search_robust(**query):
            if not q:
                yield type_

    @abstractmethod
    def search_robust(self, **query: QueryField) -> Iterable[tuple[Product, QueryDict]]:
        """
        Return dataset types that match match-able fields and dict of remaining un-matchable fields.

        :param query: Query parameters
        :return: Tuples of product model and a dict of remaining unmatchable fields
        """

    @abstractmethod
    def search_by_metadata(self, metadata: JsonDict) -> Iterable[Product]:
        """
        Perform a search using arbitrary metadata, returning results as Product objects.

        Caution - slow! This will usually not use indexes.

        :param metadata: metadata dictionary representing arbitrary search query
        :return: Matching product models
        """

    @abstractmethod
    def get_all(self) -> Iterable[Product]:
        """
        Retrieve all Products

        :returns: Product models for all known products
        """

    def get_all_docs(self) -> Iterable[JsonDict]:
        """
        Retrieve all Product metadata documents
        Default implementation calls get_all()

        API Note: This API method is not finalised and may be subject to change.

        :returns: Iterable of metadata documents for all known products
        """
        for prod in self.get_all():
            yield cast(JsonDict, prod.definition)

    @abstractmethod
    def spatial_extent(
        self, product: str | Product, crs: CRS = CRS("EPSG:4326")
    ) -> Geometry | None:
        """
        Return the combined spatial extent of the nominated product

        Uses spatial index.

        Returns None if no index for the CRS, or if no datasets for the product in the relevant spatial index,
        or if the driver does not support the spatial index api.

        Result will not include extents of datasets that cannot be validly projected into the CRS.

        :param product: A Product or product name. (or None)
        :param crs: A CRS (defaults to EPSG:4326)
        :return: The combined spatial extents of the product.
        """

    @abstractmethod
    def temporal_extent(
        self, product: str | Product
    ) -> tuple[datetime.datetime, datetime.datetime]:
        """
        Returns the minimum and maximum acquisition time of a product.
        Raises KeyError if product is not found, RuntimeError if product has no datasets in the index

        :param product: Product or name of product
        :return: minimum and maximum acquisition times
        """

    @abstractmethod
    def most_recent_change(self, product: str | Product) -> datetime.datetime | None:
        """
        Finds the time of the latest change to a dataset belonging to the product.
        Raises KeyError if product is not in the index
        Returns None if product has no datasets in the index

        :param product: Product or name of product
        :return: datetime of most recent dataset change
        """

# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2024 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
import datetime
import logging

from pathlib import Path
from threading import Lock
from time import monotonic

from abc import ABC, abstractmethod
from typing import (Any, Iterable, Iterator,
                    List, Mapping, MutableMapping,
                    NamedTuple, Optional,
                    Tuple, Union, Sequence, Type)
from urllib.parse import urlparse, ParseResult
from uuid import UUID
from datetime import timedelta
from deprecat import deprecat

from datacube.cfg.api import ODCEnvironment, ODCOptionHandler
from datacube.index.exceptions import TransactionException
from datacube.index.fields import Field
from datacube.model import Product, Dataset, MetadataType, Range
from datacube.model import LineageTree, LineageDirection, LineageRelation
from datacube.model.lineage import LineageRelations
from datacube.utils import cached_property, jsonify_document, read_documents, InvalidDocException, report_to_user
from datacube.utils.changes import AllowPolicy, Change, Offset, DocumentMismatchError, check_doc_unchanged
from datacube.utils.generic import thread_local_cache
from datacube.migration import ODC2DeprecationWarning
from odc.geo import CRS, Geometry
from odc.geo.geom import box
from datacube.utils.documents import UnknownMetadataType

_LOG = logging.getLogger(__name__)


class BatchStatus(NamedTuple):
    """
    A named tuple representing the results of a batch add operation:
    - completed: Number of objects added to theMay be None for internal functions and for datasets.
    - skipped: Number of objects skipped, either because they already exist
      or the documents are invalid for this driver.
    - seconds_elapsed: seconds elapsed during the bulk add operation;
    - safe: an optional list of names of bulk added objects that are safe to be
      used for lower level bulk adds. Includes objects added, and objects skipped
      because they already exist in the index and are identical to the version
      being added.  May be None for internal functions and for datasets.
    """
    completed: int
    skipped: int
    seconds_elapsed: float
    safe: Optional[Iterable[str]] = None


class AbstractUserResource(ABC):
    """
    Abstract base class for the User portion of an index api.

    All UserResource implementations should inherit from this base
    class and implement all abstract methods.

    (If a particular abstract method is not applicable for a particular implementation
    raise a NotImplementedError)
    """

    @abstractmethod
    def grant_role(self, role: str, *usernames: str) -> None:
        """
        Grant a role to users
        :param role: name of the database role
        :param usernames: usernames to grant the role to.
        """

    @abstractmethod
    def create_user(self,
                    username: str,
                    password: str,
                    role: str,
                    description: Optional[str] = None) -> None:
        """
        Create a new user
        :param username: username of the new user
        :param password: password of the new user
        :param role: default role of the the new user
        :param description: optional description for the new user
        """

    @abstractmethod
    def delete_user(self,
                    *usernames: str
                   ) -> None:
        """
        Delete database users
        :param usernames: usernames of users to be deleted
        """

    @abstractmethod
    def list_users(self) -> Iterable[Tuple[str, str, Optional[str]]]:
        """
        List all database users
        :return: Iterable of (role, username, description) tuples
        """


_DEFAULT_METADATA_TYPES_PATH = Path(__file__).parent.joinpath('default-metadata-types.yaml')


def default_metadata_type_docs(path=_DEFAULT_METADATA_TYPES_PATH) -> List[MetadataType]:
    """A list of the bare dictionary format of default :class:`datacube.model.MetadataType`"""
    return [doc for (path, doc) in read_documents(path)]


class AbstractMetadataTypeResource(ABC):
    """
    Abstract base class for the MetadataType portion of an index api.

    All MetadataTypeResource implementations should inherit from this base
    class and implement all abstract methods.

    (If a particular abstract method is not applicable for a particular implementation
    raise a NotImplementedError)
    """

    @abstractmethod
    def from_doc(self, definition: Mapping[str, Any]) -> MetadataType:
        """
        Construct a MetadataType object from a dictionary definition

        :param definition: A metadata definition dictionary
        :return: An unpersisted MetadataType object
        """

    @abstractmethod
    def add(self,
            metadata_type: MetadataType,
            allow_table_lock: bool = False
           ) -> MetadataType:
        """
        Add a metadata type to the index.

        :param metadata_type: Unpersisted Metadatatype model
        :param allow_table_lock:
            Allow an exclusive lock to be taken on the table while creating the indexes.
            This will halt other user's requests until completed.

            If false, creation will be slightly slower and cannot be done in a transaction.

            raise NotImplementedError if set to True, and this behaviour is not applicable
            for the implementing driver.
        :return: Persisted Metadatatype model.
        """

    def _add_batch(self, batch_types: Iterable[MetadataType]) -> BatchStatus:
        """
        Add a single "batch" of mdts.

        Default implementation is simple loop of add

        API Note: This API method is not finalised and may be subject to change.

        :param batch_types: An iterable of one batch's worth of MetadataType objects to add
        :return: BatchStatus named tuple.
        """
        b_skipped = 0
        b_added = 0
        b_started = monotonic()
        b_loaded = set()
        for mdt in batch_types:
            try:
                self.add(mdt)
                b_added += 1
                b_loaded.add(mdt.name)
            except DocumentMismatchError as e:
                _LOG.warning("%s: Skipping", str(e))
                b_skipped += 1
            except Exception as e:
                _LOG.warning("%s: Skipping", str(e))
                b_skipped += 1
        return BatchStatus(b_added, b_skipped, monotonic() - b_started, b_loaded)

    def bulk_add(self,
                 metadata_docs: Iterable[Mapping[str, Any]],
                 batch_size: int = 1000) -> BatchStatus:
        """
        Add a group of Metadata Type documents in bulk.

        API Note: This API method is not finalised and may be subject to change.

        :param metadata_docs: An iterable of metadata type metadata docs.
        :param batch_size: Number of metadata types to add per batch (default 1000)
        :return: BatchStatus named tuple, with `safe` containing a list of
                 metadata type names that are safe to include in a subsequent product bulk add.
        """
        n_in_batch = 0
        added = 0
        skipped = 0
        started = monotonic()
        batch = []
        existing = {mdt.name: mdt for mdt in self.get_all()}
        batched = set()
        safe = set()
        for doc in metadata_docs:
            try:
                mdt = self.from_doc(doc)
                if mdt.name in existing:
                    check_doc_unchanged(
                        existing[mdt.name].definition,
                        jsonify_document(mdt.definition),
                        'Metadata Type {}'.format(mdt.name)
                    )
                    _LOG.warning("%s: Skipped - already exists", mdt.name)
                    skipped += 1
                    safe.add(mdt.name)
                else:
                    batch.append(mdt)
                    batched.add(mdt.name)
                    n_in_batch += 1
            except DocumentMismatchError as e:
                _LOG.warning("%s: Skipped", str(e))
                skipped += 1
            except InvalidDocException as e:
                _LOG.warning("%s: Skipped", str(e))
                skipped += 1
            if n_in_batch >= batch_size:
                batch_results = self._add_batch(batch)
                batch = []
                added += batch_results.completed
                skipped += batch_results.skipped
                if batch_results.safe is None:
                    safe.update(batched)
                else:
                    safe.update(batch_results.safe)
                batched = set()
                n_in_batch = 0
        if n_in_batch > 0:
            batch_results = self._add_batch(batch)
            added += batch_results.completed
            skipped += batch_results.skipped
            if batch_results.safe is None:
                safe.update(batched)
            else:
                safe.update(batch_results.safe)
        return BatchStatus(added, skipped, monotonic() - started, safe)

    @abstractmethod
    def can_update(self,
                   metadata_type: MetadataType,
                   allow_unsafe_updates: bool = False
                  ) -> Tuple[bool, Iterable[Change], Iterable[Change]]:
        """
        Check if metadata type can be updated. Return bool,safe_changes,unsafe_changes

        Safe updates currently allow new search fields to be added, description to be changed.

        :param metadata_type: updated MetadataType
        :param allow_unsafe_updates: Allow unsafe changes. Use with caution.
        :return: Tuple of: boolean (can/can't update); safe changes; unsafe changes
        """

    @abstractmethod
    def update(self,
               metadata_type: MetadataType,
               allow_unsafe_updates: bool = False,
               allow_table_lock: bool = False
              ) -> MetadataType:
        """
        Update a metadata type from the document. Unsafe changes will throw a ValueError by default.

        Safe updates currently allow new search fields to be added, description to be changed.

        :param metadata_type: MetadataType model with unpersisted updates
        :param allow_unsafe_updates: Allow unsafe changes. Use with caution.
        :param allow_table_lock:
            Allow an exclusive lock to be taken on the table while creating the indexes.
            This will halt other user's requests until completed.

            If false, creation will be slower and cannot be done in a transaction.
        :return: Persisted updated MetadataType model
        """

    def update_document(self,
                        definition: Mapping[str, Any],
                        allow_unsafe_updates: bool = False,
                       ) -> MetadataType:
        """
        Update a metadata type from the document. Unsafe changes will throw a ValueError by default.

        Safe updates currently allow new search fields to be added, description to be changed.

        :param definition: Updated definition
        :param allow_unsafe_updates: Allow unsafe changes. Use with caution.
        :return: Persisted updated MetadataType model
        """
        return self.update(self.from_doc(definition), allow_unsafe_updates=allow_unsafe_updates)

    def get_with_fields(self, field_names: Iterable[str]) -> Iterable[MetadataType]:
        """
        Return all metadata types that have all of the named search fields.

        :param field_names: Iterable of search field names
        :return: Iterable of matching metadata types.
        """
        for mdt in self.get_all():
            if all(field in mdt.dataset_fields for field in field_names):
                yield mdt

    def get(self, id_: int) -> Optional[MetadataType]:
        """
        Fetch metadata type by id.

        :return: MetadataType model or None if not found
        """
        try:
            return self.get_unsafe(id_)
        except KeyError:
            return None

    def get_by_name(self, name: str) -> Optional[MetadataType]:
        """
        Fetch metadata type by name.

        :return: MetadataType model or None if not found
        """
        try:
            return self.get_by_name_unsafe(name)
        except KeyError:
            return None

    @abstractmethod
    def get_unsafe(self, id_: int) -> MetadataType:
        """
        Fetch metadata type by id

        :param id_:
        :return: metadata type model
        :raises KeyError: if not found
        """

    @abstractmethod
    def get_by_name_unsafe(self, name: str) -> MetadataType:
        """
        Fetch metadata type by name

        :param name:
        :return: metadata type model
        :raises KeyError: if not found
        """

    @abstractmethod
    def check_field_indexes(self,
                            allow_table_lock: bool = False,
                            rebuild_views: bool = False,
                            rebuild_indexes: bool = False
                           ) -> None:
        """
        Create or replace per-field indexes and views.

        May have no effect if not relevant for this index implementation

        :param allow_table_lock:
            Allow an exclusive lock to be taken on the table while creating the indexes.
            This will halt other user's requests until completed.

            If false, creation will be slightly slower and cannot be done in a transaction.
        :param: rebuild_views: whether or not views should be rebuilt
        :param: rebuild_indexes: whether or not views should be rebuilt
        """

    @abstractmethod
    def get_all(self) -> Iterable[MetadataType]:
        """
        Retrieve all Metadata Types

        :returns: All available MetadataType models
        """

    def get_all_docs(self) -> Iterable[Mapping[str, Any]]:
        """
        Retrieve all Metadata Types as documents only (e.g. for an index clone)

        Default implementation calls self.get_all()

        API Note: This API method is not finalised and may be subject to change.

        :returns: All available MetadataType definition documents
        """
        # Default implementation calls get_all()
        for mdt in self.get_all():
            yield mdt.definition


QueryField = Union[str, float, int, Range, datetime.datetime]
QueryDict = Mapping[str, QueryField]


class AbstractProductResource(ABC):
    """
    Abstract base class for the Product portion of an index api.

    All ProductResource implementations should inherit from this base
    class and implement all abstract methods.

    (If a particular abstract method is not applicable for a particular implementation
    raise a NotImplementedError)
    """
    def __init__(self, index: "AbstractIndex"):
        self._index = index

    def from_doc(self, definition: Mapping[str, Any],
                 metadata_type_cache: Optional[MutableMapping[str, MetadataType]] = None) -> Product:
        """
        Construct unpersisted Product model from product metadata dictionary

        :param definition: a Product metadata dictionary
        :param metadata_type_cache: a dict cache of MetaDataTypes to use in constructing a Product.
                                    MetaDataTypes may come from a different index.
        :return: Unpersisted product model
        """
        # This column duplication is getting out of hand:
        Product.validate(definition)   # type: ignore[attr-defined]   # validate method added by decorator
        # Validate extra dimension metadata
        Product.validate_extra_dims(definition)

        metadata_type = definition['metadata_type']

        # They either specified the name of a metadata type, or specified a metadata type.
        # Is it a name?
        if isinstance(metadata_type, str):
            if metadata_type_cache is not None and metadata_type in metadata_type_cache:
                metadata_type = metadata_type_cache[metadata_type]
            else:
                metadata_type = self._index.metadata_types.get_by_name(metadata_type)
                if (metadata_type is not None
                        and metadata_type_cache is not None
                        and metadata_type.name not in metadata_type_cache):
                    metadata_type_cache[metadata_type.name] = metadata_type
        else:
            # Otherwise they embedded a document, add it if needed:
            metadata_type = self._index.metadata_types.from_doc(metadata_type)
            definition = dict(definition)
            definition['metadata_type'] = metadata_type.name

        if not metadata_type:
            raise UnknownMetadataType('Unknown metadata type: %r' % definition['metadata_type'])

        return Product(metadata_type, definition)

    @abstractmethod
    def add(self,
            product: Product,
            allow_table_lock: bool = False
           ) -> Product:
        """
        Add a product to the index.

        :param metadata_type: Unpersisted Product model
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

        :param batch_types: An iterable of one batch's worth of Product objects to add
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
        return BatchStatus(b_added, b_skipped, monotonic()-b_started)

    def bulk_add(self,
                 product_docs: Iterable[Mapping[str, Any]],
                 metadata_types: Optional[Mapping[str, MetadataType]] = None,
                 batch_size: int = 1000) -> BatchStatus:
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
                    check_doc_unchanged(prod.definition, jsonify_document(doc), f"Product {prod.name}")
                    _LOG.warning("%s: skipped (already loaded)", prod.name)
                    skipped += 1
                    safe.add(prod.name)
                else:
                    batch.append(prod)
                    n_in_batch += 1
                    batched.add(prod.name)
            except UnknownMetadataType as e:
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
    def can_update(self,
                   product: Product,
                   allow_unsafe_updates: bool = False,
                   allow_table_lock: bool = False
                  ) -> Tuple[bool, Iterable[Change], Iterable[Change]]:
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
    def update(self,
               metadata_type: Product,
               allow_unsafe_updates: bool = False,
               allow_table_lock: bool = False
               ) -> Product:
        """
        Persist updates to a product. Unsafe changes will throw a ValueError by default.

        (An unsafe change is anything that may potentially make the product
        incompatible with existing datasets of that type)

        :param metadata_type: Product model with unpersisted updates
        :param allow_unsafe_updates: Allow unsafe changes. Use with caution.
        :param allow_table_lock:
            Allow an exclusive lock to be taken on the table while creating the indexes.
            This will halt other user's requests until completed.

            If false, creation will be slower and cannot be done in a transaction.
        :return: Persisted updated Product model
        """

    def update_document(self,
                        definition: Mapping[str, Any],
                        allow_unsafe_updates: bool = False,
                        allow_table_lock: bool = False
                       ) -> Product:
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
        return self.update(self.from_doc(definition),
                           allow_unsafe_updates=allow_unsafe_updates,
                           allow_table_lock=allow_table_lock
                          )

    def add_document(self, definition: Mapping[str, Any]) -> Product:
        """
        Add a Product using its definition

        :param dict definition: product definition document
        :return: Persisted Product model
        """
        type_ = self.from_doc(definition)
        return self.add(type_)

    def get(self, id_: int) -> Optional[Product]:
        """
        Fetch product by id.

        :param id_: Id of desired product
        :return: Product model or None if not found
        """
        try:
            return self.get_unsafe(id_)
        except KeyError:
            return None

    def get_by_name(self, name: str) -> Optional[Product]:
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
        return self.get_with_types(self._index.metadata_types.get_with_fields(field_names))

    def get_with_types(self, types: Iterable[MetadataType]) -> Iterable[Product]:
        """
        Return all products for given metadata types

        :param types: An interable of MetadataType models
        :return: An iterable of Product models
        """
        mdts = set(mdt.name for mdt in types)
        for prod in self.get_all():
            if prod.metadata_type.name in mdts:
                yield prod

    def get_field_names(self, product: Optional[str | Product] = None) -> Iterable[str]:
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
        out = set()
        for prod in prods:
            out.update(prod.metadata_type.dataset_fields)
        return out

    def search(self, **query: QueryField) -> Iterator[Product]:
        """
        Return products that match the supplied query

        :param query: Query parameters
        :return: Generator of product models
        """
        for type_, q in self.search_robust(**query):
            if not q:
                yield type_

    @abstractmethod
    def search_robust(self,
                      **query: QueryField
                     ) -> Iterable[Tuple[Product, Mapping[str, QueryField]]]:
        """
        Return dataset types that match match-able fields and dict of remaining un-matchable fields.

        :param query: Query parameters
        :return: Tuples of product model and a dict of remaining unmatchable fields
        """

    @abstractmethod
    def search_by_metadata(self,
                           metadata: Mapping[str, QueryField]
                           ) -> Iterable[Dataset]:
        """
        Perform a search using arbitrary metadata, returning results as Product objects.

        Caution – slow! This will usually not use indexes.

        :param metadata: metadata dictionary representing arbitrary search query
        :return: Matching product models
        """

    @abstractmethod
    def get_all(self) -> Iterable[Product]:
        """
        Retrieve all Products

        :returns: Product models for all known products
        """

    def get_all_docs(self) -> Iterable[Mapping[str, Any]]:
        """
        Retrieve all Product metadata documents
        Default implementation calls get_all()

        API Note: This API method is not finalised and may be subject to change.

        :returns: Iterable of metadata documents for all known products
        """
        for prod in self.get_all():
            yield prod.definition

    @abstractmethod
    def spatial_extent(self, product: str | Product, crs: CRS = CRS("EPSG:4326")) -> Optional[Geometry]:
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
    def temporal_extent(self, product: str | Product) -> tuple[datetime.datetime, datetime.datetime]:
        """
        Returns the minimum and maximum acquisition time of a product.
        Raises KeyError if product has no datasets in the index

        :param product: Product or name of product
        :return: minimum and maximum acquisition times
        """


# Non-strict Dataset ID representation
DSID = Union[str, UUID]


def dsid_to_uuid(dsid: DSID) -> UUID:
    """
    Convert non-strict dataset ID representation to strict UUID
    """
    if isinstance(dsid, UUID):
        return dsid
    else:
        return UUID(dsid)


class AbstractLineageResource(ABC):
    """
    Abstract base class for the Lineage portion of an index api.

    All LineageResource implementations should inherit from this base class.

    Note that this is a "new" resource only supported by new index drivers with `supports_external_lineage`
    set to True.  If a driver does NOT support external lineage, it can use extend the NoLineageResource class below,
    which is a minimal implementation of this resource that raises a NotImplementedError for all methods.

    However, any index driver that supports lineage must implement at least the get_all_lineage() and _add_batch()
    methods.
    """
    def __init__(self, index) -> None:
        self._index = index
        # THis is explicitly for indexes that do not support the External Lineage API.
        assert self._index.supports_external_lineage

    @abstractmethod
    def get_derived_tree(self, id_: DSID, max_depth: int = 0) -> LineageTree:
        """
        Extract a LineageTree from the index, with:
            - "id" at the root of the tree.
            - "derived" direction (i.e. datasets derived from id, datasets derived from
              datasets derived from id, etc.)
            - maximum depth as requested (default 0 = unlimited depth)

        Tree may be empty (i.e. just the root node) if no lineage for id is stored.

        :param id: the id of the dataset at the root of the returned tree
        :param max_depth: Maximum recursion depth.  Default/Zero = unlimited depth
        :return: A derived-direction Lineage tree with id at the root.
        """

    @abstractmethod
    def get_source_tree(self, id_: DSID, max_depth: int = 0) -> LineageTree:
        """
        Extract a LineageTree from the index, with:
            - "id" at the root of the tree.
            - "source" direction (i.e. datasets id was derived from, the dataset ids THEY were derived from, etc.)
            - maximum depth as requested (default 0 = unlimited depth)

        Tree may be empty (i.e. just the root node) if no lineage for id is stored.

        :param id: the id of the dataset at the root of the returned tree
        :param max_depth: Maximum recursion depth.  Default/Zero = unlimited depth
        :return: A source-direction Lineage tree with id at the root.
        """

    @abstractmethod
    def merge(self, rels: LineageRelations, allow_updates: bool = False, validate_only: bool = False) -> None:
        """
        Merge an entire LineageRelations collection into the databse.

        :param rels: The LineageRelations collection to merge.
        :param allow_updates: If False and the merging rels would require index updates,
                              then raise an InconsistentLineageException.
        :param validate_only: If True, do not actually merge the LineageRelations, just check for inconsistency.
                              allow_updates and validate_only cannot both be True
        """

    @abstractmethod
    def add(self, tree: LineageTree, max_depth: int = 0, allow_updates: bool = False) -> None:
        """
        Add or update a LineageTree into the Index.

        If the provided tree is inconsistent with lineage data already
        recorded in the database, by default a ValueError is raised,
        If replace is True, the provided tree is treated as authoritative
        and the database is updated to match.

        :param tree: The LineageTree to add to the index
        :param max_depth: Maximum recursion depth. Default/Zero = unlimited depth
        :param allow_updates: If False and the tree would require index updates to fully
                              add, then raise an InconsistentLineageException.
        """

    @abstractmethod
    def remove(self, id_: DSID, direction: LineageDirection, max_depth: int = 0) -> None:
        """
        Remove lineage information from the Index.

        Removes lineage relation data only. Home values not affected.

        :param id_: The Dataset ID to start removing lineage from.
        :param direction: The direction in which to remove lineage (from id_)
        :param max_depth: The maximum depth to which to remove lineage (0/default = no limit)
        """

    @abstractmethod
    def set_home(self, home: str, *args: DSID, allow_updates: bool = False) -> int:
        """
        Set the home for one or more dataset ids.

        :param home: The home string
        :param args: One or more dataset ids
        :param allow_updates: Allow datasets with existing homes to be updated.
        :returns: The number of records affected.  Between zero and len(args).
        """

    @abstractmethod
    def clear_home(self, *args: DSID, home: Optional[str] = None) -> int:
        """
        Clear the home for one or more dataset ids, or all dataset ids that currently have
        a particular home value.

        :param args: One or more dataset ids
        :param home: The home string.  Supply home or args - not both.
        :returns: The number of home records deleted. Usually len(args).
        """

    @abstractmethod
    def get_homes(self, *args: DSID) -> Mapping[UUID, str]:
        """
        Obtain a dictionary mapping UUIDs to home strings for the passed in DSIDs.

        If a passed in DSID does not have a home set in the database, it will not
        be included in the returned mapping.  i.e. a database index with no homes
        recorded will always return an empty mapping.

        :param args: One or more dataset ids
        :return: Mapping of dataset ids to home strings.
        """

    @abstractmethod
    def get_all_lineage(self, batch_size: int = 1000) -> Iterable[LineageRelation]:
        """
        Perform a batch-read of all lineage relations (as used by index clone operation)
        and return as an iterable stream of LineageRelation objects.

        API Note: This API method is not finalised and may be subject to change.

        :param batch_size: The number of records to read from the database at a time.
        :return: An iterable stream of LineageRelation objects.
        """

    @abstractmethod
    def _add_batch(self, batch_rels: Iterable[LineageRelation]) -> BatchStatus:
        """
        Add a single "batch" of LineageRelation objects.

        No default implementation is provided

        API Note: This API method is not finalised and may be subject to change.

        :param batch_rels: An iterable of one batch's worth of LineageRelation objects to add
        :return: BatchStatus named tuple, with `safe` set to None.
        """

    def bulk_add(self, relations: Iterable[LineageRelation], batch_size: int = 1000) -> BatchStatus:
        """
        Add a group of LineageRelation objects in bulk.

        API Note: This API method is not finalised and may be subject to change.

        :param relations: An Iterable of LineageRelation objects (i.e. as returned by get_all_lineage)
        :param batch_size: Number of lineage relations to add per batch (default 1000)
        :return: BatchStatus named tuple, with `safe` set to None.
        """

        def increment_progress():
            report_to_user(".", progress_indicator=True)

        n_batches = 0
        n_in_batch = 0
        added = 0
        skipped = 0
        batch = []
        job_started = monotonic()
        for rel in relations:
            batch.append(rel)
            n_in_batch += 1
            if n_in_batch >= batch_size:
                batch_result = self._add_batch(batch)
                _LOG.info("Batch %d/%d datasets added in %.2fs: (%.2fdatasets/min)",
                          batch_result.completed,
                          n_in_batch,
                          batch_result.seconds_elapsed,
                          batch_result.completed * 60 / batch_result.seconds_elapsed)
                added += batch_result.completed
                skipped += batch_result.skipped
                batch = []
                n_in_batch = 0
                n_batches += 1
                increment_progress()
        if n_in_batch > 0:
            batch_result = self._add_batch(batch)
            added += batch_result.completed
            skipped += batch_result.skipped
            increment_progress()

        return BatchStatus(added, skipped, monotonic() - job_started)


class NoLineageResource(AbstractLineageResource):
    """
    Minimal implementation of AbstractLineageResource that raises "not implemented"
       for all methods.

    Index drivers that do not support lineage at all may use this implementation as is.

    Index drivers that support legacy lineage should extend this implementation and provide
    implementations of the get_all_lineage() and _add_batch() methods.
    """
    def __init__(self, index) -> None:
        self._index = index
        assert not self._index.supports_external_lineage

    def get_derived_tree(self, id: DSID, max_depth: int = 0) -> LineageTree:
        raise NotImplementedError()

    def get_source_tree(self, id: DSID, max_depth: int = 0) -> LineageTree:
        raise NotImplementedError()

    def add(self, tree: LineageTree, max_depth: int = 0, allow_updates: bool = False) -> None:
        raise NotImplementedError()

    def merge(self, rels: LineageRelations, allow_updates: bool = False, validate_only: bool = False) -> None:
        raise NotImplementedError()

    def remove(self, id_: DSID, direction: LineageDirection, max_depth: int = 0) -> None:
        raise NotImplementedError()

    def set_home(self, home: str, *args: DSID, allow_updates: bool = False) -> int:
        raise NotImplementedError()

    def clear_home(self, *args: DSID, home: Optional[str] = None) -> int:
        raise NotImplementedError()

    def get_homes(self, *args: DSID) -> Mapping[UUID, str]:
        return {}

    def get_all_lineage(self, batch_size: int = 1000) -> Iterable[LineageRelation]:
        raise NotImplementedError()

    def _add_batch(self, batch_rels: Iterable[LineageRelation]) -> BatchStatus:
        raise NotImplementedError()


class DatasetTuple(NamedTuple):
    """
    A named tuple representing a complete dataset:
    - product: A Product model.
    - metadata: The dataset metadata document
    - uris: A list of locations (uris)
    """
    product: Product
    metadata: Mapping[str, Any]
    uris: Sequence[str]


class AbstractDatasetResource(ABC):
    """
    Abstract base class for the Dataset portion of an index api.

    All DatasetResource implementations should inherit from this base
    class and implement all abstract methods.

    (If a particular abstract method is not applicable for a particular implementation
    raise a NotImplementedError)
    """

    def __init__(self, index):
        self._index = index
        self.products = self._index.products
        self.types = self.products  # types is compatibility alias for products

    @abstractmethod
    def get_unsafe(self,
                   id_: DSID,
                   include_sources: bool = False,
                   include_deriveds: bool = False,
                   max_depth: int = 0
                   ) -> Dataset:
        """
        Get dataset by id (Raises KeyError if id_ does not exist)

        - Index drivers supporting the legacy lineage API:

        :param id_: id of the dataset to retrieve
        :param include_sources: include the full provenance tree of the dataset.


        - Index drivers supporting the external lineage API:

        :param id_: id of the dataset to retrieve
        :param include_sources: include the full provenance tree for the dataset.
        :param include_deriveds: include the full derivative tree for the dataset.
        :param max_depth: The maximum depth of the source and/or derived tree.  Defaults to 0, meaning no limit.
        :rtype: Dataset model (None if not found)
        """

    def get(self,
            id_: DSID,
            include_sources: bool = False,
            include_deriveds: bool = False,
            max_depth: int = 0
            ) -> Optional[Dataset]:
        """
        Get dataset by id (Return None if id_ does not exist.

        - Index drivers supporting the legacy lineage API:

        :param id_: id of the dataset to retrieve
        :param include_sources: include the full provenance tree of the dataset.


        - Index drivers supporting the external lineage API:

        :param id_: id of the dataset to retrieve
        :param include_sources: include the full provenance tree for the dataset.
        :param include_deriveds: include the full derivative tree for the dataset.
        :param max_depth: The maximum depth of the source and/or derived tree.  Defaults to 0, meaning no limit.
        :rtype: Dataset model (None if not found)
        """
        try:
            return self.get_unsafe(id_, include_sources, include_deriveds, max_depth)
        except KeyError:
            return None

    def _check_get_legacy(self,
                          include_deriveds: bool = False,
                          max_depth: int = 0
                          ) -> None:
        """
        Index drivers implementing the legacy lineage API can call this method to check get arguments
        """
        if not self._index.supports_external_lineage:
            if include_deriveds:
                raise NotImplementedError(
                    "This index driver only supports the legacy lineage data - include_deriveds not supported."
                )
            if not self._index.supports_external_lineage and (include_deriveds or max_depth > 0):
                raise NotImplementedError(
                    "This index driver only supports the legacy lineage data - max_depth not supported."
                )

    @abstractmethod
    def bulk_get(self, ids: Iterable[DSID]) -> Iterable[Dataset]:
        """
        Get multiple datasets by id. (Lineage sources NOT included)

        :param ids: ids to retrieve
        :return: Iterable of Dataset models
        """

    @deprecat(
        reason="The 'get_derived' static method is deprecated in favour of the new lineage API.",
        version='1.9.0',
        category=ODC2DeprecationWarning)
    @abstractmethod
    def get_derived(self, id_: DSID) -> Iterable[Dataset]:
        """
        Get all datasets derived from a dataset (NOT recursive)

        :param id_: dataset id
        :rtype: list[Dataset]
        """

    @abstractmethod
    def has(self, id_: DSID) -> bool:
        """
        Is this dataset in this index?

        :param id_: dataset id
        :return: True if the dataset exists in this index
        """

    @abstractmethod
    def bulk_has(self, ids_: Iterable[DSID]) -> Iterable[bool]:
        """
        Like `has` but operates on a multiple ids.

        For every supplied id check if database contains a dataset with that id.

        :param ids_: iterable of dataset ids to check existence in index

        :return: Iterable of bools, true for datasets that exist in index
        """

    @abstractmethod
    def add(self, dataset: Dataset,
            with_lineage: bool = True,
            archive_less_mature: Optional[int] = None,
           ) -> Dataset:
        """
        Add ``dataset`` to the index. No-op if it is already present.

        :param dataset: Unpersisted dataset model

        :param with_lineage:
           - ``True (default)`` attempt adding lineage datasets if missing
           - ``False`` record lineage relations, but do not attempt
             adding lineage datasets to the db

        :param archive_less_mature: if integer, search for less
        mature versions of the dataset with the int value as a millisecond
        delta in timestamp comparison

        :return: Persisted Dataset model
        """

    @abstractmethod
    def search_product_duplicates(self,
                                  product: Product,
                                  *args: Union[str, Field]
                                 ) -> Iterable[Tuple[Tuple, Iterable[UUID]]]:
        """
        Find dataset ids who have duplicates of the given set of field names.

        (Search is always restricted by Product)

        Returns a generator returning a tuple containing a namedtuple of
        the values of the supplied fields, and the datasets that match those
        values.

        :param product: The Product to restrict search to
        :param args: field names to identify duplicates over
        """

    @abstractmethod
    def can_update(self,
                   dataset: Dataset,
                   updates_allowed: Optional[Mapping[Offset, AllowPolicy]] = None
                  ) -> Tuple[bool, Iterable[Change], Iterable[Change]]:
        """
        Check if dataset can be updated. Return bool,safe_changes,unsafe_changes

        :param Dataset dataset: Dataset to update
        :param updates_allowed: Allowed updates
        :return: Tuple of: boolean (can/can't update); safe changes; unsafe changes
        """

    @abstractmethod
    def update(self,
               dataset: Dataset,
               updates_allowed: Optional[Mapping[Offset, AllowPolicy]] = None,
               archive_less_mature: Optional[int] = None,
              ) -> Dataset:
        """
        Update dataset metadata and location
        :param Dataset dataset: Dataset model with unpersisted updates
        :param updates_allowed: Allowed updates
        :param archive_less_mature: Find and archive less mature datasets with ms delta
        :return: Persisted dataset model
        """

    @abstractmethod
    def archive(self, ids: Iterable[DSID]) -> None:
        """
        Mark datasets as archived

        :param Iterable[Union[str,UUID]] ids: list of dataset ids to archive
        """

    def archive_less_mature(self, ds: Dataset, delta: Union[int, bool] = 500) -> None:
        """
        Archive less mature versions of a dataset

        :param Dataset ds: dataset to search
        :param Union[int,bool] delta: millisecond delta for time range.
        If True, default to 500ms. If False, do not find or archive less mature datasets.
        Bool value accepted only for improving backwards compatibility, int preferred.
        """
        less_mature = self.find_less_mature(ds, delta)
        less_mature_ids = map(lambda x: x.id, less_mature)

        self.archive(less_mature_ids)
        for lm_ds in less_mature_ids:
            _LOG.info(f"Archived less mature dataset: {lm_ds}")

    def find_less_mature(self, ds: Dataset, delta: Union[int, bool] = 500) -> Iterable[Dataset]:
        """
        Find less mature versions of a dataset

        :param Dataset ds: Dataset to search
        :param Union[int,bool] delta: millisecond delta for time range.
        If True, default to 500ms. If None or False, do not find or archive less mature datasets.
        Bool value accepted only for improving backwards compatibility, int preferred.
        :return: Iterable of less mature datasets
        """
        if isinstance(delta, bool):
            _LOG.warning("received delta as a boolean value. Int is prefered")
            if delta is True:  # treat True as default
                delta = 500
            else:  # treat False the same as None
                return []
        elif isinstance(delta, int):
            if delta < 0:
                raise ValueError("timedelta must be a positive integer")
        elif delta is None:
            return []
        else:
            raise TypeError("timedelta must be None, a positive integer, or a boolean")

        def check_maturity_information(dataset, props):
            # check that the dataset metadata includes all maturity-related properties
            # passing in the required props to enable greater extensibility should it be needed
            for prop in props:
                if hasattr(dataset.metadata, prop) and (getattr(dataset.metadata, prop) is not None):
                    return
                raise ValueError(
                    f"Dataset {dataset.id} is missing property {prop} required for maturity check"
                )

        check_maturity_information(ds, ["region_code", "time", "dataset_maturity"])

        # 'expand' the date range by `delta` milliseconds to give a bit more leniency in datetime comparison
        expanded_time_range = Range(ds.metadata.time.begin - timedelta(milliseconds=delta),
                                    ds.metadata.time.end + timedelta(milliseconds=delta))
        dupes = self.search(product=ds.product.name,
                            region_code=ds.metadata.region_code,
                            time=expanded_time_range)

        less_mature = []
        for dupe in dupes:
            if dupe.id == ds.id:
                continue

            # only need to check that dupe has dataset maturity, missing/null region_code and time
            # would already have been filtered out during the search query
            check_maturity_information(dupe, ["dataset_maturity"])

            if dupe.metadata.dataset_maturity == ds.metadata.dataset_maturity:
                # Duplicate has the same maturity, which one should be archived is unclear
                raise ValueError(
                    f"A dataset with the same maturity as dataset {ds.id} already exists, "
                    f"with id: {dupe.id}"
                )

            if dupe.metadata.dataset_maturity < ds.metadata.dataset_maturity:
                # Duplicate is more mature than dataset
                # Note that "final" < "nrt"
                raise ValueError(
                    f"A more mature version of dataset {ds.id} already exists, with id: "
                    f"{dupe.id} and maturity: {dupe.metadata.dataset_maturity}"
                )

            less_mature.append(dupe)
        return less_mature

    @abstractmethod
    def restore(self, ids: Iterable[DSID]) -> None:
        """
        Mark datasets as not archived

        :param Iterable[Union[str,UUID]] ids: list of dataset ids to restore
        """

    @abstractmethod
    def purge(self, ids: Iterable[DSID]) -> None:
        """
        Delete archived datasets

        :param ids: iterable of dataset ids to purge
        """

    @abstractmethod
    def get_all_dataset_ids(self, archived: bool) -> Iterable[UUID]:
        """
        Get all dataset IDs based only on archived status

        This will be very slow and inefficient for large databases, and is really
        only intended for small and/or experimental databases.

        :param archived: If true, return all archived datasets, if false, all unarchived datatsets
        :return: Iterable of dataset ids
        """

    @deprecat(
        reason="This method has been moved to the Product resource (i.e. dc.index.products.get_field_names)",
        version="1.9.0",
        category=ODC2DeprecationWarning
    )
    def get_field_names(self, product_name: Optional[str] = None) -> Iterable[str]:
        """
        Get the list of possible search fields for a Product (or all products)

        :param product_name: Name of product, or None for all products
        :return: All possible search field names
        """
        return self._index.products.get_field_names(product_name)

    @abstractmethod
    def get_locations(self, id_: DSID) -> Iterable[str]:
        """
        Get (active) storage locations for the given dataset id

        :param id_: dataset id
        :return: Storage locations for the dataset
        """

    @abstractmethod
    def get_archived_locations(self, id_: DSID) -> Iterable[str]:
        """
        Get archived locations for a dataset

        :param id_: dataset id
        :return: Archived storage locations for the dataset
        """

    @abstractmethod
    def get_archived_location_times(self,
                                    id_: DSID
                                   ) -> Iterable[Tuple[str, datetime.datetime]]:
        """
        Get each archived location along with the time it was archived.

        :param id_: dataset id
        :return: Archived storage locations, with archive date.
        """

    @abstractmethod
    def add_location(self, id_: DSID, uri: str) -> bool:
        """
        Add a location to the dataset if it doesn't already exist.

        :param id_: dataset id
        :param uri: fully qualified uri
        :return: True if a location was added, false if location already existed
        """

    @abstractmethod
    def get_datasets_for_location(self,
                                  uri: str,
                                  mode: Optional[str] = None
                                 ) -> Iterable[Dataset]:
        """
        Find datasets that exist at the given URI

        :param uri: search uri
        :param mode: 'exact', 'prefix' or None (to guess)
        :return: Matching dataset models
        """

    @abstractmethod
    def remove_location(self,
                        id_: DSID,
                        uri: str
                       ) -> bool:
        """
        Remove a location from the dataset if it exists.

        :param id_: dataset id
        :param uri: fully qualified uri
        :return: True if location was removed, false if it didn't exist for the database
        """

    @abstractmethod
    def archive_location(self,
                         id_: DSID,
                         uri: str
                        ) -> bool:
        """
        Archive a location of the dataset if it exists and is active.

        :param id_: dataset id
        :param uri: fully qualified uri
        :return: True if location was able to be archived
        """

    @abstractmethod
    def restore_location(self,
                         id_: DSID,
                         uri: str
                        ) -> bool:
        """
        Un-archive a location of the dataset if it exists.

        :param id_: dataset id
        :param uri: fully qualified uri
        :return: True location was able to be restored
        """

    @abstractmethod
    def search_by_metadata(self,
                           metadata: Mapping[str, QueryField]
                          ) -> Iterable[Dataset]:
        """
        Perform a search using arbitrary metadata, returning results as Dataset objects.

        Caution – slow! This will usually not use indexes.

        :param metadata: metadata dictionary representing arbitrary search query
        :return: Matching dataset models
        """

    @abstractmethod
    def search(self,
               limit: Optional[int] = None,
               source_filter: Optional[Mapping[str, QueryField]] = None,
               **query: QueryField) -> Iterable[Dataset]:
        """
        Perform a search, returning results as Dataset objects.

        :param limit: Limit number of datasets per product (None/default = unlimited)
        :param query: search query parameters
        :return: Matching datasets
        """

    def get_all_docs_for_product(self, product: Product, batch_size: int = 1000) -> Iterable[DatasetTuple]:
        for ds in self.search(product=[product.name]):
            yield (product, ds.metadata_doc, ds.uris)

    def get_all_docs(self, products: Optional[Iterable[Product]] = None,
                     batch_size: int = 1000) -> Iterable[DatasetTuple]:
        """
        Return all datasets in bulk, filtering by product names only. Do not instantiate models.
        Archived datasets and locations are excluded.

        API Note: This API method is not finalised and may be subject to change.

        :param products: Iterable of products used to build the Dataset models.  May come from a different index.
                         Default/None: all products, Products read from the source index.
        :return: Iterable of DatasetTuple named tuples
        """
        # Default implementation calls search
        if products is None:
            products = list(self.products.get_all())
        for product in products:
            for dstup in self.get_all_docs_for_product(product, batch_size=batch_size):
                yield dstup

    def _add_batch(self, batch_ds: Iterable[DatasetTuple], cache: Mapping[str, Any]) -> BatchStatus:
        """
        Add a single "batch" of datasets, provided as DatasetTuples.

        Default implementation is simple loop of add

        API Note: This API method is not finalised and may be subject to change.

        :param batch_types: An iterable of one batch's worth of DatasetTuples to add
        :return: BatchStatus named tuple.
        """
        b_skipped = 0
        b_added = 0
        b_started = monotonic()
        for ds_tup in batch_ds:
            try:
                ds = Dataset(product=ds_tup.product,
                             metadata_doc=ds_tup.metadata,
                             uris=ds_tup.uris)
                self.add(ds, with_lineage=False)
                b_added += 1
            except DocumentMismatchError as e:
                _LOG.warning("%s: Skipping", str(e))
                b_skipped += 1
            except Exception as e:
                _LOG.warning("%s: Skipping", str(e))
                b_skipped += 1
        return BatchStatus(b_added, b_skipped, monotonic() - b_started)

    def _init_bulk_add_cache(self) -> Mapping[str, Any]:
        """
        Initialise a cache dictionary that may be used to share data between calls to _add_batch()

        API Note: This API method is not finalised and may be subject to change.

        :return: The initialised cache dictionary
        """
        return {}

    def bulk_add(self, datasets: Iterable[DatasetTuple], batch_size: int = 1000) -> BatchStatus:
        """
        Add a group of Dataset documents in bulk.

        API Note: This API method is not finalised and may be subject to change.

        :param datasets: An Iterable of DatasetTuples (i.e. as returned by get_all_docs)
        :param batch_size: Number of datasets to add per batch (default 1000)
        :return: BatchStatus named tuple, with `safe` set to None.
        """
        def increment_progress():
            report_to_user(".", progress_indicator=True)
        n_batches = 0
        n_in_batch = 0
        added = 0
        skipped = 0
        batch = []
        job_started = monotonic()
        inter_batch_cache = self._init_bulk_add_cache()
        for ds_tup in datasets:
            batch.append(ds_tup)
            n_in_batch += 1
            if n_in_batch >= batch_size:
                batch_result = self._add_batch(batch, inter_batch_cache)
                _LOG.info("Batch %d/%d datasets added in %.2fs: (%.2fdatasets/min)",
                          batch_result.completed,
                          n_in_batch,
                          batch_result.seconds_elapsed,
                          batch_result.completed * 60 / batch_result.seconds_elapsed)
                added += batch_result.completed
                skipped += batch_result.skipped
                batch = []
                n_in_batch = 0
                n_batches += 1
                increment_progress()
        if n_in_batch > 0:
            batch_result = self._add_batch(batch, inter_batch_cache)
            added += batch_result.completed
            skipped += batch_result.skipped
            increment_progress()

        return BatchStatus(added, skipped, monotonic() - job_started)

    @abstractmethod
    def search_by_product(self,
                          **query: QueryField
                         ) -> Iterable[Tuple[Iterable[Dataset], Product]]:
        """
        Perform a search, returning datasets grouped by product type.

        :param query: search query parameters
        :return: Matching datasets, grouped by Product
        """

    @abstractmethod
    def search_returning(self,
                         field_names: Iterable[str],
                         limit: Optional[int] = None,
                         **query: QueryField
                        ) -> Iterable[Tuple]:
        """
        Perform a search, returning only the specified fields.

        This method can be faster than normal search() if you don't need all fields of each dataset.

        It also allows for returning rows other than datasets, such as a row per uri when requesting field 'uri'.

        :param field_names: Names of desired fields
        :param limit: Limit number of dataset (None/default = unlimited)
        :param query: search query parameters
        :return: Namedtuple of requested fields, for each matching dataset.
        """

    @abstractmethod
    def count(self, **query: QueryField) -> int:
        """
        Perform a search, returning count of results.

        :param query: search query parameters
        :return: Count of matching datasets in index
        """

    @abstractmethod
    def count_by_product(self, **query: QueryField) -> Iterable[Tuple[Product, int]]:
        """
        Perform a search, returning a count of for each matching product type.

        :param query: search query parameters
        :return: Counts of matching datasets in index, grouped by product.
        """

    @abstractmethod
    def count_by_product_through_time(self,
                                      period: str,
                                      **query: QueryField
                                     ) -> Iterable[Tuple[Product, Iterable[Tuple[Range, int]]]]:
        """
        Perform a search, returning counts for each product grouped in time slices
        of the given period.

        :param period: Time range for each slice: '1 month', '1 day' etc.
        :param query: search query parameters
        :returns: For each matching product type, a list of time ranges and their count.
        """

    @abstractmethod
    def count_product_through_time(self,
                                   period: str,
                                   **query: QueryField
                                  ) -> Iterable[Tuple[Range, int]]:
        """
        Perform a search, returning counts for a single product grouped in time slices
        of the given period.

        Will raise an error if the search terms match more than one product.

        :param period: Time range for each slice: '1 month', '1 day' etc.
        :param query: search query parameters
        :returns: The product, a list of time ranges and the count of matching datasets.
        """

    @abstractmethod
    def search_summaries(self, **query: QueryField) -> Iterable[Mapping[str, Any]]:
        """
        Perform a search, returning just the search fields of each dataset.

        :param query: search query parameters
        :return: Mappings of search fields for matching datasets
        """

    def search_eager(self, **query: QueryField) -> List[Dataset]:
        """
        Perform a search, returning results as Dataset objects.

        :param query: search query parameters
        :return: Fully instantiated list of matching dataset models
        """
        return list(self.search(**query))  # type: ignore[arg-type]   # mypy isn't being very smart here :(

    @abstractmethod
    def temporal_extent(self, ids: Iterable[DSID]) -> tuple[datetime.datetime, datetime.datetime]:
        """
        Returns the minimum and maximum acquisition time of an iterable of dataset ids.

        Raises KeyError if none of the datasets are in the index

        :param ids: Iterable of dataset ids.
        :return: minimum and maximum acquisition times
        """

    @deprecat(
        reason="This method has been moved to the Product Resource and renamed 'temporal_extent()'",
        version="1.9.0",
        category=ODC2DeprecationWarning
    )
    def get_product_time_bounds(self,
                                product: str | Product
                               ) -> tuple[datetime.datetime, datetime.datetime]:
        """
        Returns the minimum and maximum acquisition time of the product.

        :param product: Product of name of product
        :return: minimum and maximum acquisition times
        """
        return self._index.products.temporal_extent(product=product)

    @abstractmethod
    def search_returning_datasets_light(self,
                                        field_names: Tuple[str, ...],
                                        custom_offsets: Optional[Mapping[str, Offset]] = None,
                                        limit: Optional[int] = None,
                                        **query: QueryField
                                       ) -> Iterable[Tuple]:
        """
        This is a dataset search function that returns the results as objects of a dynamically
        generated Dataset class that is a subclass of tuple.

        Only the requested fields will be returned together with related derived attributes as property functions
        similer to the datacube.model.Dataset class. For example, if 'extent'is requested all of
        'crs', 'extent', 'transform', and 'bounds' are available as property functions.

        The field_names can be custom fields in addition to those specified in metadata_type, fixed fields, or
        native fields. The field_names can also be derived fields like 'extent', 'crs', 'transform',
        and 'bounds'. The custom fields require custom offsets of the metadata doc be provided.

        The datasets can be selected based on values of custom fields as long as relevant custom
        offsets are provided. However custom field values are not transformed so must match what is
        stored in the database.

        :param field_names: A tuple of field names that would be returned including derived fields
                            such as extent, crs
        :param custom_offsets: A dictionary of offsets in the metadata doc for custom fields
        :param limit: Number of datasets returned per product.
        :param query: query parameters that will be processed against metadata_types,
                      product definitions and/or dataset table.
        :return: A Dynamically generated DatasetLight (a subclass of namedtuple and possibly with
        property functions).
        """

    @abstractmethod
    def spatial_extent(self, ids: Iterable[DSID], crs: CRS = CRS("EPSG:4326")) -> Geometry | None:
        """
        Return the combined spatial extent of the nominated datasets

        Uses spatial index.

        Returns None if no index for the CRS, or if no identified datasets are indexed in the relevant spatial index.
        Result will not include extents of datasets that cannot be validly projected into the CRS.

        :param ids: An iterable of dataset IDs
        :param crs: A CRS (defaults to EPSG:4326)
        :return: The combined spatial extents of the datasets.
        """

    def _extract_geom_from_query(self, q: Mapping[str, QueryField]) -> Optional[Geometry]:
        """
        Utility method for index drivers supporting spatial indexes.

        Extract a Geometry from a dataset query.  Backwards compatible with old lat/lon style queries.

        :param q: A query dictionary
        :return: A polygon or multipolygon type Geometry.  None if no spatial query clauses.
        """
        geom: Optional[Geometry] = None
        if "geometry" in q:
            # New geometry-style spatial query
            geom_term = q.pop("geometry")
            try:
                geom = Geometry(geom_term)
            except ValueError:
                # Can't convert to single Geometry. If it is an iterable of Geometries, return the union
                for term in geom_term:
                    if geom is None:
                        geom = Geometry(term)
                    else:
                        geom = geom.union(Geometry(term))
            if "lat" in q or "lon" in q:
                raise ValueError("Cannot specify lat/lon AND geometry in the same query")
            assert geom.crs
        else:
            # Old lat/lon--style spatial query (or no spatial query)
            # TODO: latitude/longitude/x/y aliases for lat/lon
            #       Also some stuff is precalced at the api.core.Datacube level.
            #       THAT needs to offload to index driver when it can.
            lat = q.pop("lat", None)
            lon = q.pop("lon", None)
            if lat is None and lon is None:
                # No spatial query
                _LOG.info("No spatial query")
                return None

            # Old lat/lon--style spatial query
            if lat is None:
                lat = Range(begin=-90, end=90)
            if lon is None:
                lon = Range(begin=-180, end=180)
            delta = 0.000001
            if isinstance(lat, Range) and isinstance(lon, Range):
                # ranges for both - build a box.
                geom = box(lon.begin, lat.begin, lon.end, lat.end, crs=CRS("EPSG:4326"))
            elif isinstance(lat, Range):
                if isinstance(lon, (int, float)):
                    # lat is a range, but lon is scalar - geom is ideally a line
                    # odc.geo is always (x, y) order - ignore lat,lon order specified by EPSG:4326
                    geom = box(lon - delta, lat.begin, lon + delta, lat.end, crs=CRS("EPSG:4326"))
                else:
                    raise ValueError("lon search term must be a Range or a numeric scalar")
            elif isinstance(lon, Range):
                if isinstance(lat, (int, float)):
                    # lon is a range, but lat is scalar - geom is ideally a line
                    # odc.geo is always (x, y) order - ignore lat,lon order specified by EPSG:4326
                    geom = box(lon.begin, lat - delta, lon.end, lat + delta, crs=CRS("EPSG:4326"))
                else:
                    raise ValueError("lat search term must be a Range or a numeric scalar")
            else:
                if isinstance(lon, (int, float)) and isinstance(lat, (int, float)):
                    # Lat and Lon are both scalars - geom is ideally point
                    # odc.geo is always (x, y) order - ignore lat,lon order specified by EPSG:4326
                    geom = box(lon - delta, lat - delta, lon + delta, lat + delta, crs=CRS("EPSG:4326"))
                else:
                    raise ValueError("lat and lon search terms must be of type Range or a numeric scalar")
        _LOG.info("Spatial Query Geometry: %s", geom.wkt)
        return geom


class AbstractTransaction(ABC):
    """
    Abstract base class for a Transaction Manager.  All index implementations should extend this base class.

    Thread-local storage and locks ensures one active transaction per index per thread.
    """

    def __init__(self, index_id: str):
        self._connection: Any = None
        self._tls_id = f"txn-{index_id}"
        self._obj_lock = Lock()
        self._controlling_trans = None

    # Main Transaction API
    def begin(self) -> None:
        """
        Start a new transaction.

        Raises an error if a transaction is already active for this thread.

        Calls implementation-specific _new_connection() method and manages thread local storage and locks.
        """
        with self._obj_lock:
            if self._connection is not None:
                raise ValueError("Cannot start a new transaction as one is already active")
            self._tls_stash()

    def commit(self) -> None:
        """
        Commit the transaction.

        Raises an error if transaction is not active.

        Calls implementation-specific _commit() method, and manages thread local storage and locks.
        """
        with self._obj_lock:
            if self._connection is None:
                raise ValueError("Cannot commit inactive transaction")
            self._commit()
            self._release_connection()
            self._connection = None
            self._tls_purge()

    def rollback(self) -> None:
        """
        Rollback the transaction.

        Raises an error if transaction is not active.

        Calls implementation-specific _rollback() method, and manages thread local storage and locks.
        """
        with self._obj_lock:
            if self._connection is None:
                raise ValueError("Cannot rollback inactive transaction")
            self._rollback()
            self._release_connection()
            self._connection = None
            self._tls_purge()

    @property
    def active(self):
        """
        :return:  True if the transaction is active.
        """
        return self._connection is not None

    # Manage thread-local storage
    def _tls_stash(self) -> None:
        """
        Check TLS is empty, create a new connection and stash it.
        :return:
        """
        stored_val = thread_local_cache(self._tls_id)
        if stored_val is not None:
            # stored_val is outermost transaction in a stack of nested transaction.
            self._controlling_trans = stored_val
            self._connection = stored_val._connection
        else:
            self._connection = self._new_connection()
            thread_local_cache(self._tls_id, purge=True)
            thread_local_cache(self._tls_id, self)

    def _tls_purge(self) -> None:
        thread_local_cache(self._tls_id, purge=True)

    # Commit/Rollback exceptions for Context Manager usage patterns
    def commit_exception(self, errmsg: str) -> TransactionException:
        return TransactionException(errmsg, commit=True)

    def rollback_exception(self, errmsg: str) -> TransactionException:
        return TransactionException(errmsg, commit=False)

    # Context Manager Interface
    def __enter__(self):
        self.begin()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if not self.active:
            # User has already manually committed or rolled back.
            return True
        if exc_type is not None and issubclass(exc_type, TransactionException):
            # User raised a TransactionException,
            if self._controlling_trans:
                # Nested transaction - reraise TransactionException
                return False
            # Commit or rollback as per exception
            if exc_value.commit:
                self.commit()
            else:
                self.rollback()
            # Tell runtime exception is caught and handled.
            return True
        elif exc_value is not None:
            # Any other exception - reraise.  Rollback if outermost transaction
            if not self._controlling_trans:
                self.rollback()
            # Instruct runtime to rethrow exception
            return False
        else:
            # Exited without exception.  Commit if outermost transaction
            if not self._controlling_trans:
                self.commit()
            return True

    # Internal abstract methods for implementation-specific functionality
    @abstractmethod
    def _new_connection(self) -> Any:
        """
        :return: a new index driver object representing a database connection or equivalent against which transactions
        will be executed.
        """

    @abstractmethod
    def _commit(self) -> None:
        """
        Commit the transaction.
        """

    @abstractmethod
    def _rollback(self) -> None:
        """
        Rollback the transaction.
        """

    @abstractmethod
    def _release_connection(self) -> None:
        """
        Release the connection object stored in self._connection
        """


class UnhandledTransaction(AbstractTransaction):
    # Minimal implementation for index drivers with no transaction handling.
    def _new_connection(self) -> Any:
        return True

    def _commit(self) -> None:
        pass

    def _rollback(self) -> None:
        pass

    def _release_connection(self) -> None:
        pass


class AbstractIndex(ABC):
    """
    Abstract base class for an Index.  All Index implementations should
    inherit from this base class, and implement all abstract methods (and
    override other methods and contract flags as required.
    """

    # Interface contracts - implementations should set to True where appropriate.

    # Metadata type support flags
    #   supports legacy ODCv1 EO style metadata types.
    supports_legacy = False
    #   supports eo3 compatible metadata types.
    supports_eo3 = False
    #   supports non-geospatial (e.g. telemetry) metadata types
    supports_nongeo = False
    #   supports geospatial vector (i.e. non-raster) metadata types (reserved for future use)
    supports_vector = False

    # Database/storage feature support flags
    #   supports add() update() remove() etc methods.
    supports_write = False
    #   supports persistent storage. Writes from previous instantiations will persist into future ones.
    #   (Requires supports_write)
    supports_persistance = False
    #    Supports ACID transactions (Requires supports_write)
    supports_transactions = False
    #    Supports per-CRS spatial indexes (Requires supports_write)
    supports_spatial_indexes = False

    # User managment support flags
    #   support the index.users API
    supports_users = False

    # Lineage support flags
    #   supports lineage (either legacy or new API)
    supports_lineage = False
    #   supports external lineage API (as described in EP-08).  Requires supports_lineage
    #   IF support_lineage is True and supports_external_lineage is False THEN legacy lineage API.
    supports_external_lineage = False
    #   supports an external lineage home field.  Requires supports_external_lineage
    supports_external_home = False

    @property
    @abstractmethod
    def environment(self) -> ODCEnvironment:
        """The cfg.ODCEnvironment object this Index was initialised from."""

    @property
    @abstractmethod
    def url(self) -> str:
        """A string representing the index"""

    @cached_property
    def url_parts(self) -> ParseResult:
        return urlparse(self.url)

    @property
    @abstractmethod
    def users(self) -> AbstractUserResource:
        """A User Resource instance for the index"""

    @property
    @abstractmethod
    def metadata_types(self) -> AbstractMetadataTypeResource:
        """A MetadataType Resource instance for the index"""

    @property
    @abstractmethod
    def products(self) -> AbstractProductResource:
        """A Product Resource instance for the index"""

    @property
    @abstractmethod
    def lineage(self) -> AbstractLineageResource:
        """A Lineage Resource instance for the index"""

    @property
    @abstractmethod
    def datasets(self) -> AbstractDatasetResource:
        """A Dataset Resource instance for the index"""

    @classmethod
    @abstractmethod
    def from_config(cls,
                    cfg_env: ODCEnvironment,
                    application_name: Optional[str] = None,
                    validate_connection: bool = True
                   ) -> "AbstractIndex":
        """Instantiate a new index from an ODCEnvironment configuration object"""

    @classmethod
    @abstractmethod
    def get_dataset_fields(cls,
                           doc: dict
                          ) -> Mapping[str, Field]:
        """Return dataset search fields from a metadata type document"""

    @abstractmethod
    def init_db(self,
                with_default_types: bool = True,
                with_permissions: bool = True) -> bool:
        """
        Initialise an empty database.

        :param with_default_types: Whether to create default metadata types
        :param with_permissions: Whether to create db permissions
        :return: true if the database was created, false if already exists
        """

    # Spatial Index API

    def create_spatial_index(self, crs: CRS) -> bool:
        """
        Create a spatial index for a CRS.

        Note that a newly created spatial index is empty.  If there are already datatsets in the index whose
        extents can be safely projected into the CRS, then it is necessary to also call update_spatial_index
        otherwise they will not be found by queries against that CRS.

        Only implemented by index drivers with supports_spatial_indexes set to True.

        :param crs: The coordinate reference system to create a spatial index for.
        :return: True if the spatial index was successfully created (or already exists)
        """
        if not self.supports_spatial_indexes:
            raise NotImplementedError("This index driver does not support the Spatial Index API")
        else:
            raise NotImplementedError()

    def spatial_indexes(self, refresh=False) -> Iterable[CRS]:
        """
        Return the CRSs for which spatial indexes have been created.

        :param refresh: If true, query the backend for the list of current spatial indexes.  If false (the default)
                        a cached list of spatial index CRSs may be returned.
        :return: An iterable of CRSs for which spatial indexes exist in the index
        """
        if not self.supports_spatial_indexes:
            raise NotImplementedError("This index driver does not support the Spatial Index API")
        else:
            raise NotImplementedError()

    def update_spatial_index(self,
                             crses: Sequence[CRS] = [],
                             product_names: Sequence[str] = [],
                             dataset_ids: Sequence[DSID] = []
                             ) -> int:
        """
        Populate a newly created spatial index (or indexes).

        Spatial indexes are automatically populated with new datasets as they are indexed, but if there were
        datasets already in the index when a new spatial index is created, or if geometries have been added or
        modified outside of the ODC in a populated index (e.g. with SQL) then the spatial indexies must be
        updated manually with this method.

        This is a very slow operation.  The product_names and dataset_ids lists can be used to break the
        operation up into chunks or allow faster updating when the spatial index is only relevant to a
        small portion of the entire index.

        :param crses: A list of CRSes whose spatial indexes are to be updated.
                      Default is to update all spatial indexes
        :param product_names: A list of product names to update the spatial indexes.
                              Default is to update for all products
        :param dataset_ids: A list of ids of specific datasets to update in the spatial index.
                            Default is to update for all datasets (or all datasts in the products
                            in the product_names list)
        :return: The number of dataset extents processed - i.e. the number of datasets updated multiplied by the
                 number of spatial indexes updated.
        """
        if not self.supports_spatial_indexes:
            raise NotImplementedError("This index driver does not support the Spatial Index API")
        else:
            raise NotImplementedError()

    def drop_spatial_index(self, crs: CRS) -> bool:
        """
        Remove a spatial index from the database.

        Note that creating spatial indexes on an existing index is a slow and expensive operation.  Do not
        delete spatial indexes unless you are absolutely certain it is no longer required by any users of
        this ODC index.

        :param crs: The CRS whose spatial index is to be deleted.
        :return: True if the spatial index was successfully dropped.
                 False if spatial index could not be dropped.
        """
        if not self.supports_spatial_indexes:
            raise NotImplementedError("This index driver does not support the Spatial Index API")
        else:
            raise NotImplementedError()

    def clone(self,
              origin_index: "AbstractIndex",
              batch_size: int = 1000,
              skip_lineage=False,
              lineage_only=False) -> Mapping[str, BatchStatus]:
        """
        Clone an existing index into this one.

        Steps are:

        1) Clone all metadata types compatible with this index driver.
           * Products and Datasets with incompatible metadata types are excluded from subsequent steps.
           * Existing metadata types are skipped, but products and datasets associated with them are only
             excluded if the existing metadata type does not match the one from the origin index.
        2) Clone all products with "safe" metadata types.
           * Products are included or excluded by metadata type as discussed above.
           * Existing products are skipped, but datasets associated with them are only
             excluded if the existing product definition does not match the one from the origin index.
        3)  Clone all datasets with "safe" products
            * Datasets are included or excluded by product and metadata type, as discussed above.
            * Archived datasets and locations are not cloned.
        4) Clone all lineage relations that can be cloned.
            * All lineage relations are skipped if either index driver does not support lineage,
              or if skip_lineage is True.
            * If this index does not support external lineage then lineage relations that reference datasets
              that do not exist in this index after step 3 above are skipped.

        API Note: This API method is not finalised and may be subject to change.

        :param origin_index: Index whose contents we wish to clone.
        :param batch_size: Maximum number of objects to write to the database in one go.
        :return: Dictionary containing a BatchStatus named tuple for "metadata_types", "products"
                 and "datasets", and optionally "lineage".
        """
        results = {}
        if not lineage_only:
            if self.supports_spatial_indexes and origin_index.supports_spatial_indexes:
                for crs in origin_index.spatial_indexes(refresh=True):
                    report_to_user(f"Creating spatial index for CRS {crs}")
                    self.create_spatial_index(crs)
                    self.update_spatial_index(crs)
            # Clone Metadata Types
            report_to_user("Cloning Metadata Types:")
            results["metadata_types"] = self.metadata_types.bulk_add(origin_index.metadata_types.get_all_docs(),
                                                                     batch_size=batch_size)
            res = results["metadata_types"]
            msg = f'{res.completed} metadata types loaded ({res.skipped} skipped) in ' \
                  f'{res.seconds_elapsed:.2f}seconds ' \
                  f'({res.completed * 60 / res.seconds_elapsed:.2f} metadata_types/min)'
            report_to_user(msg, logger=_LOG)
            metadata_cache = {name: self.metadata_types.get_by_name(name) for name in res.safe}
            # Clone Products
            report_to_user("Cloning Products:")
            results["products"] = self.products.bulk_add(origin_index.products.get_all_docs(),
                                                         metadata_types=metadata_cache,
                                                         batch_size=batch_size)
            res = results["products"]
            msg = f'{res.completed} products loaded ({res.skipped} skipped) in {res.seconds_elapsed:.2f}seconds ' \
                  f'({res.completed * 60 / res.seconds_elapsed:.2f} products/min)'
            report_to_user(msg, logger=_LOG)
            # Clone Datasets (group by product for now for convenience)
            report_to_user("Cloning Datasets:")
            products = [p for p in self.products.get_all() if p.name in res.safe]
            results["datasets"] = self.datasets.bulk_add(
                origin_index.datasets.get_all_docs(products=products, batch_size=batch_size),
                batch_size=batch_size
            )
            res = results["datasets"]
            report_to_user("")
            msg = f'{res.completed} datasets loaded ({res.skipped} skipped) in {res.seconds_elapsed:.2f}seconds ' \
                  f'({res.completed * 60 / res.seconds_elapsed:.2f} datasets/min)'
            report_to_user(msg, logger=_LOG)
        if not self.supports_lineage or not origin_index.supports_lineage or skip_lineage:
            report_to_user("Skipping lineage")
            return results
        report_to_user("Cloning Lineage:")
        results["lineage"] = self.lineage.bulk_add(origin_index.lineage.get_all_lineage(batch_size), batch_size)
        res = results["lineage"]
        report_to_user("")
        msg = f'{res.completed} lineage relations loaded ({res.skipped} skipped) in {res.seconds_elapsed:.2f}seconds ' \
              f'({res.completed * 60 / res.seconds_elapsed:.2f} lineage relations/min)'
        report_to_user(msg, logger=_LOG)
        return results

    @abstractmethod
    def close(self) -> None:
        """
        Close and cleanup the Index.
        """

    @property
    @abstractmethod
    def index_id(self) -> str:
        """
        :return: Unique ID for this index
                 (e.g. same database/dataset storage + same index driver implementation = same id)
        """

    @abstractmethod
    def transaction(self) -> AbstractTransaction:
        """
        :return: a Transaction context manager for this index.
        """

    def thread_transaction(self) -> Optional["AbstractTransaction"]:
        """
        :return: The existing Transaction object cached in thread-local storage for this index, if there is one.
        """
        return thread_local_cache(f"txn-{self.index_id}", None)

    def __enter__(self):
        return self

    def __exit__(self):
        self.close()


class AbstractIndexDriver(ABC):
    """
    Abstract base class for an IndexDriver.  All IndexDrivers should inherit from this base class
    and implement all abstract methods.
    """
    @classmethod
    @abstractmethod
    def index_class(cls) -> Type[AbstractIndex]:
        ...

    @classmethod
    def connect_to_index(cls,
                         config_env: ODCEnvironment,
                         application_name: Optional[str] = None,
                         validate_connection: bool = True
                        ) -> "AbstractIndex":
        return cls.index_class().from_config(config_env, application_name, validate_connection)

    @staticmethod
    @abstractmethod
    @deprecat(
        reason="The 'metadata_type_from_doc' static method has been deprecated. "
               "Please use the 'index.metadata_type.from_doc()' instead.",
        version='1.9.0',
        category=ODC2DeprecationWarning)
    def metadata_type_from_doc(definition: dict
                              ) -> MetadataType:
        ...

    @staticmethod
    def get_config_option_handlers(env: ODCEnvironment) -> Iterable[ODCOptionHandler]:
        """
        Default Implementation does nothing.
        Override for driver-specific config handling (e.g. for db connection)
        """
        return []


# The special handling of grid_spatial, etc appears to NOT apply to EO3.
# Does EO3 handle it in metadata?
class DatasetSpatialMixin:
    __slots__ = ()

    @property
    def _gs(self):
        return self.grid_spatial

    @property
    def crs(self):
        return Dataset.crs.__get__(self)

    @cached_property
    def extent(self):
        return Dataset.extent.func(self)

    @property
    def transform(self):
        return Dataset.transform.__get__(self)

    @property
    def bounds(self):
        return Dataset.bounds.__get__(self)

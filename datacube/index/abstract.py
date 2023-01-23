# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2022 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
import datetime
import logging
import sys

from pathlib import Path
from threading import Lock
from time import monotonic

from abc import ABC, abstractmethod
from typing import (Any, Iterable, Iterator,
                    List, Mapping, MutableMapping,
                    NamedTuple, Optional,
                    Tuple, Union, Sequence)
from uuid import UUID

from datacube.config import LocalConfig
from datacube.index.exceptions import TransactionException
from datacube.index.fields import Field
from datacube.model import Dataset, MetadataType, Range
from datacube.model import DatasetType as Product
from datacube.utils import cached_property, jsonify_document, read_documents, InvalidDocException
from datacube.utils.changes import AllowPolicy, Change, Offset, DocumentMismatchError, check_doc_unchanged
from datacube.utils.generic import thread_local_cache
from datacube.utils.geometry import CRS, Geometry, box
from datacube.utils.documents import UnknownMetadataType

_LOG = logging.getLogger(__name__)


# A named tuple representing the results of a batch add operation:
# - completed: Number of objects added to theMay be None for internal functions and for datasets.
# - skipped: Number of objects skipped, either because they already exist
#   or the documents are invalid for this driver.
# - seconds_elapsed: seconds elapsed during the bulk add operation;
# - safe: an optional list of names of bulk added objects that are safe to be
#   used for lower level bulk adds. Includes objects added, and objects skipped
#   because they already exist in the index and are identical to the version
#   being added.  May be None for internal functions and for datasets.
class BatchStatus(NamedTuple):
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
    metadata_type_resource: AbstractMetadataTypeResource

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
                metadata_type = self.metadata_type_resource.get_by_name(metadata_type)
                if (metadata_type is not None
                        and metadata_type_cache is not None
                        and metadata_type.name not in metadata_type_cache):
                    metadata_type_cache[metadata_type.name] = metadata_type
        else:
            # Otherwise they embedded a document, add it if needed:
            metadata_type = self.metadata_type_resource.from_doc(metadata_type)
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

    @abstractmethod
    def get_with_fields(self, field_names: Iterable[str]) -> Iterable[Product]:
        """
        Return products that have all of the given fields.

        :param field_names: names of fields that returned products must have
        :returns: Matching product models
        """

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
    def get(self,
            id_: DSID,
            include_sources: bool = False
           ) -> Optional[Dataset]:
        """
        Get dataset by id

        :param id_: id of the dataset to retrieve
        :param include_sources: get the full provenance graph?
        :rtype: Dataset model (None if not found)
        """

    @abstractmethod
    def bulk_get(self, ids: Iterable[DSID]) -> Iterable[Dataset]:
        """
        Get multiple datasets by id. (Lineage sources NOT included)

        :param ids: ids to retrieve
        :return: Iterable of Dataset models
        """

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
            with_lineage: bool = True
           ) -> Dataset:
        """
        Add ``dataset`` to the index. No-op if it is already present.

        :param dataset: Unpersisted dataset model

        :param with_lineage:
           - ``True (default)`` attempt adding lineage datasets if missing
           - ``False`` record lineage relations, but do not attempt
             adding lineage datasets to the db

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
               updates_allowed: Optional[Mapping[Offset, AllowPolicy]] = None
              ) -> Dataset:
        """
        Update dataset metadata and location
        :param Dataset dataset: Dataset model with unpersisted updates
        :param updates_allowed: Allowed updates
        :return: Persisted dataset model
        """

    @abstractmethod
    def archive(self, ids: Iterable[DSID]) -> None:
        """
        Mark datasets as archived

        :param Iterable[Union[str,UUID]] ids: list of dataset ids to archive
        """

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

    @abstractmethod
    def get_field_names(self, product_name: Optional[str] = None) -> Iterable[str]:
        """
        Get the list of possible search fields for a Product (or all products)

        :param product_name: Name of product, or None for all products
        :return: All possible search field names
        """

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
        :param batch_size: Number of metadata types to add per batch (default 1000)
        :return: BatchStatus named tuple, with `safe` set to None.
        """
        def increment_progress():
            if sys.stdout.isatty():
                print(".", end="", flush=True)
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
    def get_product_time_bounds(self,
                                product: str
                               ) -> Tuple[datetime.datetime, datetime.datetime]:
        """
        Returns the minimum and maximum acquisition time of the product.

        :param product: Name of product
        :return: minimum and maximum acquisition times
        """

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
    def spatial_extent(self, ids: Iterable[DSID], crs: CRS = CRS("EPSG:4326")) -> Optional[Geometry]:
        """
        Return the combined spatial extent of the nominated datasets.

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
                    # datacube.utils.geometry is always (x, y) order - ignore lat,lon order specified by EPSG:4326
                    geom = box(lon - delta, lat.begin, lon + delta, lat.end, crs=CRS("EPSG:4326"))
                else:
                    raise ValueError("lon search term must be a Range or a numeric scalar")
            elif isinstance(lon, Range):
                if isinstance(lat, (int, float)):
                    # lon is a range, but lat is scalar - geom is ideally a line
                    # datacube.utils.geometry is always (x, y) order - ignore lat,lon order specified by EPSG:4326
                    geom = box(lon.begin, lat - delta, lon.end, lat + delta, crs=CRS("EPSG:4326"))
                else:
                    raise ValueError("lat search term must be a Range or a numeric scalar")
            else:
                if isinstance(lon, (int, float)) and isinstance(lat, (int, float)):
                    # Lat and Lon are both scalars - geom is ideally point
                    # datacube.utils.geometry is always (x, y) order - ignore lat,lon order specified by EPSG:4326
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
    inherit from this base class and implement all abstract methods.
    """

    # Interface contracts
    #   supports add() update() remove() etc methods.
    supports_persistance = True
    #   supports legacy ODCv1 EO style metadata types.
    supports_legacy = True
    #   supports non-geospatial (e.g. telemetry) metadata types
    supports_nongeo = True
    #   supports lineage
    supports_lineage = True
    supports_source_filters = True
    # Supports ACID transactions
    supports_transactions = False

    @property
    @abstractmethod
    def url(self) -> str:
        """A string representing the index"""

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
    def datasets(self) -> AbstractDatasetResource:
        """A Dataset Resource instance for the index"""

    @classmethod
    @abstractmethod
    def from_config(cls,
                    config: LocalConfig,
                    application_name: Optional[str] = None,
                    validate_connection: bool = True
                   ) -> "AbstractIndex":
        """Instantiate a new index from a LocalConfig object"""

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

    def clone(self, origin_index: "AbstractIndex", batch_size: int = 1000) -> Mapping[str, BatchStatus]:
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
            * Dataset source (lineage) are not currently cloned (TODO)

        API Note: This API method is not finalised and may be subject to change.

        :param origin_index: Index whose contents we wish to clone.
        :param batch_size: Maximum number of objects to write to the database in one go.
        :return: Dictionary containing a BatchStatus named tuple for "metadata_types", "products"
                 and "datasets".
        """
        results = {}
        # Clone Metadata Types
        if sys.stdout.isatty():
            print("Cloning Metadata Types:")
        results["metadata_types"] = self.metadata_types.bulk_add(origin_index.metadata_types.get_all_docs(),
                                                                 batch_size=batch_size)
        res = results["metadata_types"]
        msg = f'{res.completed} metadata types loaded ({res.skipped} skipped) in {res.seconds_elapsed:.2f}seconds ' \
              f'({res.completed * 60 / res.seconds_elapsed:.2f} metadata_types/min)'
        if sys.stdout.isatty():
            print(msg)
        else:
            _LOG.info(msg)
        metadata_cache = {name: self.metadata_types.get_by_name(name) for name in res.safe}
        # Clone Products
        if sys.stdout.isatty():
            print("Cloning Products:")
        results["products"] = self.products.bulk_add(origin_index.products.get_all_docs(),
                                                     metadata_types=metadata_cache,
                                                     batch_size=batch_size)
        res = results["products"]
        msg = f'{res.completed} products loaded ({res.skipped} skipped) in {res.seconds_elapsed:.2f}seconds ' \
              f'({res.completed * 60 / res.seconds_elapsed:.2f} products/min)'
        if sys.stdout.isatty():
            print(msg)
        else:
            _LOG.info(msg)
        # Clone Datasets (group by product for now for convenience)
        if sys.stdout.isatty():
            print("Cloning Datasets:")
        products = [p for p in self.products.get_all() if p.name in res.safe]
        results["datasets"] = self.datasets.bulk_add(
            origin_index.datasets.get_all_docs(products=products, batch_size=batch_size),
            batch_size=batch_size
        )
        res = results["datasets"]
        if sys.stdout.isatty():
            print("")
        msg = f'{res.completed} datasets loaded ({res.skipped} skipped) in {res.seconds_elapsed:.2f}seconds ' \
              f'({res.completed * 60 / res.seconds_elapsed:.2f} datasets/min)'
        if sys.stdout.isatty():
            print(msg)
        else:
            _LOG.info(msg)
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

    @abstractmethod
    def create_spatial_index(self, crs: CRS) -> bool:
        """
        Create a spatial index using the nominated CRS.

        :param crs: The CRS to use in the spatial index.
        :return: True is the index was successfully created or already exists.
                 None if spatial indexes are not supported.
        """

    def thread_transaction(self) -> Optional["AbstractTransaction"]:
        """
        :return: The existing Transaction object cached in thread-local storage for this index, if there is one.
        """
        return thread_local_cache(f"txn-{self.index_id}", None)

    def spatial_indexes(self, refresh=False) -> Iterable[CRS]:
        """
        Return a list of CRSs for which spatiotemporal indexes exist in the database.

        :param refresh: If true, re-read from database record (e.g. to catch spatial
                        indexes recently created in another datacube session.
        :return:
        """
        _LOG.warning("Spatial index API is unstable and may change between releases.")
        return []

    def update_spatial_index(self,
                             crses: Sequence[CRS] = [],
                             product_names: Sequence[str] = [],
                             dataset_ids: Sequence[DSID] = []
                             ) -> int:
        """
        Update a spatial index
        :param crs: CRSs for Spatial Indexes to update. Default=all indexes
        :param product_names: Product names to update
        :param dsids: Dataset IDs to update

        If neither product_names nor dataset ids are supplied, update for all datasets.

        If both are supplied, both the named products and identified datasets are updated.

        If spatial indexes are not supported by the index driver, always return zero.

        :return:  Number of spatial index entries updated or verified as unindexed.
        """
        _LOG.warning("Spatial index API is unstable and may change between releases.")
        return 0

    def __enter__(self):
        return self

    def __exit__(self):
        self.close()


class AbstractIndexDriver(ABC):
    """
    Abstract base class for an IndexDriver.  All IndexDrivers should inherit from this base class
    and implement all abstract methods.
    """
    @staticmethod
    @abstractmethod
    def connect_to_index(config: LocalConfig,
                         application_name: Optional[str] = None,
                         validate_connection: bool = True
                        ) -> "AbstractIndex":
        ...

    @staticmethod
    @abstractmethod
    def metadata_type_from_doc(definition: dict
                              ) -> MetadataType:
        ...


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

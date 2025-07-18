# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2025 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
import logging
from abc import ABC, abstractmethod
from collections.abc import Iterable
from pathlib import Path
from time import monotonic
from typing import cast

from datacube.model import MetadataType
from datacube.utils import InvalidDocException, jsonify_document, read_documents
from datacube.utils.changes import Change, DocumentMismatchError, check_doc_unchanged
from datacube.utils.documents import JsonDict

from ._types import BatchStatus

_LOG: logging.Logger = logging.getLogger(__name__)

_DEFAULT_METADATA_TYPES_PATH: Path = Path(__file__).parent.joinpath(
    "default-metadata-types.yaml"
)


def default_metadata_type_docs(path: Path = _DEFAULT_METADATA_TYPES_PATH) -> list[dict]:
    """A list of the bare dictionary format of default :class:`datacube.model.MetadataType`"""
    return [doc for (_, doc) in read_documents(path)]


class AbstractMetadataTypeResource(ABC):
    """
    Abstract base class for the MetadataType portion of an index api.

    All MetadataTypeResource implementations should inherit from this base
    class and implement all abstract methods.

    (If a particular abstract method is not applicable for a particular implementation
    raise a NotImplementedError)
    """

    @abstractmethod
    def from_doc(self, definition: JsonDict) -> MetadataType:
        """
        Construct a MetadataType object from a dictionary definition

        :param definition: A metadata definition dictionary
        :return: An unpersisted MetadataType object
        """

    @abstractmethod
    def add(
        self, metadata_type: MetadataType, allow_table_lock: bool = False
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

    def bulk_add(
        self, metadata_docs: Iterable[JsonDict], batch_size: int = 1000
    ) -> BatchStatus:
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
                        f"Metadata Type {mdt.name}",
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
    def can_update(
        self, metadata_type: MetadataType, allow_unsafe_updates: bool = False
    ) -> tuple[bool, Iterable[Change], Iterable[Change]]:
        """
        Check if metadata type can be updated. Return bool,safe_changes,unsafe_changes

        Safe updates currently allow new search fields to be added, description to be changed.

        :param metadata_type: updated MetadataType
        :param allow_unsafe_updates: Allow unsafe changes. Use with caution.
        :return: Tuple of: boolean (can/can't update); safe changes; unsafe changes
        """

    @abstractmethod
    def update(
        self,
        metadata_type: MetadataType,
        allow_unsafe_updates: bool = False,
        allow_table_lock: bool = False,
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

    def update_document(
        self,
        definition: JsonDict,
        allow_unsafe_updates: bool = False,
    ) -> MetadataType:
        """
        Update a metadata type from the document. Unsafe changes will throw a ValueError by default.

        Safe updates currently allow new search fields to be added, description to be changed.

        :param definition: Updated definition
        :param allow_unsafe_updates: Allow unsafe changes. Use with caution.
        :return: Persisted updated MetadataType model
        """
        return self.update(
            self.from_doc(definition), allow_unsafe_updates=allow_unsafe_updates
        )

    def get_with_fields(self, field_names: Iterable[str]) -> Iterable[MetadataType]:
        """
        Return all metadata types that have all of the named search fields.

        :param field_names: Iterable of search field names
        :return: Iterable of matching metadata types.
        """
        for mdt in self.get_all():
            if all(field in mdt.dataset_fields for field in field_names):
                yield mdt

    def get(self, id_: int) -> MetadataType | None:
        """
        Fetch metadata type by id.

        :return: MetadataType model or None if not found
        """
        try:
            return self.get_unsafe(id_)
        except KeyError:
            return None

    def get_by_name(self, name: str) -> MetadataType | None:
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
    def check_field_indexes(
        self,
        allow_table_lock: bool = False,
        rebuild_views: bool = False,
        rebuild_indexes: bool = False,
    ) -> None:
        """
        Create or replace per-field indexes and views.

        May have no effect if not relevant for this index implementation

        :param allow_table_lock:
            Allow an exclusive lock to be taken on the table while creating the indexes.
            This will halt other user's requests until completed.

            If false, creation will be slightly slower and cannot be done in a transaction.
        :param rebuild_views: whether or not views should be rebuilt
        :param rebuild_indexes: whether or not views should be rebuilt
        """

    @abstractmethod
    def get_all(self) -> Iterable[MetadataType]:
        """
        Retrieve all Metadata Types

        :returns: All available MetadataType models
        """

    def get_all_docs(self) -> Iterable[JsonDict]:
        """
        Retrieve all Metadata Types as documents only (e.g. for an index clone)

        Default implementation calls self.get_all()

        API Note: This API method is not finalised and may be subject to change.

        :returns: All available MetadataType definition documents
        """
        # Default implementation calls get_all()
        for mdt in self.get_all():
            yield cast(JsonDict, mdt.definition)

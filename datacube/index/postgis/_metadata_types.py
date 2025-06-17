# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2025 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import logging
from collections.abc import Iterable, Mapping
from time import monotonic
from typing import TYPE_CHECKING

from cachetools.func import lru_cache
from typing_extensions import override

from datacube.index.abstract import AbstractMetadataTypeResource, BatchStatus
from datacube.index.postgis._transaction import IndexResourceAddIn
from datacube.model import MetadataType
from datacube.utils import _readable_offset, changes, jsonify_document
from datacube.utils.changes import (
    AllowPolicy,
    Change,
    Offset,
    check_doc_unchanged,
    get_doc_changes,
)
from datacube.utils.documents import JsonDict

if TYPE_CHECKING:
    from datacube.drivers.postgis import PostGisDb
    from datacube.index.postgis.index import Index

_LOG: logging.Logger = logging.getLogger(__name__)


class MetadataTypeResource(AbstractMetadataTypeResource, IndexResourceAddIn):
    def __init__(self, db: PostGisDb, index: Index) -> None:
        self._db = db
        self._index = index

        self.get_unsafe = lru_cache()(self.get_unsafe)  # type: ignore[method-assign]
        self.get_by_name_unsafe = lru_cache()(self.get_by_name_unsafe)  # type: ignore[method-assign]

    def __getstate__(self) -> tuple:
        """
        We define getstate/setstate to avoid pickling the caches
        """
        return (self._db,)

    def __setstate__(self, state):
        """
        We define getstate/setstate to avoid pickling the caches
        """
        self.__init__(*state)

    @override
    def from_doc(self, definition: JsonDict) -> MetadataType:
        """
        :param definition:
        """
        return self._make(definition)

    @override
    def add(
        self, metadata_type: MetadataType, allow_table_lock: bool = False
    ) -> MetadataType:
        """
        :param metadata_type:
        :param allow_table_lock:
            Allow an exclusive lock to be taken on the table while creating the indexes.
            This will halt other user's requests until completed.

            If false, creation will be slightly slower and cannot be done in a transaction.
        """
        # This column duplication is getting out of hand:
        MetadataType.validate_eo3(metadata_type.definition)

        existing = self.get_by_name(metadata_type.name)
        if existing:
            # They've passed us the same one again. Make sure it matches what is stored.
            _LOG.warning(
                f"Metadata Type {metadata_type.name} is already in the database, checking for differences"
            )
            check_doc_unchanged(
                existing.definition,
                jsonify_document(metadata_type.definition),
                f"Metadata Type {metadata_type.name}",
            )
            return existing
        with self._db_connection() as connection:
            connection.insert_metadata_type(
                name=metadata_type.name,
                definition=metadata_type.definition,
            )
        return self.get_by_name_unsafe(metadata_type.name)

    @override
    def _add_batch(self, batch_types: Iterable[MetadataType]) -> BatchStatus:
        # Add a "batch" of mdts.  Simple loop in a transaction for now.
        b_started = monotonic()
        values = [
            {"name": mdt.name, "definition": mdt.definition} for mdt in batch_types
        ]
        with self._db_connection() as connection:
            added, skipped = connection.insert_metadata_bulk(values)
            return BatchStatus(added, skipped, monotonic() - b_started)

    @override
    def can_update(
        self, metadata_type: MetadataType, allow_unsafe_updates: bool = False
    ) -> tuple[bool, Iterable[Change], Iterable[Change]]:
        """
        Check if metadata type can be updated. Return bool,safe_changes,unsafe_changes

        Safe updates currently allow new search fields to be added, description to be changed.

        :param metadata_type: updated MetadataType
        :param allow_unsafe_updates: Allow unsafe changes. Use with caution.
        """
        MetadataType.validate(metadata_type.definition)  # type: ignore[attr-defined]

        existing = self.get_by_name(metadata_type.name)
        if not existing:
            raise ValueError(
                f"Unknown metadata type {metadata_type.name}, cannot update - "
                "did you intend to add it?"
            )

        updates_allowed: Mapping[Offset, AllowPolicy] = {
            ("description",): changes.allow_any,
            # You can add new fields safely but not modify existing ones.
            ("dataset",): changes.allow_extension,
            ("dataset", "search_fields"): changes.allow_extension,
        }

        doc_changes = get_doc_changes(
            existing.definition, jsonify_document(metadata_type.definition)
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
        metadata_type: MetadataType,
        allow_unsafe_updates: bool = False,
        allow_table_lock: bool = False,
    ) -> MetadataType:
        """
        Update a metadata type from the document. Unsafe changes will throw a ValueError by default.

        Safe updates currently allow new search fields to be added, description to be changed.

        :param metadata_type: updated MetadataType
        :param allow_unsafe_updates: Allow unsafe changes. Use with caution.
        :param allow_table_lock:
            Allow an exclusive lock to be taken on the table while creating the indexes.
            This will halt other user's requests until completed.

            If false, creation will be slower and cannot be done in a transaction.
        """
        can_update, safe_changes, unsafe_changes = self.can_update(
            metadata_type, allow_unsafe_updates
        )

        if not safe_changes and not unsafe_changes:
            _LOG.warning("No changes detected for metadata type %s", metadata_type.name)
        elif not can_update:
            raise ValueError(
                f"Unsafe changes in {metadata_type.name}: "
                + (
                    ", ".join(
                        _readable_offset(offset) for offset, _, _ in unsafe_changes
                    )
                )
            )
        else:
            _LOG.info("Updating metadata type %s", metadata_type.name)

            with self._db_connection() as connection:
                connection.update_metadata_type(
                    name=metadata_type.name,
                    definition=metadata_type.definition,
                )

            self.get_by_name_unsafe.cache_clear()  # type: ignore[attr-defined]
            self.get_unsafe.cache_clear()  # type: ignore[attr-defined]
        return self.get_by_name_unsafe(metadata_type.name)

    @override
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
        """
        return self.update(
            self.from_doc(definition), allow_unsafe_updates=allow_unsafe_updates
        )

    # This is memoized in the constructor
    # pylint: disable=method-hidden
    @override
    def get_unsafe(self, id_: int) -> MetadataType:
        with self._db_connection() as connection:
            record = connection.get_metadata_type(id_)
        if record is None:
            raise KeyError("%s is not a valid MetadataType id")
        return self._make_from_query_row(record)

    # This is memoized in the constructor
    # pylint: disable=method-hidden
    @override
    def get_by_name_unsafe(self, name: str) -> MetadataType:
        with self._db_connection() as connection:
            record = connection.get_metadata_type_by_name(name)
        if not record:
            raise KeyError(f"{name} is not a valid MetadataType name")
        return self._make_from_query_row(record)

    @override
    def check_field_indexes(
        self,
        allow_table_lock: bool = False,
        rebuild_views: bool = False,
        rebuild_indexes: bool = False,
    ) -> None:
        """
        Create or replace per-field indexes and views.
        :param allow_table_lock:
            Allow an exclusive lock to be taken on the table while creating the indexes.
            This will halt other user's requests until completed.

            If false, creation will be slightly slower and cannot be done in a transaction.
        """
        pass

    @override
    def get_all(self) -> Iterable[MetadataType]:
        """
        Retrieve all Metadata Types
        """
        with self._db_connection() as connection:
            return self._make_many(connection.get_all_metadata_types())

    @override
    def get_all_docs(self) -> Iterable[JsonDict]:
        with self._db_connection() as connection:
            yield from connection.get_all_metadata_type_defs()

    def _make_many(self, query_rows) -> list[MetadataType]:
        return [self._make_from_query_row(c) for c in query_rows]

    def _make_from_query_row(self, query_row) -> MetadataType:
        return self._make(query_row.definition, query_row.id)

    def _make(self, definition: dict, id_: int | None = None) -> MetadataType:
        MetadataType.validate_eo3(definition)
        return MetadataType(
            definition,
            dataset_search_fields=self._db.get_dataset_fields(definition),
            id_=id_,
        )

# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2022 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
import logging

from datacube.index.abstract import AbstractMetadataTypeResource, default_metadata_type_docs
from datacube.model import MetadataType
from datacube.model.fields import get_dataset_fields
from datacube.utils import jsonify_document, changes, _readable_offset
from datacube.utils.changes import check_doc_unchanged, get_doc_changes

_LOG = logging.getLogger(__name__)


class MetadataTypeResource(AbstractMetadataTypeResource):
    def __init__(self):
        self.by_id = {}
        self.by_name = {}
        self.next_id = 1
        for doc in default_metadata_type_docs():
            self.add(self.from_doc(doc))

    def from_doc(self, definition):
        MetadataType.validate(definition)
        return self._make(definition)

    def add(self, metadata_type, allow_table_lock=False):
        MetadataType.validate(metadata_type.definition)
        if metadata_type.name in self.by_name:
            # Error unless it's the exact same metadata_type
            check_doc_unchanged(self.by_name[metadata_type.name].definition,
                                jsonify_document(metadata_type.definition),
                                f"Metadata Type {metadata_type.name}")
        else:
            persisted = self._make(jsonify_document(metadata_type.definition),
                                   id=self.next_id)
            self.next_id += 1
            self.by_id[metadata_type.id] = persisted
            self.by_name[metadata_type.name] = persisted
        return self.get_by_name(metadata_type.name)

    def can_update(self, metadata_type, allow_unsafe_updates=False):
        MetadataType.validate(metadata_type.definition)
        existing = self.get_by_name(metadata_type.name)
        if not existing:
            raise ValueError(f"Unknown metadata type {metadata_type.name}, cannot update - add first")
        updates_allowed = {
            ('description',): changes.allow_any,
            # You can add new fields safely but not modify existing ones.
            ('dataset',): changes.allow_extension,
            ('dataset', 'search_fields'): changes.allow_extension
        }

        doc_changes = get_doc_changes(existing.definition, jsonify_document(metadata_type.definition))
        good_changes, bad_changes = changes.classify_changes(doc_changes, updates_allowed)
        for offset, old_val, new_val in good_changes:
            _LOG.info("Safe change in %s from %r to %r", _readable_offset(offset), old_val, new_val)

        for offset, old_val, new_val in bad_changes:
            _LOG.warning("Unsafe change in %s from %r to %r", _readable_offset(offset), old_val, new_val)

        return (
            (allow_unsafe_updates or not bad_changes),
            good_changes,
            bad_changes
        )

    def update(self, metadata_type: MetadataType, allow_unsafe_updates=False, allow_table_lock=False):
        can_update, safe_changes, unsafe_changes = self.can_update(metadata_type, allow_unsafe_updates)

        if not safe_changes and not unsafe_changes:
            _LOG.info(f"No changes detected for metadata type {metadata_type.name}")
            return self.get_by_name(metadata_type.name)

        if not can_update:
            errs = ", ".join(_readable_offset(change[0]) for change in unsafe_changes)
            raise ValueError(f"Unsafe changes in {metadata_type.name}: {errs}")

        _LOG.info(f"Updating metadata type {metadata_type.name}")

        persisted = self._make(jsonify_document(metadata_type.definition),
                               id_=metadata_type.id)
        self.by_id[metadata_type.id] = persisted
        self.by_name[metadata_type.name] = persisted
        return persisted

    def get_unsafe(self, id_):
        return self.by_id[id_]

    def get_by_name_unsafe(self, name):
        return self.by_id[name]

    def check_field_indexes(self, allow_table_lock=False, rebuild_all=None,
                            rebuild_views=False, rebuild_indexes=False):
        # Cannot implement this method without separating index implementation into
        # separate layer from the API Resource implmentations.
        pass

    def get_all(self):
        return (mdt for mdt in self.by_id.values())

    @staticmethod
    def _make(definition, id_=None):
        return MetadataType(definition,
                            dataset_search_fields=get_dataset_fields(definition),
                            id_=id_)

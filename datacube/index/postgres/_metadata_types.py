# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2020 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
import logging

from cachetools.func import lru_cache

from datacube.index.abstract import AbstractMetadataTypeResource
from datacube.model import MetadataType
from datacube.utils import jsonify_document, changes, _readable_offset
from datacube.utils.changes import check_doc_unchanged, get_doc_changes

_LOG = logging.getLogger(__name__)


class MetadataTypeResource(AbstractMetadataTypeResource):
    def __init__(self, db):
        """
        :type db: datacube.drivers.postgres._connections.PostgresDb
        """
        self._db = db

        self.get_unsafe = lru_cache()(self.get_unsafe)
        self.get_by_name_unsafe = lru_cache()(self.get_by_name_unsafe)

    def __getstate__(self):
        """
        We define getstate/setstate to avoid pickling the caches
        """
        return (self._db,)

    def __setstate__(self, state):
        """
        We define getstate/setstate to avoid pickling the caches
        """
        self.__init__(*state)

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
            _LOG.warning(f"Metadata Type {metadata_type.name} is already in the database, checking for differences")
            check_doc_unchanged(
                existing.definition,
                jsonify_document(metadata_type.definition),
                'Metadata Type {}'.format(metadata_type.name)
            )
        else:
            with self._db.connect() as connection:
                connection.insert_metadata_type(
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
            raise ValueError('Unknown metadata type %s, cannot update – '
                             'did you intend to add it?' % metadata_type.name)

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

        return allow_unsafe_updates or not bad_changes, good_changes, bad_changes

    def update(self, metadata_type: MetadataType, allow_unsafe_updates=False, allow_table_lock=False):
        """
        Update a metadata type from the document. Unsafe changes will throw a ValueError by default.

        Safe updates currently allow new search fields to be added, description to be changed.

        :param datacube.model.MetadataType metadata_type: updated MetadataType
        :param bool allow_unsafe_updates: Allow unsafe changes. Use with caution.
        :param allow_table_lock:
            Allow an exclusive lock to be taken on the table while creating the indexes.
            This will halt other user's requests until completed.

            If false, creation will be slower and cannot be done in a transaction.
        :rtype: datacube.model.MetadataType
        """
        can_update, safe_changes, unsafe_changes = self.can_update(metadata_type, allow_unsafe_updates)

        if not safe_changes and not unsafe_changes:
            _LOG.warning("No changes detected for metadata type %s", metadata_type.name)
            return self.get_by_name(metadata_type.name)

        if not can_update:
            raise ValueError(f"Unsafe changes in {metadata_type.name}: " + (
                ", ".join(
                    _readable_offset(offset)
                    for offset, _, _ in unsafe_changes
                )
            ))

        _LOG.info("Updating metadata type %s", metadata_type.name)

        with self._db.connect() as connection:
            connection.update_metadata_type(
                name=metadata_type.name,
                definition=metadata_type.definition,
                concurrently=not allow_table_lock
            )

        self.get_by_name_unsafe.cache_clear()   # type: ignore[attr-defined]
        self.get_unsafe.cache_clear()           # type: ignore[attr-defined]
        return self.get_by_name(metadata_type.name)

    def update_document(self, definition, allow_unsafe_updates=False):
        """
        Update a metadata type from the document. Unsafe changes will throw a ValueError by default.

        Safe updates currently allow new search fields to be added, description to be changed.

        :param dict definition: Updated definition
        :param bool allow_unsafe_updates: Allow unsafe changes. Use with caution.
        :rtype: datacube.model.MetadataType
        """
        return self.update(self.from_doc(definition), allow_unsafe_updates=allow_unsafe_updates)

    # This is memoized in the constructor
    # pylint: disable=method-hidden
    def get_unsafe(self, id_):  # type: ignore
        with self._db.connect() as connection:
            record = connection.get_metadata_type(id_)
        if record is None:
            raise KeyError('%s is not a valid MetadataType id')
        return self._make_from_query_row(record)

    # This is memoized in the constructor
    # pylint: disable=method-hidden
    def get_by_name_unsafe(self, name):  # type: ignore
        with self._db.connect() as connection:
            record = connection.get_metadata_type_by_name(name)
        if not record:
            raise KeyError('%s is not a valid MetadataType name' % name)
        return self._make_from_query_row(record)

    def check_field_indexes(self, allow_table_lock=False,
                            rebuild_views=False, rebuild_indexes=False):
        """
        Create or replace per-field indexes and views.
        :param allow_table_lock:
            Allow an exclusive lock to be taken on the table while creating the indexes.
            This will halt other user's requests until completed.

            If false, creation will be slightly slower and cannot be done in a transaction.
        """
        with self._db.connect() as connection:
            connection.check_dynamic_fields(
                concurrently=not allow_table_lock,
                rebuild_indexes=rebuild_indexes,
                rebuild_views=rebuild_views,
            )

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
            dataset_search_fields=self._db.get_dataset_fields(definition),
            id_=id_
        )

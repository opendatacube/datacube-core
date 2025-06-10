# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2025 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
"""
API for dataset indexing, access and search.
"""

from __future__ import annotations

import datetime
import json
import logging
import warnings
from collections import namedtuple
from collections.abc import Generator, Iterable, Iterator, Mapping, Sequence
from time import monotonic
from typing import TYPE_CHECKING, Any
from uuid import UUID

from deprecat import deprecat
from typing_extensions import override

from datacube.drivers.postgres._api import split_uri
from datacube.drivers.postgres._fields import SimpleDocField
from datacube.drivers.postgres._schema import DATASET
from datacube.index import fields
from datacube.index.abstract import (
    DSID,
    AbstractDatasetResource,
    BatchStatus,
    DatasetSpatialMixin,
    DatasetTuple,
)
from datacube.index.postgres._transaction import IndexResourceAddIn
from datacube.migration import ODC2DeprecationWarning
from datacube.model import Dataset, Product, Range
from datacube.model._base import QueryField
from datacube.model.fields import Expression, Field
from datacube.model.utils import flatten_datasets
from datacube.utils import _readable_offset, changes, jsonify_document
from datacube.utils.changes import Offset, get_doc_changes

if TYPE_CHECKING:
    from datacube.drivers.postgres import PostgresDb
    from datacube.index.postgres.index import Index

_LOG: logging.Logger = logging.getLogger(__name__)


# It's a public api, so we can't reorganise old methods.
# pylint: disable=too-many-public-methods, too-many-lines
class DatasetResource(AbstractDatasetResource, IndexResourceAddIn):
    def __init__(self, db: PostgresDb, index: Index) -> None:
        self._db = db
        super().__init__(index)

    @override
    def get_unsafe(
        self,
        id_: DSID,
        include_sources: bool = False,
        include_deriveds: bool = False,
        max_depth: int = 0,
    ) -> Dataset:
        """
        Get dataset by id (raise KeyError if not in index)

        :param id_: id of the dataset to retrieve
        :param include_sources: get the full provenance graph?
        """
        # include_derived and max_depth arguments not supported.
        self._check_get_legacy(include_deriveds, max_depth)
        if isinstance(id_, str):
            id_ = UUID(id_)

        with self._db_connection() as connection:
            if not include_sources:
                dataset = connection.get_dataset(id_)
                if not dataset:
                    raise KeyError(id_)
                return self._make(dataset, full_info=True)

            datasets = {
                result.id: (self._make(result, full_info=True), result)
                for result in connection.get_dataset_sources(id_)
            }

        if not datasets:
            # No dataset found
            raise KeyError(id_)

        for dataset, result in datasets.values():
            dataset.metadata.sources = {
                classifier: datasets[source][0].metadata_doc
                for source, classifier in zip(result.sources, result.classes)
                if source
            }
            dataset.sources = {
                classifier: datasets[source][0]
                for source, classifier in zip(result.sources, result.classes)
                if source
            }
        return datasets[id_][0]

    @override
    def bulk_get(self, ids: Iterable[str | UUID]) -> list:
        def to_uuid(x) -> UUID:
            return x if isinstance(x, UUID) else UUID(x)

        ids = [to_uuid(i) for i in ids]

        with self._db_connection() as connection:
            rows = connection.get_datasets(ids)
            return [self._make(r, full_info=True) for r in rows]

    @override
    def get_derived(self, id_: DSID) -> list[Dataset]:
        """
        Get all derived datasets

        :param id_: dataset id
        """
        if not isinstance(id_, UUID):
            id_ = UUID(id_)
        with self._db_connection() as connection:
            return [
                self._make(result, full_info=True)
                for result in connection.get_derived_datasets(id_)
            ]

    @override
    def has(self, id_: DSID) -> bool:
        """
        Have we already indexed this dataset?

        :param id_: dataset id
        """
        with self._db_connection() as connection:
            return connection.contains_dataset(id_)

    @override
    def bulk_has(self, ids_: Iterable[DSID]) -> list[bool]:
        """
        Like `has` but operates on a list of ids.

        For every supplied id check if database contains a dataset with that id.

        :param ids_: list of dataset ids
        """
        with self._db_connection() as connection:
            existing = set(connection.datasets_intersection(ids_))

        return [
            x in existing for x in (UUID(x) if isinstance(x, str) else x for x in ids_)
        ]

    @override
    def add(
        self,
        dataset: Dataset,
        with_lineage: bool = True,
        archive_less_mature: int | None = None,
    ) -> Dataset:
        """
        Add ``dataset`` to the index. No-op if it is already present.

        :param dataset: dataset to add

        :param with_lineage:
           - ``True (default)`` attempt adding lineage datasets if missing
           - ``False`` record lineage relations, but do not attempt
             adding lineage datasets to the db

        :param archive_less_mature: if integer, search for less
               mature versions of the dataset with the int value as a millisecond
               delta in timestamp comparison
        """

        def process_bunch(dss, main_ds, transaction) -> None:
            edges: list = []

            # First insert all new datasets
            for ds in dss:
                product_id = ds.product.id
                if product_id is None:
                    # don't assume the product has an id value since it's optional,
                    # but we should error if the product doesn't exist in the db
                    product_id = self.products.get_by_name_unsafe(ds.product.name).id
                is_new = transaction.insert_dataset(
                    ds.metadata_doc_without_lineage(), ds.id, product_id
                )
                sources = ds.sources
                if is_new and sources is not None:
                    edges.extend((name, ds.id, src.id) for name, src in sources.items())

            # Second insert lineage graph edges
            for ee in edges:
                transaction.insert_dataset_source(*ee)

            # Finally update location for top-level dataset only
            if main_ds.uri is not None:
                self._ensure_new_locations(main_ds, transaction=transaction)

        _LOG.info("Indexing %s", dataset.id)

        if with_lineage:
            # Tuple return type is only with_depth_grouping=True.
            ds_by_uuid = flatten_datasets(dataset)
            assert isinstance(ds_by_uuid, dict)  # For typechecker.
            all_uuids = list(ds_by_uuid)

            present = dict(zip(all_uuids, self.bulk_has(all_uuids)))

            if present[dataset.id]:
                _LOG.warning("Dataset %s is already in the database", dataset.id)
                return dataset

            dss = [
                ds
                for ds in [dss[0] for dss in ds_by_uuid.values()]
                if not present[ds.id]
            ]
        else:
            dss = [dataset]

        with self._db_connection(transaction=True) as transaction:
            if self.has(dataset.id):
                _LOG.warning("Dataset %s is already in the database", dataset.id)
            else:
                process_bunch(dss, dataset, transaction)
            if archive_less_mature is not None:
                self.archive_less_mature(dataset, archive_less_mature)

        return dataset

    @override
    def _add_batch(
        self, batch_ds: Iterable[DatasetTuple], cache: Mapping[str, Any]
    ) -> BatchStatus:
        b_started = monotonic()
        batch: dict[str, list[dict[str, Any]]] = {
            "datasets": [],
            "uris": [],
        }
        for prod, metadata_doc, uris in batch_ds:
            dsid = UUID(str(metadata_doc["id"]))
            batch["datasets"].append(
                {
                    "id": dsid,
                    "dataset_type_ref": prod.id,
                    "metadata": metadata_doc,
                    "metadata_type_ref": prod.metadata_type.id,
                }
            )
            if isinstance(uris, str):
                uris = [uris]
            for uri in uris:
                scheme, body = split_uri(uri)
                batch["uris"].append(
                    {
                        "dataset_ref": dsid,
                        "uri_scheme": scheme,
                        "uri_body": body,
                    }
                )
        with self._db_connection(transaction=True) as connection:
            if batch["datasets"]:
                b_added, b_skipped = connection.insert_dataset_bulk(batch["datasets"])
            if batch["uris"]:
                connection.insert_dataset_location_bulk(batch["uris"])
        return BatchStatus(b_added, b_skipped, monotonic() - b_started)

    @override
    def search_product_duplicates(
        self, product: Product, *args
    ) -> Iterable[tuple[tuple, Iterable]]:
        """
        Find dataset ids who have duplicates of the given set of field names.

        Product is always inserted as the first grouping field.

        Returns each set of those field values and the datasets that have them.
        """

        def load_field(f: str | fields.Field) -> fields.Field:
            if isinstance(f, str):
                return product.metadata_type.dataset_fields[f]
            assert isinstance(f, fields.Field), f"Not a field: {f!r}"
            return f

        group_fields: list[fields.Field] = [load_field(f) for f in args]
        expressions: list[Expression] = [
            product.metadata_type.dataset_fields["product"] == product.name
        ]

        with self._db_connection() as connection:
            for record in connection.get_duplicates(group_fields, expressions):
                as_dict = record._asdict()
                if "ids" in as_dict.keys():
                    ids = as_dict.pop("ids")
                    yield (
                        namedtuple("search_result", as_dict.keys())(**as_dict),
                        set(ids),
                    )

    @override
    def can_update(
        self, dataset: Dataset, updates_allowed: Mapping | None = None
    ) -> tuple[bool, list[changes.Change], list[changes.Change]]:
        """
        Check if dataset can be updated. Return bool,safe_changes,unsafe_changes

        :param dataset: Dataset to update
        :param updates_allowed: Allowed updates
        """
        need_sources = dataset.sources is not None
        existing = self.get(dataset.id, include_sources=need_sources)
        if not existing:
            raise ValueError(
                f"Unknown dataset {dataset.id}, cannot update - did you intend to add it?"
            )

        if dataset.product.name != existing.product.name:
            raise ValueError(
                "Changing product is not supported. From "
                f"{existing.product.name} to {dataset.product.name} "
                f"in {dataset.id}"
            )

        # TODO: figure out (un)safe changes from metadata type?
        allowed: dict = {
            # can always add more metadata
            (): changes.allow_extension,
        }
        allowed.update(updates_allowed or {})

        doc_changes = get_doc_changes(
            existing.metadata_doc, jsonify_document(dataset.metadata_doc)
        )
        good_changes, bad_changes = changes.classify_changes(doc_changes, allowed)

        return not bad_changes, good_changes, bad_changes

    @override
    def update(
        self,
        dataset: Dataset,
        updates_allowed=None,
        archive_less_mature: int | None = None,
    ) -> Dataset:
        """
        Update dataset metadata and location
        :param dataset: Dataset to update
        :param updates_allowed: Allowed updates
        :param archive_less_mature: if integer, search for less
               mature versions of the dataset with the int value as a millisecond
               delta in timestamp comparison
        """
        existing = self.get(dataset.id)
        can_update, safe_changes, unsafe_changes = self.can_update(
            dataset, updates_allowed
        )

        if not safe_changes and not unsafe_changes:
            self._ensure_new_locations(dataset, existing)
            _LOG.info("No changes detected for dataset %s", dataset.id)
            return dataset

        for offset, old_val, new_val in safe_changes:
            _LOG.info(
                "Safe change in %s from %r to %r",
                _readable_offset(offset),
                old_val,
                new_val,
            )

        for offset, old_val, new_val in unsafe_changes:
            _LOG.warning(
                "Unsafe change in %s from %r to %r",
                _readable_offset(offset),
                old_val,
                new_val,
            )

        if not can_update:
            raise ValueError(
                f"Unsafe changes in {dataset.id}: "
                + (
                    ", ".join(
                        _readable_offset(offset) for offset, _, _ in unsafe_changes
                    )
                )
            )

        _LOG.info("Updating dataset %s", dataset.id)

        product = self.products.get_by_name(dataset.product.name)
        with self._db_connection(transaction=True) as transaction:
            if not transaction.update_dataset(
                dataset.metadata_doc_without_lineage(), dataset.id, product.id
            ):
                raise ValueError(f"Failed to update dataset {dataset.id}...")
            if archive_less_mature is not None:
                self.archive_less_mature(dataset, archive_less_mature)

        self._ensure_new_locations(dataset, existing)

        return dataset

    def _ensure_new_locations(self, dataset, existing=None, transaction=None) -> None:
        old_uris = set()
        if existing:
            old_uris.update(existing._uris)
        new_uris = dataset._uris

        def ensure_locations_in_transaction(old_uris, new_uris, transaction) -> None:
            if (
                len(old_uris) <= 1
                and len(new_uris) == 1
                and new_uris[0] not in old_uris
            ):
                # Only one location, so treat as an update.
                if len(old_uris):
                    transaction.remove_location(dataset.id, old_uris.pop())
                transaction.insert_dataset_location(dataset.id, new_uris[0])
            else:
                for uri in new_uris[::-1]:
                    if uri not in old_uris:
                        transaction.insert_dataset_location(dataset.id, uri)

        if transaction:
            ensure_locations_in_transaction(old_uris, new_uris, transaction)
        else:
            with self._db_connection(transaction=True) as tr:
                ensure_locations_in_transaction(old_uris, new_uris, tr)

    @override
    def archive(self, ids: Iterable[DSID]) -> None:
        """
        Mark datasets as archived

        :param ids: list of dataset ids to archive
        """
        with self._db_connection(transaction=True) as transaction:
            for id_ in ids:
                transaction.archive_dataset(id_)

    @override
    def restore(self, ids: Iterable[DSID]) -> None:
        """
        Mark datasets as not archived

        :param ids: list of dataset ids to restore
        """
        with self._db_connection(transaction=True) as transaction:
            for id_ in ids:
                transaction.restore_dataset(id_)

    @override
    def purge(
        self, ids: Iterable[DSID], allow_delete_active: bool = False
    ) -> Sequence[DSID]:
        """
        Delete datasets

        :param ids: iterable of dataset ids to purge
        :param allow_delete_active: whether active datasets can be deleted
        :return: list of purged dataset ids
        """
        purged = []
        with self._db_connection(transaction=True) as transaction:
            for id_ in ids:
                ds = self.get(id_)
                if ds is None:
                    continue
                if not ds.is_archived and not allow_delete_active:
                    _LOG.warning(f"Cannot purge unarchived dataset: {id_}")
                    continue
                transaction.delete_dataset(id_)
                purged.append(id_)

        return purged

    @override
    def get_all_dataset_ids(self, archived: bool | None = False) -> list[UUID]:
        """
        Get list of all dataset IDs based only on archived status

        This will be very slow and inefficient for large databases, and is really
        only intended for small and/or experimental databases.

        :param archived:
        """
        with self._db_connection(transaction=True) as transaction:
            return [dsid[0] for dsid in transaction.all_dataset_ids(archived)]

    @deprecat(
        reason="Multiple locations per dataset are now deprecated.  Please use the 'get_location' method.",
        version="1.9.0",
        category=ODC2DeprecationWarning,
    )
    @override
    def get_locations(self, id_: DSID) -> Iterable[str]:
        """
        Get the list of storage locations for the given dataset id

        :param id_: dataset id
        """
        with self._db_connection() as connection:
            return connection.get_locations(id_)

    @override
    def get_location(self, id_: DSID) -> str | None:
        """
        Get the list of storage locations for the given dataset id

        :param id_: dataset id
        """
        with self._db_connection() as connection:
            locations = connection.get_locations(id_)
        if not locations:
            return None
        return locations[0]

    @deprecat(
        reason="Multiple locations per dataset are now deprecated. "
        "Archived locations may not be accessible in future releases.",
        version="1.9.0",
        category=ODC2DeprecationWarning,
    )
    @override
    def get_archived_locations(self, id_: DSID) -> list[str]:
        """
        Find locations which have been archived for a dataset

        :param id_: dataset id
        """
        with self._db_connection() as connection:
            return [uri for uri, archived_dt in connection.get_archived_locations(id_)]

    @deprecat(
        reason="Multiple locations per dataset are now deprecated. "
        "Archived locations may not be accessible in future releases.",
        version="1.9.0",
        category=ODC2DeprecationWarning,
    )
    @override
    def get_archived_location_times(
        self, id_: DSID
    ) -> list[tuple[str, datetime.datetime]]:
        """
        Get each archived location along with the time it was archived.

        :param id_: dataset id
        """
        with self._db_connection() as connection:
            return list(connection.get_archived_locations(id_))

    @deprecat(
        reason="Multiple locations per dataset are now deprecated. "
        "Dataset location can be set or updated with the update() method.",
        version="1.9.0",
        category=ODC2DeprecationWarning,
    )
    @override
    def add_location(self, id_: DSID, uri: str) -> bool:
        """
        Add a location to the dataset if it doesn't already exist.

        :param id_: dataset id
        :param uri: fully qualified uri
        :return: Was one added?
        """
        if not uri:
            warnings.warn(f"Cannot add empty uri. (dataset {id_})")
            return False

        with self._db_connection() as connection:
            return connection.insert_dataset_location(id_, uri)

    @override
    def get_datasets_for_location(self, uri: str, mode: str | None = None) -> Iterator:
        """
        Find datasets that exist at the given URI

        :param uri: search uri
        :param mode: 'exact', 'prefix' or None (to guess)
        :return:
        """
        with self._db_connection() as connection:
            return (
                self._make(row)
                for row in connection.get_datasets_for_location(uri, mode=mode)
            )

    @deprecat(
        reason="Multiple locations per dataset are now deprecated. "
        "Dataset location can be set or updated with the update() method.",
        version="1.9.0",
        category=ODC2DeprecationWarning,
    )
    @override
    def remove_location(self, id_: DSID, uri: str) -> bool:
        """
        Remove a location from the dataset if it exists.

        :param id_: dataset id
        :param uri: fully qualified uri
        :return: True if one was removed
        """
        with self._db_connection() as connection:
            was_removed = connection.remove_location(id_, uri)
            return was_removed

    @deprecat(
        reason="Multiple locations per dataset are now deprecated. "
        "Archived locations may not be accessible in future releases. "
        "Dataset location can be set or updated with the update() method.",
        version="1.9.0",
        category=ODC2DeprecationWarning,
    )
    @override
    def archive_location(self, id_: DSID, uri: str) -> bool:
        """
        Archive a location of the dataset if it exists.

        :param id_: dataset id
        :param uri: fully qualified uri
        :return: True if location was able to be archived
        """
        with self._db_connection() as connection:
            was_archived = connection.archive_location(id_, uri)
            return was_archived

    @deprecat(
        reason="Multiple locations per dataset are now deprecated. "
        "Archived locations may not be restorable in future releases. "
        "Dataset location can be set or updated with the update() method.",
        version="1.9.0",
        category=ODC2DeprecationWarning,
    )
    @override
    def restore_location(self, id_: DSID, uri: str) -> bool:
        """
        Un-archive a location of the dataset if it exists.

        :param id_: dataset id
        :param uri: fully qualified uri
        :return: True if location was able to be restored
        """
        with self._db_connection() as connection:
            was_restored = connection.restore_location(id_, uri)
            return was_restored

    def _make(self, dataset_res, full_info: bool = False, product=None) -> Dataset:
        """
        :param full_info: Include all available fields
        """
        if dataset_res.uris:
            if len(dataset_res.uris) > 1:
                # Deprecated legacy code path
                kwargs = {"uris": [uri for uri in dataset_res.uris if uri]}
            else:
                kwargs = {"uri": dataset_res.uris[0]}
        else:
            kwargs = {}

        return Dataset(
            product=product or self.products.get(dataset_res.dataset_type_ref),
            metadata_doc=dataset_res.metadata,
            indexed_by=dataset_res.added_by if full_info else None,
            indexed_time=dataset_res.added if full_info else None,
            archived_time=dataset_res.archived,
            **kwargs,
        )

    def _make_many(
        self, query_result, product=None, fetch_all: bool = False
    ) -> Iterable[Dataset]:
        if fetch_all:
            return [self._make(dataset, product=product) for dataset in query_result]
        else:
            return (self._make(dataset, product=product) for dataset in query_result)

    @override
    def search_by_metadata(
        self, metadata: dict, archived: bool | None = False
    ) -> Iterable[Dataset]:
        """
        Perform a search using arbitrary metadata, returning results as Dataset objects.

        Caution - slow! This will usually not use indexes.

        :param metadata:
        :param archived: include archived datasets
        """
        with self._db_connection() as connection:
            yield from self._make_many(
                connection.search_datasets_by_metadata(metadata, archived)
            )

    @override
    @deprecat(
        deprecated_args={
            "source_filter": {
                "reason": "Filtering by source metadata is deprecated and will be removed in future.",
                "version": "1.9.0",
                "category": ODC2DeprecationWarning,
            }
        }
    )
    def search(
        self,
        limit: int | None = None,
        source_filter: int | None = None,
        archived: bool | None = False,
        order_by=None,
        **query: QueryField,
    ) -> Iterable[Dataset]:
        """
        Perform a search, returning results as Dataset objects.

        :param query:
        :param source_filter: query terms against source datasets
        :param archived: include archived datasets
        :param limit: Limit number of datasets
        """
        for product, datasets in self._do_search_by_product(
            query,
            source_filter=source_filter,
            limit=limit,
            archived=archived,
            order_by=order_by,
        ):
            yield from self._make_many(datasets, product)

    @override
    def search_by_product(
        self, archived: bool | None = False, **query: QueryField
    ) -> Iterable[tuple[Product, Iterable[Dataset]]]:
        """
        Perform a search, returning datasets grouped by product type.

        :param archived: include archived datasets
        :param query:
        """
        for product, datasets in self._do_search_by_product(query, archived=archived):
            yield product, self._make_many(datasets, product)

    @override
    def search_returning(
        self,
        field_names: Iterable[str] | None = None,
        custom_offsets: Mapping[str, Offset] | None = None,
        limit: int | None = None,
        archived: bool | None = False,
        order_by: Iterable[Any] | None = None,
        **query: QueryField,
    ) -> Generator[tuple]:
        """
        Perform a search, returning only the specified fields.

        This method can be faster than normal search() if you don't need all fields of each dataset.

        It also allows for returning rows other than datasets, such as a row per uri when requesting field 'uri'.

        :param field_names: defaults to all known search fields
        :param query:
        :param limit: Limit number of datasets
        :param archived: include archived datasets
        :param order_by: sql text, dataset field, sqlalchemy function, or expression
        by which to order results
        :return: sequence of results, each result is a namedtuple of your requested fields
        """
        field_name_d: dict[str, None] = {}
        if field_names is None and custom_offsets is None:
            for f in self._index.products.get_field_names():
                field_name_d[f] = None
        elif field_names:
            for f in field_names:
                field_name_d[f] = None
        if custom_offsets:
            custom_fields = {
                name: SimpleDocField(
                    name=name,
                    description="",
                    alchemy_column=DATASET.c.metadata,
                    indexed=False,
                    offset=offset,
                )
                for name, offset in custom_offsets.items()
            }
            for name in custom_fields:
                field_name_d[name] = None
        else:
            custom_fields = {}

        result_type = namedtuple("search_result", list(field_name_d.keys()))  # type: ignore[misc]
        for _, p_results in self._do_search_by_product(
            query,
            return_fields=True,
            select_field_names=list(field_name_d.keys()),
            additional_fields=custom_fields,
            limit=limit,
            archived=archived,
            order_by=order_by,
        ):
            for columns in p_results:
                coldict = columns._asdict()  # type: ignore[attr-defined]

                def extract_field(f):
                    # Custom fields are not type-aware and returned as stringified json.
                    return (
                        json.loads(coldict.get(f))
                        if f in custom_fields
                        else coldict.get(f)
                    )

                kwargs = {f: extract_field(f) for f in field_name_d}
                yield result_type(**kwargs)

    @override
    def count(self, archived: bool | None = False, **query: QueryField) -> int:
        """
        Perform a search, returning count of results.

        :param archived: include archived datasets
        :param query:
        """
        # This may be optimised into one query in the future.
        result = 0
        for _, count in self._do_count_by_product(query, archived=archived):
            result += count

        return result

    @override
    def count_by_product(
        self, archived: bool | None = False, **query: QueryField
    ) -> Iterable[tuple[Product, int]]:
        """
        Perform a search, returning a count of for each matching product type.

        :param archived: include archived datasets
        :param query:
        :returns: Sequence of (product, count)
        """
        return self._do_count_by_product(query, archived=archived)

    @override
    def count_by_product_through_time(
        self, period: str, archived: bool | None = False, **query: QueryField
    ) -> Iterable[tuple[Product, Iterable[tuple[Range, int]]]]:
        """
        Perform a search, returning counts for each product grouped in time slices
        of the given period.

        :param query:
        :param period: Time range for each slice: '1 month', '1 day' etc.
        :returns: For each matching product type, a list of time ranges and their count.
        """
        return self._do_time_count(period, query)

    @override
    def count_product_through_time(
        self, period: str, archived: bool | None = False, **query: QueryField
    ) -> Iterable[tuple[Range, int]]:
        """
        Perform a search, returning counts for a single product grouped in time slices
        of the given period.

        Will raise an error if the search terms match more than one product.

        :param query:
        :param period: Time range for each slice: '1 month', '1 day' etc.
        :returns: For each matching product type, a list of time ranges and their count.
        """
        return next(self._do_time_count(period, query, ensure_single=True))[1]

    def _get_dataset_types(self, q) -> set:
        types = set()
        if "product" in q.keys():
            types.add(self.products.get_by_name(q["product"]))
        else:
            # Otherwise search any metadata type that has all the given search fields.
            types = self.products.get_with_fields(tuple(q.keys()))
            if not types:
                raise ValueError(f"No type of dataset has fields: {q.keys()}")

        return types

    def _get_product_queries(self, query) -> Iterator:
        for product, q in self.products.search_robust(**query):
            q["product_id"] = product.id
            yield q, product

    # pylint: disable=too-many-locals
    def _do_search_by_product(
        self,
        query,
        return_fields: bool = False,
        additional_fields: Mapping[str, Field] | None = None,
        select_field_names: Sequence[str] | None = None,
        with_source_ids: bool = False,
        source_filter=None,
        limit: int | None = None,
        archived: bool | None = False,
        order_by=None,
    ) -> Iterable[tuple[Product, Iterable[Dataset]]]:
        if "geopolygon" in query:
            raise NotImplementedError("Spatial search API not supported by this index.")
        if source_filter:
            product_queries = list(self._get_product_queries(source_filter))
            if not product_queries:
                # No products match our source filter, so there will be no search results regardless.
                raise ValueError(f"No products match source filter: {source_filter}")
            if len(product_queries) > 1:
                raise RuntimeError(
                    "Multi-product source filters are not supported. Try adding 'product' field"
                )

            source_queries, source_product = product_queries[0]
            dataset_fields = source_product.metadata_type.dataset_fields
            source_exprs = tuple(
                fields.to_expressions(dataset_fields.get, **source_queries)
            )
        else:
            source_exprs = None

        product_queries = list(self._get_product_queries(query))
        if not product_queries:
            product = query.get("product", None)
            if product is None:
                raise ValueError(f"No products match search terms: {query!r}")
            else:
                raise ValueError(f"No such product: {product}")

        for q, product in product_queries:
            dataset_fields = product.metadata_type.dataset_fields.copy()
            if additional_fields:
                dataset_fields.update(additional_fields)
            query_exprs = tuple(fields.to_expressions(dataset_fields.get, **q))
            select_fields = None
            if return_fields:
                # if no fields specified, select all
                if select_field_names is None:
                    select_fields = None
                else:
                    select_fields = tuple(
                        dataset_fields[field_name]
                        for field_name in select_field_names
                        if field_name in dataset_fields
                    )
            with self._db_connection() as connection:
                yield (
                    product,
                    connection.search_datasets(
                        query_exprs,
                        source_exprs,
                        select_fields=select_fields,
                        limit=limit,
                        with_source_ids=with_source_ids,
                        archived=archived,
                        order_by=order_by,
                    ),
                )

    def _do_count_by_product(self, query, archived: bool | None = False) -> Iterator:
        if "geopolygon" in query:
            raise NotImplementedError("Spatial index API not supported by this index.")
        product_queries = self._get_product_queries(query)

        for q, product in product_queries:
            dataset_fields = product.metadata_type.dataset_fields
            query_exprs = tuple(fields.to_expressions(dataset_fields.get, **q))
            with self._db_connection() as connection:
                count = connection.count_datasets(query_exprs, archived=archived)
            if count > 0:
                yield product, count

    def _do_time_count(self, period, query, ensure_single: bool = False) -> Iterator:
        if "geopolygon" in query:
            raise NotImplementedError("Spatial index API not supported by this index.")
        if "time" not in query:
            raise ValueError(
                'Counting through time requires a "time" range query argument'
            )

        query = dict(query)

        start, end = query["time"]
        del query["time"]

        product_queries = list(self._get_product_queries(query))
        if ensure_single:
            if len(product_queries) == 0:
                raise ValueError(f"No products match search terms: {query!r}")
            if len(product_queries) > 1:
                raise ValueError(
                    "Multiple products match single query search: "
                    f"{[dt.name for _, dt in product_queries]!r}"
                )

        for q, product in product_queries:
            dataset_fields = product.metadata_type.dataset_fields
            query_exprs = tuple(fields.to_expressions(dataset_fields.get, **q))
            with self._db_connection() as connection:
                yield (
                    product,
                    list(
                        connection.count_datasets_through_time(
                            start, end, period, dataset_fields.get("time"), query_exprs
                        )
                    ),
                )

    @override
    @deprecat(
        reason="This method is deprecated and will be removed in 2.0.  "
        "Consider migrating to search_returning()",
        version="1.9.0",
        category=ODC2DeprecationWarning,
    )
    def search_summaries(
        self, archived: bool | None = False, **query: QueryField
    ) -> Generator[Mapping[str, Any]]:
        """
        Perform a search, returning just the search fields of each dataset.

        :param archived: include archived datasets
        :param query:
        """
        for _, results in self._do_search_by_product(
            query, return_fields=True, archived=archived
        ):
            for columns in results:
                yield columns._asdict()  # type: ignore[attr-defined]

    @override
    def spatial_extent(self, ids, crs=None) -> None:
        return None

    @override
    def temporal_extent(
        self, ids: Iterable[DSID]
    ) -> tuple[datetime.datetime, datetime.datetime]:
        """
        Returns the minimum and maximum acquisition time of the specified datasets.
        """
        raise NotImplementedError(
            "Sorry Temporal Extent by dataset ids is not supported in postgres driver."
        )

    @deprecat(
        reason="This method is deprecated and will be removed in 2.0.  "
        "Consider migrating to search_returning()",
        version="1.9.0",
        category=ODC2DeprecationWarning,
    )
    # pylint: disable=redefined-outer-name
    @override
    def search_returning_datasets_light(
        self,
        field_names: tuple,
        custom_offsets=None,
        limit: int | None = None,
        archived: bool | None = False,
        **query,
    ) -> Generator[tuple]:
        """
        This is a dataset search function that returns the results as objects of a dynamically
        generated Dataset class that is a subclass of tuple.

        Only the requested fields will be returned together with related derived attributes as property functions
        similar to the datacube.model.Dataset class. For example, if 'extent' is requested all of
        'crs', 'extent', 'transform', and 'bounds' are available as property functions.

        The field_names can be custom fields in addition to those specified in metadata_type, fixed fields, or
        native fields. The field_names can also be derived fields like 'extent', 'crs', 'transform',
        and 'bounds'. The custom fields require custom offsets of the metadata doc be provided.

        The datasets can be selected based on values of custom fields as long as relevant custom
        offsets are provided. However, custom field values are not transformed so must match what is
        stored in the database.

        :param field_names: A tuple of field names that would be returned including derived fields
                            such as extent, crs
        :param custom_offsets: A dictionary of offsets in the metadata doc for custom fields
        :param limit: Number of datasets returned per product.
        :param archived: include archived datasets
        :param query: key, value mappings of query that will be processed against metadata_types,
                      product definitions and/or dataset table.
        :return: A Dynamically generated DatasetLight (a subclass of namedtuple and possibly with
        property functions).
        """
        assert field_names

        for product, query_exprs in self.make_query_expr(query, custom_offsets):
            select_fields = self.make_select_fields(
                product, field_names, custom_offsets
            )
            select_field_names = tuple(field.name for field in select_fields)
            result_type = namedtuple("DatasetLight", select_field_names)  # type: ignore

            if "grid_spatial" in select_field_names:

                class DatasetLight(result_type, DatasetSpatialMixin):
                    pass
            else:

                class DatasetLight(result_type):  # type: ignore
                    __slots__ = ()

            with self._db_connection() as connection:
                results = connection.search_datasets(
                    query_exprs,
                    select_fields=select_fields,
                    limit=limit,
                    archived=archived,
                )

            for result in results:
                field_values = {}
                for i_, field in enumerate(select_fields):
                    # We need to load the simple doc fields
                    if isinstance(field, SimpleDocField):
                        field_values[field.name] = json.loads(result[i_])
                    else:
                        field_values[field.name] = result[i_]

                yield DatasetLight(**field_values)

    def make_select_fields(
        self, product, field_names: Sequence[str], custom_offsets
    ) -> list:
        """
        Parse and generate the list of select fields to be passed to the database API.
        """
        assert product and field_names

        dataset_fields = product.metadata_type.dataset_fields
        dataset_section = product.metadata_type.definition["dataset"]

        select_fields = []
        for field_name in field_names:
            if dataset_fields.get(field_name):
                select_fields.append(dataset_fields[field_name])
            else:
                # try to construct the field
                if field_name in {"transform", "extent", "crs", "bounds"}:
                    grid_spatial = dataset_section.get("grid_spatial")
                    if grid_spatial:
                        select_fields.append(
                            SimpleDocField(
                                "grid_spatial",
                                "grid_spatial",
                                DATASET.c.metadata,
                                False,
                                offset=grid_spatial,
                            )
                        )
                elif custom_offsets and field_name in custom_offsets:
                    select_fields.append(
                        SimpleDocField(
                            field_name,
                            field_name,
                            DATASET.c.metadata,
                            False,
                            offset=custom_offsets[field_name],
                        )
                    )
                elif field_name == "uris":
                    select_fields.append(Field("uris", "uris"))

        return select_fields

    def make_query_expr(self, query, custom_offsets) -> Iterator:
        """
        Generate query expressions including queries based on custom fields
        """
        product_queries = list(self._get_product_queries(query))
        custom_query = {}
        if not product_queries:
            # The key, values in query that are un-machable with info
            # in metadata types and product definitions, perhaps there are custom
            # fields, will need to handle custom fields separately

            canonical_query = query.copy()
            custom_query = {
                key: canonical_query.pop(key)
                for key in custom_offsets
                if key in canonical_query
            }
            product_queries = list(self._get_product_queries(canonical_query))

            if not product_queries:
                raise ValueError(f"No products match search terms: {query!r}")

        for q, product in product_queries:
            dataset_fields = product.metadata_type.dataset_fields
            query_exprs = tuple(fields.to_expressions(dataset_fields.get, **q))
            custom_query_exprs = tuple(
                self.get_custom_query_expressions(custom_query, custom_offsets)
            )

            yield product, query_exprs + custom_query_exprs

    def get_custom_query_expressions(self, custom_query, custom_offsets) -> list:
        """
        Generate query expressions for custom fields. it is assumed that custom fields are to be found
        in metadata doc and their offsets are provided. custom_query is a dict of key fields involving
        custom fields.
        """
        custom_exprs = []
        for key in custom_query:
            # for now, we assume all custom query fields are SimpleDocFields
            custom_field = SimpleDocField(
                custom_query[key],
                custom_query[key],
                DATASET.c.metadata,
                False,
                offset=custom_offsets[key],
            )
            custom_exprs.append(fields.as_expression(custom_field, custom_query[key]))

        return custom_exprs

    @override
    def get_all_docs_for_product(
        self, product: Product, batch_size: int = 1000
    ) -> Iterable[DatasetTuple]:
        product_search_key = [product.name]
        with self._db_connection(transaction=True) as connection:
            for row in connection.bulk_simple_dataset_search(
                products=product_search_key, batch_size=batch_size
            ):
                prod_name, metadata_doc, uris = tuple(row)
                yield DatasetTuple(product, metadata_doc, uris)

# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2025 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
import datetime
import logging
import re
import warnings
from collections import namedtuple
from collections.abc import Callable, Iterable, Mapping, Sequence
from itertools import chain
from time import monotonic
from typing import Any, cast
from uuid import UUID

from deprecat import deprecat
from typing_extensions import override

from datacube.index import fields
from datacube.index.abstract import (
    DSID,
    AbstractDatasetResource,
    AbstractIndex,
    BatchStatus,
    DatasetSpatialMixin,
    NoLineageResource,
    dsid_to_uuid,
)
from datacube.index.fields import Field
from datacube.index.memory._fields import build_custom_fields, get_dataset_fields
from datacube.migration import ODC2DeprecationWarning
from datacube.model import Dataset, LineageRelation, Product, Range, ranges_overlap
from datacube.model._base import QueryField
from datacube.utils import _readable_offset, changes, jsonify_document
from datacube.utils.changes import AllowPolicy, Change, Offset, get_doc_changes
from datacube.utils.dates import tz_aware
from datacube.utils.documents import JsonDict, metadata_subset

_LOG: logging.Logger = logging.getLogger(__name__)


class DatasetResource(AbstractDatasetResource):
    def __init__(self, index: AbstractIndex) -> None:
        super().__init__(index)
        # Main dataset index
        self._by_id: dict[UUID, Dataset] = {}
        # Indexes for active and archived datasets
        self._active_by_id: dict[UUID, Dataset] = {}
        self._archived_by_id: dict[UUID, Dataset] = {}
        # Lineage indexes:
        self._derived_from: dict[UUID, dict[str, UUID]] = {}
        self._derivations: dict[UUID, dict[str, UUID]] = {}
        # Location registers
        self._locations: dict[UUID, list[str]] = {}
        self._archived_locations: dict[UUID, list[tuple[str, datetime.datetime]]] = {}
        # Active Index By Product
        self._by_product: dict[str, set[UUID]] = {}
        self._archived_by_product: dict[str, set[UUID]] = {}

    @override
    def get_unsafe(
        self,
        id_: DSID,
        include_sources: bool = False,
        include_deriveds: bool = False,
        max_depth: int = 0,
    ) -> Dataset:
        self._check_get_legacy(include_deriveds, max_depth)
        ds = self.clone(
            self._by_id[dsid_to_uuid(id_)]
        )  # N.B. raises KeyError if id not in index.
        if include_sources:
            ds.sources = {
                classifier: cast(Dataset, self.get(dsid, include_sources=True))
                for classifier, dsid in self._derived_from.get(ds.id, {}).items()
            }
        return ds

    @override
    def bulk_get(self, ids: Iterable[DSID]) -> Iterable[Dataset]:
        return (ds for ds in (self.get(dsid) for dsid in ids) if ds is not None)

    @override
    def get_derived(self, id_: DSID) -> Iterable[Dataset]:
        return (
            cast(Dataset, self.get(dsid))
            for dsid in self._derivations.get(dsid_to_uuid(id_), {}).values()
        )

    @override
    def has(self, id_: DSID) -> bool:
        return dsid_to_uuid(id_) in self._by_id

    @override
    def bulk_has(self, ids_: Iterable[DSID]) -> Iterable[bool]:
        return (self.has(id_) for id_ in ids_)

    @override
    def add(
        self,
        dataset: Dataset,
        with_lineage: bool = True,
        archive_less_mature: int | None = None,
    ) -> Dataset:
        if with_lineage is None:
            with_lineage = True
        _LOG.info("indexing %s", dataset.id)
        if with_lineage and dataset.sources:
            # Add base dataset without lineage
            self.add(dataset, with_lineage=False)
            # Add lineage
            for classifier, src in dataset.sources.items():
                # Recursively add source dataset and lineage
                self.add(src, with_lineage=True)
                self.persist_source_relationship(dataset, src, classifier)
        else:
            if self.has(dataset.id):
                _LOG.warning("Dataset %s is already in the database", dataset.id)
                return dataset
            persistable = self.clone(dataset, for_save=True)
            self._by_id[persistable.id] = persistable
            self._active_by_id[persistable.id] = persistable
            if dataset._uris:
                self._locations[persistable.id] = dataset._uris.copy()
            else:
                self._locations[persistable.id] = []
            self._archived_locations[persistable.id] = []
            if dataset.product.name in self._by_product:
                self._by_product[dataset.product.name].add(dataset.id)
            else:
                self._by_product[dataset.product.name] = {dataset.id}
        if archive_less_mature is not None:
            _LOG.warning(
                "archive-less-mature functionality is not implemented for memory driver"
            )
        return cast(Dataset, self.get(dataset.id))

    def persist_source_relationship(
        self, ds: Dataset, src: Dataset, classifier: str
    ) -> None:
        # Add source lineage link
        if ds.id not in self._derived_from:
            self._derived_from[ds.id] = {}
        if self._derived_from[ds.id].get(classifier, src.id) != src.id:
            _LOG.warning(
                "Dataset %s: Old %s dataset source %s getting overwritten by %s",
                ds.id,
                classifier,
                self._derived_from[ds.id][classifier],
                src.id,
            )
        self._derived_from[ds.id][classifier] = src.id
        # Add source back-link
        if src.id not in self._derivations:
            self._derivations[src.id] = {}
        if self._derivations[src.id].get(classifier, ds.id) != ds.id:
            _LOG.warning(
                "Dataset %s: Old %s dataset derivation %s getting overwritten by %s",
                src.id,
                classifier,
                self._derivations[src.id][classifier],
                ds.id,
            )
        self._derivations[src.id][classifier] = ds.id

    @override
    def search_product_duplicates(
        self, product: Product, *args: str | Field
    ) -> Iterable[tuple[tuple, Iterable[UUID]]]:
        """
        Find dataset ids of a given product that have duplicates of the given set of field names.
        Returns each set of those field values and the datasets that have them.
        Note that this implementation does not account for slight timestamp discrepancies.
        """

        def to_field(f: str | Field) -> Field:
            if isinstance(f, str):
                f = product.metadata_type.dataset_fields[f]
            assert isinstance(f, Field), f"Not a field: {f!r}"
            return f

        fields = [to_field(f) for f in args]
        # Typing note: mypy cannot handle dynamically created namedtuples
        GroupedVals = namedtuple("search_result", [f.name for f in fields])  # type: ignore[misc]

        def values(ds: Dataset) -> GroupedVals:
            vals = []
            for field in fields:
                vals.append(field.extract(ds.metadata_doc))
            return GroupedVals(*vals)

        dups: dict[tuple, set[UUID]] = {}
        for ds in self._active_by_id.values():
            if ds.product.name != product.name:
                continue
            vals = values(ds)
            if vals in dups:
                dups[vals].add(ds.id)
            else:
                dups[vals] = {ds.id}  # avoid duplicate entries
        # only return entries with more than one dataset
        return list({k: v for k, v in dups.items() if len(v) > 1})

    @override
    def can_update(
        self,
        dataset: Dataset,
        updates_allowed: Mapping[Offset, AllowPolicy] | None = None,
    ) -> tuple[bool, Iterable[Change], Iterable[Change]]:
        # Current exactly the same as postgres implementation.  Could be pushed up to base class?
        existing = self.get(dataset.id, include_sources=dataset.sources is not None)
        if not existing:
            raise ValueError(
                f"Unknown dataset {dataset.id}, cannot update - did you intend to add it?"
            )
        if dataset.product.name != existing.product.name:
            raise ValueError(
                "Changing product is not supported. "
                f"From {existing.product.name} to {dataset.product.name} in {dataset.id}"
            )
        # TODO: Determine (un)safe changes from metadata type
        allowed: dict[Offset, AllowPolicy] = {(): changes.allow_extension}
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
        updates_allowed: Mapping[Offset, AllowPolicy] | None = None,
        archive_less_mature: int | None = None,
    ) -> Dataset:
        existing = self.get(dataset.id)
        if not existing:
            raise ValueError(
                f"Unknown dataset {dataset.id}, cannot update - did you intend to add it?"
            )
        elif existing.is_archived:
            raise ValueError(
                f"Dataset {dataset.id} is archived.  Please restore before updating."
            )
        can_update, safe_changes, unsafe_changes = self.can_update(
            dataset, updates_allowed
        )
        if not safe_changes and not unsafe_changes:
            self._update_locations(dataset, existing)
            _LOG.info("No metadata changes detected for dataset %s", dataset.id)
            return dataset

        for offset, old_val, new_val in safe_changes:
            _LOG.info(
                "Safe metadata changes in %s from %r to %r",
                _readable_offset(offset),
                old_val,
                new_val,
            )
        for offset, old_val, new_val in safe_changes:
            _LOG.warning(
                "Unsafe metadata changes in %s from %r to %r",
                _readable_offset(offset),
                old_val,
                new_val,
            )

        if not can_update:
            unsafe_txt = ", ".join(
                _readable_offset(offset) for offset, _, _ in unsafe_changes
            )
            raise ValueError(f"Unsafe metadata changes in {dataset.id}: {unsafe_txt}")

        # Apply update
        _LOG.info("Updating dataset %s", dataset.id)
        self._update_locations(dataset, existing)
        persistable = self.clone(dataset, for_save=True)
        self._by_id[dataset.id] = persistable
        self._active_by_id[dataset.id] = persistable
        if archive_less_mature is not None:
            _LOG.warning(
                "archive-less-mature functionality is not implemented for memory driver"
            )
        return cast(Dataset, self.get(dataset.id))

    def _update_locations(
        self, dataset: Dataset, existing: Dataset | None = None
    ) -> bool:
        skip_set: set[str | None] = {None}
        new_uris: list[str] = []
        if existing and existing.uris:
            for uri in existing.uris:
                skip_set.add(uri)
            if dataset.uris:
                new_uris = [uri for uri in dataset.uris if uri not in skip_set]
        if len(new_uris):
            _LOG.info(
                "Adding locations for dataset %s: %s", dataset.id, ", ".join(new_uris)
            )
        for uri in reversed(new_uris):
            self.add_location(dataset.id, uri)
        return len(new_uris) > 0

    @override
    def archive(self, ids: Iterable[DSID]) -> None:
        for id_ in ids:
            id_ = dsid_to_uuid(id_)
            if id_ in self._active_by_id:
                ds = self._active_by_id.pop(id_)
                self._by_product[ds.product.name].remove(ds.id)
                if ds.product.name not in self._archived_by_product:
                    self._archived_by_product[ds.product.name] = {ds.id}
                else:
                    self._archived_by_product[ds.product.name].add(ds.id)
                ds.archived_time = datetime.datetime.now()
                self._archived_by_id[id_] = ds

    @override
    def restore(self, ids: Iterable[DSID]) -> None:
        for id_ in ids:
            id_ = dsid_to_uuid(id_)
            if id_ in self._archived_by_id:
                ds = self._archived_by_id.pop(id_)
                ds.archived_time = None
                self._active_by_id[id_] = ds
                self._archived_by_product[ds.product.name].remove(ds.id)
                self._by_product[ds.product.name].add(ds.id)

    @override
    def purge(
        self, ids: Iterable[DSID], allow_delete_active: bool = False
    ) -> Sequence[DSID]:
        purged = []
        for id_ in ids:
            id_ = dsid_to_uuid(id_)
            if id_ in self._by_id:
                ds = self._by_id.pop(id_)
                if id_ in self._active_by_id:
                    if not allow_delete_active:
                        _LOG.warning(f"Cannot purge unarchived dataset: {id_}")
                        continue
                    del self._active_by_id[id_]
                    self._by_product[ds.product.name].remove(id_)
                if id_ in self._archived_by_id:
                    del self._archived_by_id[id_]
                    self._archived_by_product[ds.product.name].remove(id_)
                if id_ in self._derived_from:
                    for classifier, src_id in self._derived_from[id_].items():
                        del self._derivations[src_id][classifier]
                    del self._derived_from[id_]
                if id_ in self._derivations:
                    for classifier, child_id in self._derivations[id_].items():
                        del self._derived_from[child_id][classifier]
                    del self._derivations[id_]
                purged.append(id_)
        return purged

    @override
    def get_all_dataset_ids(self, archived: bool | None = False) -> Iterable[UUID]:
        if archived:
            return (id_ for id_ in self._archived_by_id.keys())
        elif archived is not None:
            return (id_ for id_ in self._active_by_id.keys())
        else:
            return (id_ for id_ in self._by_id.keys())

    @override
    @deprecat(
        reason="Multiple locations per dataset are now deprecated.  Please use the 'get_location' method.",
        version="1.9.0",
        category=ODC2DeprecationWarning,
    )
    def get_locations(self, id_: DSID) -> Iterable[str]:
        uuid = dsid_to_uuid(id_)
        return (s for s in self._locations[uuid])

    @override
    def get_location(self, id_: DSID) -> str | None:
        uuid = dsid_to_uuid(id_)
        locations = list(self._locations.get(uuid, []))
        if not locations:
            return None
        return locations[0]

    @override
    @deprecat(
        reason="Multiple locations per dataset are now deprecated. "
        "Archived locations may not be accessible in future releases.",
        version="1.9.0",
        category=ODC2DeprecationWarning,
    )
    def get_archived_locations(self, id_: DSID) -> Iterable[str]:
        uuid = dsid_to_uuid(id_)
        return (s for s, dt in self._archived_locations[uuid])

    @override
    @deprecat(
        reason="Multiple locations per dataset are now deprecated. "
        "Archived locations may not be accessible in future releases.",
        version="1.9.0",
        category=ODC2DeprecationWarning,
    )
    def get_archived_location_times(
        self, id_: DSID
    ) -> Iterable[tuple[str, datetime.datetime]]:
        uuid = dsid_to_uuid(id_)
        return ((s, dt) for s, dt in self._archived_locations[uuid])

    @override
    @deprecat(
        reason="Multiple locations per dataset are now deprecated. "
        "Dataset location can be set or updated with the update() method.",
        version="1.9.0",
        category=ODC2DeprecationWarning,
    )
    def add_location(self, id_: DSID, uri: str) -> bool:
        uuid = dsid_to_uuid(id_)
        if uuid not in self._by_id:
            warnings.warn(f"dataset {id_} is not an active dataset")
            return False
        if not uri:
            warnings.warn(f"Cannot add empty uri. (dataset {id_})")
            return False
        if uri in self._locations[uuid]:
            return False
        self._locations[uuid].append(uri)
        return True

    @override
    def get_datasets_for_location(
        self, uri: str, mode: str | None = None
    ) -> Iterable[Dataset]:
        if mode is None:
            mode = "exact" if uri.count("#") > 0 else "prefix"
        if mode not in ("exact", "prefix"):
            raise ValueError(f"Unsupported query mode: {mode}")
        ids: set[DSID] = set()
        if mode == "exact":
            test: Callable[[str], bool] = lambda l: l == uri  # noqa: E731,E741
        else:
            test = lambda l: l.startswith(uri)  # noqa: E741,E731
        for id_, locs in self._locations.items():
            for loc in locs:
                if test(loc):
                    ids.add(id_)
                    break
        return self.bulk_get(ids)

    @override
    @deprecat(
        reason="Multiple locations per dataset are now deprecated. "
        "Dataset location can be set or updated with the update() method.",
        version="1.9.0",
        category=ODC2DeprecationWarning,
    )
    def remove_location(self, id_: DSID, uri: str) -> bool:
        uuid = dsid_to_uuid(id_)
        removed = False
        if uuid in self._locations:
            old_locations = self._locations[uuid]
            new_locations = [loc for loc in old_locations if loc != uri]
            if len(new_locations) != len(old_locations):
                self._locations[uuid] = new_locations
                removed = True
        if not removed and uuid in self._archived_locations:
            archived_locations = self._archived_locations[uuid]
            new_archived_locations = [
                (loc, dt) for loc, dt in archived_locations if loc != uri
            ]
            if len(new_archived_locations) != len(archived_locations):
                self._archived_locations[uuid] = new_archived_locations
                removed = True
        return removed

    @override
    @deprecat(
        reason="Multiple locations per dataset are now deprecated. "
        "Archived locations may not be accessible in future releases. "
        "Dataset location can be set or updated with the update() method.",
        version="1.9.0",
        category=ODC2DeprecationWarning,
    )
    def archive_location(self, id_: DSID, uri: str) -> bool:
        uuid = dsid_to_uuid(id_)
        if uuid not in self._locations:
            return False
        old_locations = self._locations[uuid]
        new_locations = [loc for loc in old_locations if loc != uri]
        if len(new_locations) == len(old_locations):
            return False
        self._locations[uuid] = new_locations
        self._archived_locations[uuid].append((uri, datetime.datetime.now()))
        return True

    @override
    @deprecat(
        reason="Multiple locations per dataset are now deprecated. "
        "Archived locations may not be restorable in future releases. "
        "Dataset location can be set or updated with the update() method.",
        version="1.9.0",
        category=ODC2DeprecationWarning,
    )
    def restore_location(self, id_: DSID, uri: str) -> bool:
        uuid = dsid_to_uuid(id_)
        if uuid not in self._archived_locations:
            return False
        old_locations = self._archived_locations[uuid]
        new_locations = [(loc, dt) for loc, dt in old_locations if loc != uri]
        if len(new_locations) == len(old_locations):
            return False
        self._archived_locations[uuid] = new_locations
        self._locations[uuid].append(uri)
        return True

    @override
    def search_by_metadata(
        self, metadata: JsonDict, archived: bool | None = False
    ) -> Iterable[Dataset]:
        if archived:
            # True: Return archived datasets only
            dss: Iterable[Dataset] = self._archived_by_id.values()
        elif archived is not None:
            # False: Return active datasets only
            dss = self._active_by_id.values()
        else:
            # True: Return archived datasets only
            dss = chain(self._active_by_id.values(), self._archived_by_id.values())
        for ds in dss:
            if metadata_subset(metadata, ds.metadata_doc):
                yield ds

    RET_FORMAT_DATASETS = 0
    RET_FORMAT_PRODUCT_GROUPED = 1

    def _search(
        self,
        return_format: int,
        limit: int | None = None,
        source_filter: Mapping[str, QueryField] | None = None,
        archived: bool | None = False,
        **query: QueryField,
    ) -> Iterable[Dataset | tuple[Product, Iterable[Dataset]]]:
        if "geopolygon" in query:
            raise NotImplementedError(
                "Spatial search index API not supported by this index."
            )
        if source_filter:
            product_queries = list(self._get_prod_queries(**source_filter))
            if not product_queries:
                raise ValueError(f"No products match source filter: {source_filter}")
            if len(product_queries) > 1:
                raise RuntimeError(
                    "Multiproduct source_filters are not supported. Try adding 'product' field."
                )
            source_queries, source_product = product_queries[0]
            source_exprs = tuple(
                fields.to_expressions(
                    source_product.metadata_type.dataset_fields.get, **source_queries
                )
            )
        else:
            source_product = None
            source_exprs = ()
        product_queries = list(self._get_prod_queries(**query))
        if not product_queries:
            prod_name = query.get("product")
            if prod_name is None:
                raise ValueError(f"No products match search terms: {query}")
            else:
                raise ValueError(f"No such product: {prod_name}")

        matches = 0
        for q, product in product_queries:
            if limit is not None and matches >= limit:
                break
            query_exprs = tuple(
                fields.to_expressions(product.metadata_type.dataset_fields.get, **q)
            )
            product_results = []

            if archived is None:
                dsids: Iterable[UUID] = chain(
                    self._archived_by_product.get(product.name, set()),
                    self._by_product.get(product.name, set()),
                )
            elif archived:
                dsids = self._archived_by_product.get(product.name, set())
            else:
                dsids = self._by_product.get(product.name, set())

            for dsid in dsids:
                if limit is not None and matches >= limit:
                    break
                ds = cast(Dataset, self.get(dsid, include_sources=True))
                query_matches = True
                for expr in query_exprs:
                    if not expr.evaluate(ds.metadata_doc):
                        query_matches = False
                        break
                if not query_matches:
                    continue
                if source_product:
                    matching_source = None
                    for sds in cast(Mapping[str, Dataset], ds.sources).values():
                        if sds.product != source_product:
                            continue
                        source_matches = True
                        for expr in source_exprs:
                            if not expr.evaluate(sds.metadata_doc):
                                source_matches = False
                                break
                        if source_matches:
                            matching_source = sds
                            break
                    if not matching_source:
                        continue
                matches += 1
                if return_format == self.RET_FORMAT_DATASETS:
                    yield ds
                elif return_format == self.RET_FORMAT_PRODUCT_GROUPED:
                    product_results.append(ds)
            if return_format == self.RET_FORMAT_PRODUCT_GROUPED and product_results:
                yield product, product_results

    def _search_flat(
        self,
        limit: int | None = None,
        source_filter: Mapping[str, QueryField] | None = None,
        archived: bool | None = False,
        **query: QueryField,
    ) -> Iterable[Dataset]:
        return cast(
            Iterable[Dataset],
            self._search(
                return_format=self.RET_FORMAT_DATASETS,
                limit=limit,
                source_filter=source_filter,
                archived=archived,
                **query,
            ),
        )

    def _search_grouped(
        self,
        limit: int | None = None,
        source_filter: Mapping[str, QueryField] | None = None,
        archived: bool | None = False,
        **query: QueryField,
    ) -> Iterable[tuple[Product, Iterable[Dataset]]]:
        return cast(
            Iterable[tuple[Product, Iterable[Dataset]]],
            self._search(
                return_format=self.RET_FORMAT_PRODUCT_GROUPED,
                limit=limit,
                source_filter=source_filter,
                archived=archived,
                **query,
            ),
        )

    def _get_prod_queries(
        self, **query: QueryField
    ) -> Iterable[tuple[Mapping[str, QueryField], Product]]:
        return (
            (q, product) for product, q in self._index.products.search_robust(**query)
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
        source_filter: Mapping[str, QueryField] | None = None,
        archived: bool | None = False,
        order_by: Iterable[Any] | None = None,
        **query: QueryField,
    ) -> Iterable[Dataset]:
        if order_by:
            raise NotImplementedError(
                "order_by argument is not currently supported by the memory index driver."
            )
        return self._search_flat(
            limit=limit, source_filter=source_filter, archived=archived, **query
        )

    @override
    def search_by_product(
        self, archived: bool | None = False, **query: QueryField
    ) -> Iterable[tuple[Product, Iterable[Dataset]]]:
        return self._search_grouped(archived=archived, **query)  # type: ignore[arg-type]

    @override
    def search_returning(
        self,
        field_names: Iterable[str] | None = None,
        custom_offsets: Mapping[str, Offset] | None = None,
        limit: int | None = None,
        archived: bool | None = False,
        order_by: Iterable[Any] | None = None,
        **query: QueryField,
    ) -> Iterable[tuple]:
        if "geopolygon" in query:
            raise NotImplementedError(
                "Spatial search index API not supported by this index."
            )
        if order_by:
            raise NotImplementedError(
                "order_by argument is not currently supported by the memory index driver."
            )
        # Note that this implementation relies on dictionaries being ordered by insertion - this has been the case
        # since Py3.6, and officially guaranteed behaviour since Py3.7.
        if field_names is None and custom_offsets is None:
            field_name_d = dict.fromkeys(self._index.products.get_field_names())
        elif field_names:
            field_name_d = dict.fromkeys(field_names)
        else:
            field_name_d = {}

        if custom_offsets:
            custom_fields = build_custom_fields(custom_offsets)
            for name in custom_fields:
                field_name_d[name] = None
        else:
            custom_fields = {}

        #    Typing note: mypy can't handle dynamically created namedtuples
        result_type = namedtuple("search_result", field_name_d.keys())  # type: ignore[misc]
        for ds in self.search(limit=limit, archived=archived, **query):
            ds_fields = get_dataset_fields(ds.metadata_type.definition)
            ds_fields.update(custom_fields)
            result_vals = {
                fn: ds_fields[fn].extract(ds.metadata_doc) if fn in ds_fields else None
                for fn in field_name_d.keys()
            }
            yield result_type(**result_vals)

    @override
    def count(self, archived: bool | None = False, **query: QueryField) -> int:
        return len(list(self.search(archived=archived, **query)))

    @override
    def count_by_product(
        self, archived: bool | None = False, **query: QueryField
    ) -> Iterable[tuple[Product, int]]:
        for prod, datasets in self.search_by_product(archived=archived, **query):
            yield prod, len(list(datasets))

    @override
    def count_by_product_through_time(
        self, period: str, archived: bool | None = False, **query: QueryField
    ) -> Iterable[tuple[Product, Iterable[tuple[Range, int]]]]:
        return self._product_period_count(period, archived=archived, **query)  # type: ignore[arg-type]

    def _expand_period(
        self, period: str, begin: datetime.datetime, end: datetime.datetime
    ) -> Iterable[Range]:
        begin = tz_aware(begin)
        end = tz_aware(end)
        match = re.match(r"(?P<precision>[0-9]+) (?P<unit>day|month|week|year)", period)
        if not match:
            raise ValueError(
                "Invalid period string. Must specify a number of days, weeks, months or years"
            )
        precision = int(match.group("precision"))
        if precision <= 0:
            raise ValueError(
                "Invalid period string. Must specify a natural number of days, weeks, months or years"
            )
        unit = match.group("unit")

        def next_period(prev: datetime.datetime) -> datetime.datetime:
            if unit == "day":
                return prev + datetime.timedelta(days=precision)
            elif unit == "week":
                return prev + datetime.timedelta(days=precision * 7)
            elif unit == "year":
                return datetime.datetime(
                    prev.year + precision,
                    prev.month,
                    prev.day,
                    prev.hour,
                    prev.minute,
                    prev.second,
                    tzinfo=prev.tzinfo,
                )
            # unit == month
            year = prev.year
            month = prev.month
            month += precision
            while month > 12:
                month -= 12
                year += 1
            day = prev.day
            while True:
                try:
                    return datetime.datetime(
                        year,
                        month,
                        day,
                        prev.hour,
                        prev.minute,
                        prev.second,
                        tzinfo=prev.tzinfo,
                    )
                except ValueError:
                    day -= 1

        period_start = begin
        while period_start < end:
            period_end = next_period(period_start)
            yield Range(begin=period_start, end=period_end)
            period_start = period_end

    def _product_period_count(
        self,
        period: str,
        single_product_only: bool = False,
        archived: bool | None = False,
        **query: QueryField,
    ) -> Iterable[tuple[Product, Iterable[tuple[Range, int]]]]:
        YieldType = tuple[Product, Iterable[tuple[Range, int]]]  # noqa: N806
        query = dict(query)
        try:
            start, end = cast(Range, query.pop("time"))
        except KeyError:
            raise ValueError(
                'Must specify "time" range in period-counting query'
            ) from None
        periods = self._expand_period(period, start, end)
        last_product: YieldType | None = None
        for product, dss in self._search_grouped(archived=archived, **query):  # type: ignore[arg-type]
            if last_product and single_product_only:
                raise ValueError(
                    f"Multiple products match single query search: {query!r}"
                )
            if last_product:
                yield last_product
            period_counts = []
            for p in periods:
                count = 0
                for ds in dss:
                    if ranges_overlap(cast(Range, ds.time), p):
                        count += 1
                period_counts.append((p, count))
            retval = (product, period_counts)
            if last_product is not None:
                yield retval
            last_product = retval

        if last_product is None:
            raise ValueError(f"No products match search terms: {query!r}")
        else:
            yield last_product

    @override
    def count_product_through_time(
        self, period: str, archived: bool | None = False, **query: QueryField
    ) -> Iterable[tuple[Range, int]]:
        return next(
            iter(
                self._product_period_count(
                    period, archived=archived, single_product_only=True, **query
                )
            )
        )[1]

    @override
    @deprecat(
        reason="This method is deprecated and will be removed in 2.0.  "
        "Consider migrating to search_returning()",
        version="1.9.0",
        category=ODC2DeprecationWarning,
    )
    def search_summaries(self, **query: QueryField) -> Iterable[Mapping[str, Any]]:
        def make_summary(ds: Dataset) -> Mapping[str, Any]:
            fields = ds.metadata_type.dataset_fields
            return {
                field_name: field.extract(ds.metadata_doc)
                for field_name, field in fields.items()
            }

        for ds in self.search(**query):
            yield make_summary(ds)

    @override
    def spatial_extent(self, ids, crs=None):
        return None

    @override
    def temporal_extent(
        self, ids: Iterable[DSID]
    ) -> tuple[datetime.datetime, datetime.datetime]:
        min_time: datetime.datetime | None = None
        max_time: datetime.datetime | None = None
        if len(list(ids)) == 0:
            raise ValueError("no dataset ids provided")
        for dsid in ids:
            ds = self.get_unsafe(dsid)
            time_fld = ds.product.metadata_type.dataset_fields["time"]
            dsmin, dsmax = time_fld.extract(ds.metadata_doc)
            if dsmax is None and dsmin is None:
                continue
            elif dsmin is None:
                dsmin = dsmax
            elif dsmax is None:
                dsmax = dsmin
            if min_time is None or dsmin < min_time:
                min_time = dsmin
            if max_time is None or dsmax > max_time:
                max_time = dsmax
        return cast(datetime.datetime, min_time), cast(datetime.datetime, max_time)

    # pylint: disable=redefined-outer-name
    @override
    def search_returning_datasets_light(
        self,
        field_names: tuple[str, ...],
        custom_offsets: Mapping[str, Offset] | None = None,
        limit: int | None = None,
        archived: bool | None = False,
        **query: QueryField,
    ) -> Iterable[tuple]:
        if custom_offsets:
            custom_fields = build_custom_fields(custom_offsets)
        else:
            custom_fields = {}

        def make_ds_light(ds: Dataset) -> tuple:
            fields = {
                fname: ds.metadata_type.dataset_fields[fname] for fname in field_names
            }
            fields.update(custom_fields)
            #   Typing note: mypy cannot handle dynamically created namedtuples
            result_type = namedtuple("DatasetLight", list(fields.keys()))  # type: ignore[misc]
            if "grid_spatial" in fields:

                class DatasetLight(result_type, DatasetSpatialMixin):
                    pass
            else:

                class DatasetLight(result_type):  # type: ignore[no-redef]
                    __slots__ = ()

            fld_vals = {
                fname: field.extract(ds.metadata_doc) for fname, field in fields.items()
            }
            return DatasetLight(**fld_vals)

        for ds in self.search(limit=limit, archived=archived, **query):
            yield make_ds_light(ds)

    def clone(
        self, orig: Dataset, for_save: bool = False, lookup_locations: bool = True
    ) -> Dataset:
        if for_save:
            uris = []
        elif lookup_locations:
            uris = self._locations[orig.id].copy()
        elif orig.uris:
            uris = orig.uris.copy()
        else:
            uris = []
        if len(uris) == 1:
            kwargs: dict[str, Any] = {"uri": uris[0]}
        elif len(uris) > 1:
            kwargs = {"uris": uris}
        else:
            kwargs = {}
        return Dataset(
            product=self._index.products.clone(orig.product),
            metadata_doc=jsonify_document(orig.metadata_doc_without_lineage()),
            indexed_by="user"
            if for_save and orig.indexed_by is None
            else orig.indexed_by,
            indexed_time=datetime.datetime.now()
            if for_save and orig.indexed_time is None
            else orig.indexed_time,
            archived_time=None if for_save else orig.archived_time,
            **kwargs,
        )

    # Lineage methods need to be implemented on the dataset resource as that is where the relevant indexes
    # currently live.
    def _get_all_lineage(self) -> Iterable[LineageRelation]:
        for derived_id, sources in self._derived_from.items():
            for classifier, source_id in sources.items():
                yield LineageRelation(
                    derived_id=derived_id, source_id=source_id, classifier=classifier
                )

    def _add_lineage_batch(self, batch_rels: Iterable[LineageRelation]) -> BatchStatus:
        b_added = 0
        b_skipped = 0
        b_started = monotonic()
        for rel in batch_rels:
            if rel.derived_id in self._derived_from:
                if (
                    rel.classifier in self._derived_from[rel.derived_id]
                    and self._derived_from[rel.derived_id][rel.classifier]
                    != rel.source_id
                ):
                    b_skipped += 1
                    continue
                else:
                    self._derived_from[rel.derived_id][rel.classifier] = rel.source_id
                    b_added += 1
            else:
                self._derived_from[rel.derived_id] = {rel.classifier: rel.source_id}
                b_added += 1

            if rel.source_id in self._derivations:
                self._derivations[rel.source_id][rel.classifier] = rel.derived_id
            else:
                self._derivations[rel.source_id] = {rel.classifier: rel.derived_id}

        return BatchStatus(b_added, b_skipped, monotonic() - b_started)


class LineageResource(NoLineageResource):
    """
    Minimal implementation as does not support external lineage.
    Lineage indexes live in the Dataset resource, so thin wrapper around that.
    """

    @override
    def get_all_lineage(self, batch_size: int = 1000) -> Iterable[LineageRelation]:
        return self._index.datasets._get_all_lineage()

    @override
    def _add_batch(self, batch_rels: Iterable[LineageRelation]) -> BatchStatus:
        return self._index.datasets._add_lineage_batch(batch_rels)

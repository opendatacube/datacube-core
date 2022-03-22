# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2020 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
import datetime
import logging
import warnings
from typing import Iterable, List, Mapping, Optional, Tuple
from uuid import UUID

from datacube.index.abstract import AbstractDatasetResource, DSID, dsid_to_uuid
from datacube.index.memory._products import ProductResource
from datacube.model import Dataset, DatasetType
from datacube.utils import jsonify_document

_LOG = logging.getLogger(__name__)

class DatasetResource(AbstractDatasetResource):
    def __init__(self, product_resource: ProductResource) -> None:
        self.product_resource = product_resource
        self.metadata_type_resource = product_resource.metadata_type_resource
        # Main dataset index
        self.by_id: Mapping[UUID, Dataset] = {}
        # Indexes for active and archived datasets
        self.active_by_id: Mapping[UUID, Dataset] = {}
        self.archived_by_id: Mapping[UUID, Dataset] = {}
        # Lineage indexes:
        self.derived_from: Mapping[UUID, Mapping[str, UUID]] = {}
        self.derivations: Mapping[UUID, Mapping[str, UUID]] = {}
        # Location registers
        self.locations: Mapping[UUID, List[str]] = {}
        self.archived_locations: Mapping[UUID, List[Tuple[str, datetime.datetime]]] = {}

    def get(self, id_: DSID, include_sources: bool = False) -> Optional[Dataset]:
        try:
            ds = self.clone(self.by_id[dsid_to_uuid(id_)])
            if include_sources:
                ds.sources = {
                    classifier: self.get(dsid, include_sources=True)
                    for classifier, dsid in self.derived_from.get(ds.id, {}).items()
                }
            return ds
        except KeyError:
            return None

    def bulk_get(self, ids):
        return (ds for ds in (self.get(dsid) for dsid in ids) if ds is not None)

    def get_derived(self, id_):
        return (self.get(dsid) for dsid in self.derivations.get(dsid_to_uuid(id_), {}).values())

    def has(self, id_):
        return dsid_to_uuid(id_) in self.by_id

    def bulk_has(self, ids_):
        return (self.has(id_) for id_ in ids_)

    def add(self, dataset: Dataset,
            with_lineage: Optional[bool] = None,
            **kwargs) -> Dataset:
        if with_lineage is None:
            with_lineage = True
        _LOG.info('indexing %s', dataset.id)
        if with_lineage:
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
            self.by_id[persistable.id] = persistable
            self.active_by_id[persistable.id] = persistable
            if dataset.uris is not None:
                self.locations[persistable.id] = dataset.uris.copy()
            else:
                self.locations[persistable.id] = []
            self.archived_locations[persistable.id] = []
        return self.get(dataset.id)

    def persist_source_relationship(self, ds: Dataset, src: Dataset, classifier: str):
        # Add source lineage link
        if ds.id not in self.derived_from:
            self.derived_from[ds.id] = {}
        if self.derived_from[ds.id].get(classifier, src.id) != src.id:
            _LOG.warning("Dataset %s: Old %s dataset source %s getting overwritten by %s",
                         ds.id,
                         classifier,
                         self.derived_from[ds.id][classifier],
                         src.id)
        self.derived_from[ds.id][classifier] = src.id
        # Add source back-link
        if src.id not in self.derivations:
            self.derivations[src.id] = {}
        if self.derivations[src.id].get(classifier, ds.id) != ds.id:
            _LOG.warning("Dataset %s: Old %s dataset derivation %s getting overwritten by %s",
                         src.id,
                         classifier,
                         self.derivations[src.id][classifier],
                         ds.id)
        self.derivations[src.id][classifier] = ds.id

    def search_product_duplicates(self, product: DatasetType, *args):
        return []

    def can_update(self, dataset, updates_allowed=None):
        raise NotImplementedError()

    def update(self, dataset: Dataset, updates_allowed=None):
        raise NotImplementedError()

    def archive(self, ids: Iterable[DSID]) -> None:
        for id_ in ids:
            id_ = dsid_to_uuid(id_)
            if id_ in self.active_by_id:
                ds = self.active_by_id.pop(id_)
                ds.archived_time = datetime.datetime.now()
                self.archived_by_id[id_] = ds

    def restore(self, ids: Iterable[DSID]) -> None:
        for id_ in ids:
            id_ = dsid_to_uuid(id_)
            if id_ in self.archived_by_id:
                ds = self.archived_by_id.pop(id_)
                ds.archived_time = None
                self.active_by_id[id_] = ds

    def purge(self, ids: Iterable[DSID]):
        for id_ in ids:
            id_ = dsid_to_uuid(id_)
            if id_ in self.archived_by_id:
                del self.archived_by_id[id_]
                del self.by_id[id_]
                if id_ in self.derived_from:
                    for classifier, src_id in self.derived_from[id_].items():
                        del self.derivations[src_id][classifier]
                    del self.derived_from[id_]
                if id_ in self.derivations:
                    for classifier, child_id in self.derivations[id_].items():
                        del self.derived_from[child_id][classifier]
                    del self.derivations[id_]

    def get_all_dataset_ids(self, archived: bool) -> Iterable[UUID]:
        if archived:
            return (id_ for id_ in self.archived_by_id.keys())
        else:
            return (id_ for id_ in self.active_by_id.keys())

    def get_field_names(self, product_name=None) -> Iterable[str]:
        return []

    def get_locations(self, id_: DSID) -> Iterable[str]:
        uuid = dsid_to_uuid(id_)
        return (s for s in self.locations[uuid])

    def get_archived_locations(self, id_: DSID) -> Iterable[str]:
        uuid = dsid_to_uuid(id_)
        return (s for s, dt in self.archived_locations[uuid])

    def get_archived_location_times(self, id_: DSID) -> Iterable[Tuple[str, datetime.datetime]]:
        uuid = dsid_to_uuid(id_)
        return ((s,dt) for s, dt in self.archived_locations[uuid])

    def add_location(self, id_: DSID, uri: str) -> bool:
        uuid = dsid_to_uuid(id_)
        if uuid not in self.by_id:
            warnings.warn(f"dataset {id_} is not an active dataset")
            return False
        if not uri:
            warnings.warn(f"Cannot add empty uri. (dataset {id_})")
            return False
        if uri in self.locations[uuid]:
            return False
        self.locations[uuid].append(uri)
        return True

    def get_datasets_for_location(self, uri: str, mode : Optional[str]=None) -> Iterable[Dataset]:
        return []

    def remove_location(self, id_: DSID, uri: str) -> bool:
        uuid = dsid_to_uuid(id_)
        if uuid in self.locations:
            old_locations = self.locations[uuid]
            new_locations = [loc for loc in old_locations if loc != uri]
            if len(new_locations) == len(old_locations):
                return False
            self.locations[uuid] = new_locations
            return True
        if uuid in self.archived_locations:
            old_locations = self.archived_locations[uuid]
            new_locations = [(loc, dt) for loc, dt in old_locations if loc != uri]
            if len(new_locations) == len(old_locations):
                return False
            self.archived_locations[uuid] = new_locations
            return True
        return False

    def archive_location(self, id_: DSID, uri: str) -> bool:
        uuid = dsid_to_uuid(id_)
        if uuid not in self.locations:
            return False
        old_locations = self.locations[uuid]
        new_locations = [loc for loc in old_locations if loc != uri]
        if len(new_locations) == len(old_locations):
            return False
        self.locations[uuid] = new_locations
        self.archived_locations[uuid].append((uri, datetime.datetime.now()))
        return True

    def restore_location(self, id_: DSID, uri: str) -> bool:
        uuid = dsid_to_uuid(id_)
        if uuid not in self.archived_locations:
            return False
        old_locations = self.archived_locations[uuid]
        new_locations = [(loc, dt) for loc, dt in old_locations if loc != uri]
        if len(new_locations) == len(old_locations):
            return False
        self.archived_locations[uuid] = new_locations
        self.locations[uuid].append(uri)
        return True

    def search_by_metadata(self, metadata):
        return []

    def search(self, limit=None, **query):
        return []

    def search_by_product(self, **query):
        return []

    def search_returning(self, field_names, limit=None, **query):
        return []

    def count(self, **query):
        return 0

    def count_by_product(self, **query):
        return []

    def count_by_product_through_time(self, period, **query):
        return []

    def count_product_through_time(self, period, **query):
        return []

    def search_summaries(self, **query):
        return []

    def get_product_time_bounds(self, product: str):
        raise NotImplementedError()

    # pylint: disable=redefined-outer-name
    def search_returning_datasets_light(self, field_names: tuple, custom_offsets=None, limit=None, **query):
        return []

    def clone(self, orig: Dataset, for_save=False, lookup_locations=True) -> Dataset:
        if for_save:
            uris = []
        elif lookup_locations:
            uris = self.locations[orig.id].copy()
        else:
            uris = orig.uris.copy()
        return Dataset(
            type_=self.product_resource.clone(orig.type),
            metadata_doc=jsonify_document(orig.metadata_doc_without_lineage()),
            uris=uris,
            indexed_by="user" if for_save and orig.indexed_by is None else orig.indexed_by,
            indexed_time=datetime.datetime.now() if for_save and orig.indexed_time is None else orig.indexed_time,
            archived_time=None if for_save else orig.archived_time
        )

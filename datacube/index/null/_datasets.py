# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2024 ODC Contributors
# SPDX-License-Identifier: Apache-2.0

import datetime
from deprecat import deprecat

from datacube.migration import ODC2DeprecationWarning
from datacube.index.abstract import AbstractDatasetResource, DSID
from datacube.model import Dataset, Product
from typing import Iterable, Optional


class DatasetResource(AbstractDatasetResource):
    def __init__(self, index):
        super().__init__(index)

    def get_unsafe(self, id_: DSID, include_sources: bool = False, include_deriveds: bool = False, max_depth: int = 0):
        raise KeyError(id_)

    def bulk_get(self, ids):
        return []

    def get_derived(self, id_):
        return []

    def has(self, id_):
        return False

    def bulk_has(self, ids_):
        return [False for id_ in ids_]

    def add(self, dataset: Dataset,
            with_lineage: bool = True,
            archive_less_mature: Optional[int] = None) -> Dataset:
        raise NotImplementedError()

    def search_product_duplicates(self, product: Product, *args):
        return []

    def can_update(self, dataset, updates_allowed=None):
        raise NotImplementedError()

    def update(self, dataset: Dataset, updates_allowed=None, archive_less_mature=None):
        raise NotImplementedError()

    def archive(self, ids):
        raise NotImplementedError()

    def restore(self, ids):
        raise NotImplementedError()

    def purge(self, ids: Iterable[DSID]):
        raise NotImplementedError()

    def get_all_dataset_ids(self, archived: bool):
        return []

    @deprecat(
        reason="Multiple locations per dataset are now deprecated.  Please use the 'get_location' method.",
        version="1.9.0",
        category=ODC2DeprecationWarning
    )
    def get_locations(self, id_):
        return []

    def get_location(self, id_):
        return None

    @deprecat(
        reason="Multiple locations per dataset are now deprecated. "
               "Archived locations may not be accessible in future releases.",
        version="1.9.0",
        category=ODC2DeprecationWarning
    )
    def get_archived_locations(self, id_):
        return []

    @deprecat(
        reason="Multiple locations per dataset are now deprecated. "
               "Archived locations may not be accessible in future releases.",
        version="1.9.0",
        category=ODC2DeprecationWarning
    )
    def get_archived_location_times(self, id_):
        return []

    @deprecat(
        reason="Multiple locations per dataset are now deprecated. "
               "Dataset location can be set or updated with the update() method.",
        version="1.9.0",
        category=ODC2DeprecationWarning
    )
    def add_location(self, id_, uri):
        raise NotImplementedError()

    def get_datasets_for_location(self, uri, mode=None):
        return []

    @deprecat(
        reason="Multiple locations per dataset are now deprecated. "
               "Dataset location can be set or updated with the update() method.",
        version="1.9.0",
        category=ODC2DeprecationWarning
    )
    def remove_location(self, id_, uri):
        raise NotImplementedError()

    @deprecat(
        reason="Multiple locations per dataset are now deprecated. "
               "Archived locations may not be accessible in future releases. "
               "Dataset location can be set or updated with the update() method.",
        version="1.9.0",
        category=ODC2DeprecationWarning
    )
    def archive_location(self, id_, uri):
        raise NotImplementedError()

    @deprecat(
        reason="Multiple locations per dataset are now deprecated. "
               "Archived locations may not be restorable in future releases. "
               "Dataset location can be set or updated with the update() method.",
        version="1.9.0",
        category=ODC2DeprecationWarning
    )
    def restore_location(self, id_, uri):
        raise NotImplementedError()

    def search_by_metadata(self, metadata, archived=False):
        return []

    @deprecat(
        deprecated_args={
            "source_filter": {
                "reason": "Filtering by source metadata is deprecated and will be removed in future.",
                "version": "1.9.0",
                "category": ODC2DeprecationWarning

            }
        }
    )
    def search(self, limit=None, archived=False, **query):
        return []

    def search_by_product(self, archived=False, **query):
        return []

    def search_returning(self,
                         field_names=None, custom_offsets=None,
                         limit=None, archived=False, order_by=None,
                         **query):
        return []

    def count(self, archived=False, **query):
        return 0

    def count_by_product(self, archived=False, **query):
        return []

    def count_by_product_through_time(self, period, archived=False, **query):
        return []

    def count_product_through_time(self, period, archived=False, **query):
        return []

    @deprecat(
        reason="This method is deprecated and will be removed in 2.0.  "
               "Consider migrating to search_returning()",
        version="1.9.0",
        category=ODC2DeprecationWarning
    )
    def search_summaries(self, **query):
        return []

    def temporal_extent(self, ids: Iterable[DSID]) -> tuple[datetime.datetime, datetime.datetime]:
        raise KeyError(str(ids))

    # pylint: disable=redefined-outer-name
    def search_returning_datasets_light(self,
                                        field_names: tuple,
                                        custom_offsets=None, limit=None, archived=False, **query):
        return []

    def spatial_extent(self, ids=None, product=None, crs=None):
        return None

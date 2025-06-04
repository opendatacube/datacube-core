# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2025 ODC Contributors
# SPDX-License-Identifier: Apache-2.0

import datetime
from collections.abc import Iterable, Sequence

from deprecat import deprecat
from typing_extensions import override

from datacube.index.abstract import DSID, AbstractDatasetResource
from datacube.migration import ODC2DeprecationWarning
from datacube.model import Dataset, Product, QueryField


class DatasetResource(AbstractDatasetResource):
    def __init__(self, index) -> None:
        super().__init__(index)

    @override
    def get_unsafe(
        self,
        id_: DSID,
        include_sources: bool = False,
        include_deriveds: bool = False,
        max_depth: int = 0,
    ):
        raise KeyError(id_)

    @override
    def bulk_get(self, ids) -> list:
        return []

    @override
    def get_derived(self, id_) -> list:
        return []

    @override
    def has(self, id_) -> bool:
        return False

    @override
    def bulk_has(self, ids_) -> list:
        return [False for id_ in ids_]

    @override
    def add(
        self,
        dataset: Dataset,
        with_lineage: bool = True,
        archive_less_mature: int | None = None,
    ) -> Dataset:
        raise NotImplementedError()

    @override
    def search_product_duplicates(self, product: Product, *args) -> list:
        return []

    @override
    def can_update(
        self, dataset, updates_allowed=None
    ) -> tuple[bool, Iterable, Iterable]:
        raise NotImplementedError()

    @override
    def update(self, dataset: Dataset, updates_allowed=None, archive_less_mature=None):
        raise NotImplementedError()

    @override
    def archive(self, ids):
        raise NotImplementedError()

    @override
    def restore(self, ids):
        raise NotImplementedError()

    @override
    def purge(self, ids: Iterable[DSID], allow_delete_active: bool = False) -> Sequence:
        raise NotImplementedError()

    @override
    def get_all_dataset_ids(self, archived: bool) -> list:
        return []

    @deprecat(
        reason="Multiple locations per dataset are now deprecated.  Please use the 'get_location' method.",
        version="1.9.0",
        category=ODC2DeprecationWarning,
    )
    @override
    def get_locations(self, id_) -> list:
        return []

    @override
    def get_location(self, id_) -> None:
        return None

    @deprecat(
        reason="Multiple locations per dataset are now deprecated. "
        "Archived locations may not be accessible in future releases.",
        version="1.9.0",
        category=ODC2DeprecationWarning,
    )
    @override
    def get_archived_locations(self, id_) -> list:
        return []

    @deprecat(
        reason="Multiple locations per dataset are now deprecated. "
        "Archived locations may not be accessible in future releases.",
        version="1.9.0",
        category=ODC2DeprecationWarning,
    )
    @override
    def get_archived_location_times(self, id_) -> list:
        return []

    @deprecat(
        reason="Multiple locations per dataset are now deprecated. "
        "Dataset location can be set or updated with the update() method.",
        version="1.9.0",
        category=ODC2DeprecationWarning,
    )
    @override
    def add_location(self, id_, uri) -> bool:
        raise NotImplementedError()

    @override
    def get_datasets_for_location(self, uri, mode=None) -> list:
        return []

    @deprecat(
        reason="Multiple locations per dataset are now deprecated. "
        "Dataset location can be set or updated with the update() method.",
        version="1.9.0",
        category=ODC2DeprecationWarning,
    )
    @override
    def remove_location(self, id_, uri) -> bool:
        raise NotImplementedError()

    @deprecat(
        reason="Multiple locations per dataset are now deprecated. "
        "Archived locations may not be accessible in future releases. "
        "Dataset location can be set or updated with the update() method.",
        version="1.9.0",
        category=ODC2DeprecationWarning,
    )
    @override
    def archive_location(self, id_, uri) -> bool:
        raise NotImplementedError()

    @deprecat(
        reason="Multiple locations per dataset are now deprecated. "
        "Archived locations may not be restorable in future releases. "
        "Dataset location can be set or updated with the update() method.",
        version="1.9.0",
        category=ODC2DeprecationWarning,
    )
    @override
    def restore_location(self, id_, uri) -> bool:
        raise NotImplementedError()

    @override
    def search_by_metadata(self, metadata, archived: bool | None = False) -> list:
        return []

    @deprecat(
        deprecated_args={
            "source_filter": {
                "reason": "Filtering by source metadata is deprecated and will be removed in future.",
                "version": "1.9.0",
                "category": ODC2DeprecationWarning,
            }
        }
    )
    @override
    def search(
        self,
        limit: int | None = None,
        archived: bool | None = False,
        order_by=None,
        **query,
    ) -> list:
        return []

    @override
    def search_by_product(
        self, archived: bool | None = False, **query: QueryField
    ) -> Iterable[tuple[Product, Iterable[Dataset]]]:
        return []

    @override
    def search_returning(
        self,
        field_names: Iterable[str] | None = None,
        custom_offsets=None,
        limit: int | None = None,
        archived: bool | None = False,
        order_by=None,
        **query,
    ) -> list:
        return []

    @override
    def count(self, archived: bool | None = False, **query) -> int:
        return 0

    @override
    def count_by_product(self, archived: bool | None = False, **query) -> list:
        return []

    @override
    def count_by_product_through_time(
        self, period: str, archived: bool | None = False, **query
    ) -> list:
        return []

    @override
    def count_product_through_time(
        self, period: str, archived: bool | None = False, **query
    ) -> list:
        return []

    @deprecat(
        reason="This method is deprecated and will be removed in 2.0.  "
        "Consider migrating to search_returning()",
        version="1.9.0",
        category=ODC2DeprecationWarning,
    )
    @override
    def search_summaries(self, **query) -> list:
        return []

    @override
    def temporal_extent(
        self, ids: Iterable[DSID]
    ) -> tuple[datetime.datetime, datetime.datetime]:
        raise KeyError(str(ids))

    # pylint: disable=redefined-outer-name
    @override
    def search_returning_datasets_light(
        self,
        field_names: tuple,
        custom_offsets=None,
        limit: int | None = None,
        archived: bool | None = False,
        **query,
    ) -> list:
        return []

    @override
    def spatial_extent(self, ids=None, product=None, crs=None):
        return None

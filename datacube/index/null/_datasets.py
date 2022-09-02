# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2020 ODC Contributors
# SPDX-License-Identifier: Apache-2.0

from datacube.index.abstract import AbstractDatasetResource, DSID
from datacube.model import Dataset, DatasetType
from typing import Iterable


class DatasetResource(AbstractDatasetResource):
    def __init__(self, product_resource):
        self.types = product_resource

    def get(self, id_: DSID, include_sources=False):
        return None

    def bulk_get(self, ids):
        return []

    def get_derived(self, id_):
        return []

    def has(self, id_):
        return False

    def bulk_has(self, ids_):
        return [False for id_ in ids_]

    def add(self, dataset: Dataset,
            with_lineage: bool = True) -> Dataset:
        raise NotImplementedError()

    def search_product_duplicates(self, product: DatasetType, *args):
        return []

    def can_update(self, dataset, updates_allowed=None):
        raise NotImplementedError()

    def update(self, dataset: Dataset, updates_allowed=None):
        raise NotImplementedError()

    def archive(self, ids):
        raise NotImplementedError()

    def restore(self, ids):
        raise NotImplementedError()

    def purge(self, ids: Iterable[DSID]):
        raise NotImplementedError()

    def get_all_dataset_ids(self, archived: bool):
        return []

    def get_field_names(self, product_name=None):
        return []

    def get_locations(self, id_):
        return []

    def get_archived_locations(self, id_):
        return []

    def get_archived_location_times(self, id_):
        return []

    def add_location(self, id_, uri):
        raise NotImplementedError()

    def get_datasets_for_location(self, uri, mode=None):
        return []

    def remove_location(self, id_, uri):
        raise NotImplementedError()

    def archive_location(self, id_, uri):
        raise NotImplementedError()

    def restore_location(self, id_, uri):
        raise NotImplementedError()

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

    def spatial_extent(self, ids, crs=None):
        return None

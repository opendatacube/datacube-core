# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2025 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
import datetime
import logging
from abc import ABC, abstractmethod
from collections.abc import Iterable, Mapping, Sequence
from datetime import timedelta
from time import monotonic
from typing import Any
from uuid import UUID

from deprecat import deprecat
from odc.geo import CRS, Geometry

from datacube.migration import ODC2DeprecationWarning
from datacube.model import Dataset, Field, Product, QueryDict, QueryField, Range
from datacube.utils import report_to_user
from datacube.utils.changes import AllowPolicy, Change, DocumentMismatchError, Offset
from datacube.utils.documents import JsonDict

from ._types import DSID, BatchStatus, DatasetTuple

_LOG: logging.Logger = logging.getLogger(__name__)


class AbstractDatasetResource(ABC):
    """
    Abstract base class for the Dataset portion of an index api.

    All DatasetResource implementations should inherit from this base
    class and implement all abstract methods.

    (If a particular abstract method is not applicable for a particular implementation
    raise a NotImplementedError)
    """

    def __init__(self, index) -> None:
        self._index = index
        self.products = self._index.products
        self.types = self.products  # types is compatibility alias for products

    @abstractmethod
    def get_unsafe(
        self,
        id_: DSID,
        include_sources: bool = False,
        include_deriveds: bool = False,
        max_depth: int = 0,
    ) -> Dataset:
        """
        Get dataset by id (Raises KeyError if id_ does not exist)

        - Index drivers supporting the legacy lineage API:

        :param id_: id of the dataset to retrieve
        :param include_sources: include the full provenance tree of the dataset.


        - Index drivers supporting the external lineage API:

        :param id_: id of the dataset to retrieve
        :param include_sources: include the full provenance tree for the dataset.
        :param include_deriveds: include the full derivative tree for the dataset.
        :param max_depth: The maximum depth of the source and/or derived tree.  Defaults to 0, meaning no limit.
        """

    def get(
        self,
        id_: DSID,
        include_sources: bool = False,
        include_deriveds: bool = False,
        max_depth: int = 0,
    ) -> Dataset | None:
        """
        Get dataset by id (Return None if ``id_`` does not exist).

        - Index drivers supporting the legacy lineage API:

        :param id_: id of the dataset to retrieve
        :param include_sources: include the full provenance tree of the dataset.


        - Index drivers supporting the external lineage API:

        :param id_: id of the dataset to retrieve
        :param include_sources: include the full provenance tree for the dataset.
        :param include_deriveds: include the full derivative tree for the dataset.
        :param max_depth: The maximum depth of the source and/or derived tree.  Defaults to 0, meaning no limit.
        """
        try:
            return self.get_unsafe(id_, include_sources, include_deriveds, max_depth)
        except KeyError:
            return None

    def _check_get_legacy(
        self, include_deriveds: bool = False, max_depth: int = 0
    ) -> None:
        """
        Index drivers implementing the legacy lineage API can call this method to check get arguments
        """
        if not self._index.supports_external_lineage:
            if include_deriveds:
                raise NotImplementedError(
                    "This index driver only supports the legacy lineage data - include_deriveds not supported."
                )
            if not self._index.supports_external_lineage and (
                include_deriveds or max_depth > 0
            ):
                raise NotImplementedError(
                    "This index driver only supports the legacy lineage data - max_depth not supported."
                )

    @abstractmethod
    def bulk_get(self, ids: Iterable[DSID]) -> Iterable[Dataset]:
        """
        Get multiple datasets by id. (Lineage sources NOT included)

        :param ids: ids to retrieve
        :return: Iterable of Dataset models
        """

    @deprecat(
        reason="The 'get_derived' static method is deprecated in favour of the new lineage API.",
        version="1.9.0",
        category=ODC2DeprecationWarning,
    )
    @abstractmethod
    def get_derived(self, id_: DSID) -> Iterable[Dataset]:
        """
        Get all datasets derived from a dataset (NOT recursive)

        :param id_: dataset id
        """

    @abstractmethod
    def has(self, id_: DSID) -> bool:
        """
        Is this dataset in this index?

        :param id_: dataset id
        :return: True if the dataset exists in this index
        """

    @abstractmethod
    def bulk_has(self, ids_: Iterable[DSID]) -> Iterable[bool]:
        """
        Like `has` but operates on a multiple ids.

        For every supplied id check if database contains a dataset with that id.

        :param ids_: iterable of dataset ids to check existence in index

        :return: Iterable of bools, true for datasets that exist in index
        """

    @abstractmethod
    def add(
        self,
        dataset: Dataset,
        with_lineage: bool = True,
        archive_less_mature: int | None = None,
    ) -> Dataset:
        """
        Add ``dataset`` to the index. No-op if it is already present.

        :param dataset: Unpersisted dataset model

        :param with_lineage:
           - ``True (default)`` attempt adding lineage datasets if missing
           - ``False`` record lineage relations, but do not attempt
             adding lineage datasets to the db

        :param archive_less_mature: if integer, search for less
            mature versions of the dataset with the int value as a millisecond
            delta in timestamp comparison

        :return: Persisted Dataset model
        """

    @abstractmethod
    def search_product_duplicates(
        self, product: Product, *args: str | Field
    ) -> Iterable[tuple[tuple, Iterable[UUID]]]:
        """
        Find dataset ids who have duplicates of the given set of field names.

        (Search is always restricted by Product)

        Returns a generator returning a tuple containing a namedtuple of
        the values of the supplied fields, and the datasets that match those
        values.

        :param product: The Product to restrict search to
        :param args: field names to identify duplicates over
        """

    @abstractmethod
    def can_update(
        self,
        dataset: Dataset,
        updates_allowed: Mapping[Offset, AllowPolicy] | None = None,
    ) -> tuple[bool, Iterable[Change], Iterable[Change]]:
        """
        Check if dataset can be updated. Return bool,safe_changes,unsafe_changes

        :param dataset: Dataset to update
        :param updates_allowed: Allowed updates
        :return: Tuple of: boolean (can/can't update); safe changes; unsafe changes
        """

    @abstractmethod
    def update(
        self,
        dataset: Dataset,
        updates_allowed: Mapping[Offset, AllowPolicy] | None = None,
        archive_less_mature: int | None = None,
    ) -> Dataset:
        """
        Update dataset metadata and location
        :param dataset: Dataset model with unpersisted updates
        :param updates_allowed: Allowed updates
        :param archive_less_mature: Find and archive less mature datasets with ms delta
        :return: Persisted dataset model
        """

    @abstractmethod
    def archive(self, ids: Iterable[DSID]) -> None:
        """
        Mark datasets as archived

        :param ids: list of dataset ids to archive
        """

    def archive_less_mature(self, ds: Dataset, delta: int | bool = 500) -> None:
        """
        Archive less mature versions of a dataset

        :param ds: dataset to search
        :param delta: millisecond delta for time range.
            If True, default to 500ms. If False, do not find or archive less mature datasets.
            Bool value accepted only for improving backwards compatibility, int preferred.
        """
        less_mature = self.find_less_mature(ds, delta)
        less_mature_ids = (x.id for x in less_mature)

        self.archive(less_mature_ids)
        for lm_ds in less_mature_ids:
            _LOG.info(f"Archived less mature dataset: {lm_ds}")

    def find_less_mature(
        self, ds: Dataset, delta: int | bool = 500
    ) -> Iterable[Dataset]:
        """
        Find less mature versions of a dataset

        :param ds: Dataset to search
        :param delta: millisecond delta for time range.
            If True, default to 500ms. If None or False, do not find or archive less mature datasets.
            Bool value accepted only for improving backwards compatibility, int preferred.
        :return: Iterable of less mature datasets
        """
        if isinstance(delta, bool):
            _LOG.warning("received delta as a boolean value. Int is preferred")
            if delta is True:  # treat True as default
                delta = 500
            else:  # treat False the same as None
                return []
        elif isinstance(delta, int):
            if delta < 0:
                raise ValueError("timedelta must be a positive integer")
        elif delta is None:
            return []
        else:
            raise TypeError("timedelta must be None, a positive integer, or a boolean")

        def check_maturity_information(dataset, props):
            # check that the dataset metadata includes all maturity-related properties
            # passing in the required props to enable greater extensibility should it be needed
            for prop in props:
                if hasattr(dataset.metadata, prop) and (
                    getattr(dataset.metadata, prop) is not None
                ):
                    return
                raise ValueError(
                    f"Dataset {dataset.id} is missing property {prop} required for maturity check"
                )

        check_maturity_information(ds, ["region_code", "time", "dataset_maturity"])

        # 'expand' the date range by `delta` milliseconds to give a bit more leniency in datetime comparison
        expanded_time_range = Range(
            ds.metadata.time.begin - timedelta(milliseconds=delta),
            ds.metadata.time.end + timedelta(milliseconds=delta),
        )
        dupes = self.search(
            product=ds.product.name,
            region_code=ds.metadata.region_code,
            time=expanded_time_range,
        )

        less_mature = []
        for dupe in dupes:
            if dupe.id == ds.id:
                continue

            # only need to check that dupe has dataset maturity, missing/null region_code and time
            # would already have been filtered out during the search query
            check_maturity_information(dupe, ["dataset_maturity"])

            if dupe.metadata.dataset_maturity == ds.metadata.dataset_maturity:
                # Duplicate has the same maturity, which one should be archived is unclear
                raise ValueError(
                    f"A dataset with the same maturity as dataset {ds.id} already exists, "
                    f"with id: {dupe.id}"
                )

            if dupe.metadata.dataset_maturity < ds.metadata.dataset_maturity:
                # Duplicate is more mature than dataset
                # Note that "final" < "nrt"
                raise ValueError(
                    f"A more mature version of dataset {ds.id} already exists, with id: "
                    f"{dupe.id} and maturity: {dupe.metadata.dataset_maturity}"
                )

            less_mature.append(dupe)
        return less_mature

    @abstractmethod
    def restore(self, ids: Iterable[DSID]) -> None:
        """
        Mark datasets as not archived

        :param ids: list of dataset ids to restore
        """

    @abstractmethod
    def purge(
        self, ids: Iterable[DSID], allow_delete_active: bool = False
    ) -> Sequence[DSID]:
        """
        Delete datasets

        :param ids: iterable of dataset ids to purge
        :param allow_delete_active: if false, only archived datasets can be deleted
        :return: list of purged dataset ids
        """

    @abstractmethod
    def get_all_dataset_ids(self, archived: bool) -> Iterable[UUID]:
        """
        Get all dataset IDs based only on archived status

        This will be very slow and inefficient for large databases, and is really
        only intended for small and/or experimental databases.

        :param archived: If true, return all archived datasets, if false, all unarchived datasets
        :return: Iterable of dataset ids
        """

    @deprecat(
        reason="This method has been moved to the Product resource (i.e. dc.index.products.get_field_names)",
        version="1.9.0",
        category=ODC2DeprecationWarning,
    )
    def get_field_names(self, product_name: str | None = None) -> Iterable[str]:
        """
        Get the list of possible search fields for a Product (or all products)

        :param product_name: Name of product, or None for all products
        :return: All possible search field names
        """
        return self._index.products.get_field_names(product_name)

    @deprecat(
        reason="Multiple locations per dataset are now deprecated.  Please use the 'get_location' method.",
        version="1.9.0",
        category=ODC2DeprecationWarning,
    )
    @abstractmethod
    def get_locations(self, id_: DSID) -> Iterable[str]:
        """
        Get (active) storage locations for the given dataset id

        :param id_: dataset id
        :return: Storage locations for the dataset
        """

    @abstractmethod
    def get_location(self, id_: DSID) -> str | None:
        """
        Get (active) storage location for the given dataset id

        :param id_: dataset id
        :return: Storage location for the dataset - None if no location for the id_, or if id_ not in db.
        """

    @deprecat(
        reason="Multiple locations per dataset are now deprecated. "
        "Archived locations may not be accessible in future releases.",
        version="1.9.0",
        category=ODC2DeprecationWarning,
    )
    @abstractmethod
    def get_archived_locations(self, id_: DSID) -> Iterable[str]:
        """
        Get archived locations for a dataset

        :param id_: dataset id
        :return: Archived storage locations for the dataset
        """

    @deprecat(
        reason="Multiple locations per dataset are now deprecated. "
        "Archived locations may not be accessible in future releases.",
        version="1.9.0",
        category=ODC2DeprecationWarning,
    )
    @abstractmethod
    def get_archived_location_times(
        self, id_: DSID
    ) -> Iterable[tuple[str, datetime.datetime]]:
        """
        Get each archived location along with the time it was archived.

        :param id_: dataset id
        :return: Archived storage locations, with archive date.
        """

    @deprecat(
        reason="Multiple locations per dataset are now deprecated. "
        "Dataset location can be set or updated with the update() method.",
        version="1.9.0",
        category=ODC2DeprecationWarning,
    )
    @abstractmethod
    def add_location(self, id_: DSID, uri: str) -> bool:
        """
        Add a location to the dataset if it doesn't already exist.

        :param id_: dataset id
        :param uri: fully qualified uri
        :return: True if a location was added, false if location already existed
        """

    @abstractmethod
    def get_datasets_for_location(
        self, uri: str, mode: str | None = None
    ) -> Iterable[Dataset]:
        """
        Find datasets that exist at the given URI

        :param uri: search uri
        :param mode: 'exact', 'prefix' or None (to guess)
        :return: Matching dataset models
        """

    @deprecat(
        reason="Multiple locations per dataset are now deprecated. "
        "Dataset location can be set or updated with the update() method.",
        version="1.9.0",
        category=ODC2DeprecationWarning,
    )
    @abstractmethod
    def remove_location(self, id_: DSID, uri: str) -> bool:
        """
        Remove a location from the dataset if it exists.

        :param id_: dataset id
        :param uri: fully qualified uri
        :return: True if location was removed, false if it didn't exist for the database
        """

    @deprecat(
        reason="Multiple locations per dataset are now deprecated. "
        "Archived locations may not be accessible in future releases. "
        "Dataset location can be set or updated with the update() method.",
        version="1.9.0",
        category=ODC2DeprecationWarning,
    )
    @abstractmethod
    def archive_location(self, id_: DSID, uri: str) -> bool:
        """
        Archive a location of the dataset if it exists and is active.

        :param id_: dataset id
        :param uri: fully qualified uri
        :return: True if location was archived
        """

    @deprecat(
        reason="Multiple locations per dataset are now deprecated. "
        "Archived locations may not be restorable in future releases. "
        "Dataset location can be set or updated with the update() method.",
        version="1.9.0",
        category=ODC2DeprecationWarning,
    )
    @abstractmethod
    def restore_location(self, id_: DSID, uri: str) -> bool:
        """
        Un-archive a location of the dataset if it exists.

        :param id_: dataset id
        :param uri: fully qualified uri
        :return: True if location was restored
        """

    @abstractmethod
    def search_by_metadata(
        self, metadata: JsonDict, archived: bool | None = False
    ) -> Iterable[Dataset]:
        """
        Perform a search using arbitrary metadata, returning results as Dataset objects.

        Caution - slow! This will usually not use indexes.

        :param metadata: metadata dictionary representing arbitrary search query
        :param archived: False (default): Return active datasets only.
                         None: Include archived and active datasets.
                         True: Return archived datasets only.
        :return: Matching dataset models
        """

    @deprecat(
        deprecated_args={
            "source_filter": {
                "reason": "Filtering by source metadata is deprecated and will be removed in future.",
                "version": "1.9.0",
                "category": ODC2DeprecationWarning,
            }
        }
    )
    @abstractmethod
    def search(
        self,
        limit: int | None = None,
        source_filter: QueryDict | None = None,
        archived: bool | None = False,
        order_by: Iterable[Any] | None = None,
        **query: QueryField,
    ) -> Iterable[Dataset]:
        """
        Perform a search, returning results as Dataset objects.

        Prior to dataccube-1.9.0, search always returned datasets sorted by product.  From 1.9, no ordering
        is guaranteed.  Ordering of results is now unspecified and may vary between index drivers.

        :param limit: Limit number of datasets per product (None/default = unlimited)
        :param source_filter: Filter criteria for sources (None/default = no filtering)
        :param archived: False (default): Return active datasets only.
                         None: Include archived and active datasets.
                         True: Return archived datasets only.
        :param order_by: field or expression by which to order results
        :param query: search query parameters
        :return: Matching datasets
        """

    def get_all_docs_for_product(
        self, product: Product, batch_size: int = 1000
    ) -> Iterable[DatasetTuple]:
        for ds in self.search(product=[product.name]):
            yield DatasetTuple(product, ds.metadata_doc, ds._uris)  # 2.0: ds.uri

    def get_all_docs(
        self, products: Iterable[Product] | None = None, batch_size: int = 1000
    ) -> Iterable[DatasetTuple]:
        """
        Return all datasets in bulk, filtering by product names only. Do not instantiate models.
        Archived datasets and locations are excluded.

        API Note: This API method is not finalised and may be subject to change.

        :param products: Iterable of products used to build the Dataset models.  May come from a different index.
                         Default/None: all products, Products read from the source index.
        :param batch_size: Size of each chunk in the returned iterable (default = 1000)
        :return: Iterable of DatasetTuple named tuples
        """
        # Default implementation calls search
        if products is None:
            products = list(self.products.get_all())
        for product in products:
            yield from self.get_all_docs_for_product(product, batch_size=batch_size)

    def _add_batch(
        self, batch_ds: Iterable[DatasetTuple], cache: dict[str, Any]
    ) -> BatchStatus:
        """
        Add a single "batch" of datasets, provided as DatasetTuples.

        Default implementation is simple loop of add

        API Note: This API method is not finalised and may be subject to change.

        :param batch_ds: An iterable of one batch's worth of DatasetTuples to add
        :return: BatchStatus named tuple.
        """
        b_skipped = 0
        b_added = 0
        b_started = monotonic()
        for ds_tup in batch_ds:
            if ds_tup.is_legacy:  # 2.0: {'uri': ds_tup.uri}
                kwargs = {"uris": ds_tup.uris}
            else:
                kwargs = {"uri": ds_tup.uri}
            try:
                ds = Dataset(
                    product=ds_tup.product, metadata_doc=ds_tup.metadata, **kwargs
                )
                self.add(ds, with_lineage=False)
                b_added += 1
            except DocumentMismatchError as e:
                _LOG.warning("%s: Skipping", str(e))
                b_skipped += 1
            except Exception as e:
                _LOG.warning("%s: Skipping", str(e))
                b_skipped += 1
        return BatchStatus(b_added, b_skipped, monotonic() - b_started)

    def _init_bulk_add_cache(self) -> dict[str, Any]:
        """
        Initialise a cache dictionary that may be used to share data between calls to _add_batch()

        API Note: This API method is not finalised and may be subject to change.

        :return: The initialised cache dictionary
        """
        return {}

    def bulk_add(
        self, datasets: Iterable[DatasetTuple], batch_size: int = 1000
    ) -> BatchStatus:
        """
        Add a group of Dataset documents in bulk.

        API Note: This API method is not finalised and may be subject to change.

        :param datasets: An Iterable of DatasetTuples (i.e. as returned by get_all_docs)
        :param batch_size: Number of datasets to add per batch (default 1000)
        :return: BatchStatus named tuple, with `safe` set to None.
        """

        def increment_progress() -> None:
            report_to_user(".", progress_indicator=True)

        n_batches = 0
        n_in_batch = 0
        added = 0
        skipped = 0
        batch = []
        job_started = monotonic()
        inter_batch_cache = self._init_bulk_add_cache()
        for ds_tup in datasets:
            batch.append(ds_tup)
            n_in_batch += 1
            if n_in_batch >= batch_size:
                batch_result = self._add_batch(batch, inter_batch_cache)
                _LOG.info(
                    "Batch %d/%d datasets added in %.2fs: (%.2fdatasets/min)",
                    batch_result.completed,
                    n_in_batch,
                    batch_result.seconds_elapsed,
                    batch_result.completed * 60 / batch_result.seconds_elapsed,
                )
                added += batch_result.completed
                skipped += batch_result.skipped
                batch = []
                n_in_batch = 0
                n_batches += 1
                increment_progress()
        if n_in_batch > 0:
            batch_result = self._add_batch(batch, inter_batch_cache)
            added += batch_result.completed
            skipped += batch_result.skipped
            increment_progress()

        return BatchStatus(added, skipped, monotonic() - job_started)

    @abstractmethod
    def search_by_product(
        self, archived: bool | None = False, **query: QueryField
    ) -> Iterable[tuple[Product, Iterable[Dataset]]]:
        """
        Perform a search, returning datasets grouped by product type.

        :param archived: False (default): Return active datasets only.
                         None: Include archived and active datasets.
                         True: Return archived datasets only.
        :param query: search query parameters
        :return: Matching datasets, grouped by Product
        """

    @abstractmethod
    def search_returning(
        self,
        field_names: Iterable[str] | None = None,
        custom_offsets: Mapping[str, Offset] | None = None,
        limit: int | None = None,
        archived: bool | None = False,
        order_by: Iterable[Any] | None = None,
        **query: QueryField,
    ) -> Iterable[tuple]:
        """
        Perform a search, returning only the specified fields.

        This method can be faster than normal search() if you don't need all fields of each dataset.

        It also allows for returning rows other than datasets, such as a row per uri when requesting field 'uri'.

        :param field_names: Names of desired fields (default = all known search fields, unless custom_offsets is set,
                            see below)
        :param custom_offsets: A dictionary of offsets in the metadata doc for custom fields custom offsets
                               are returned in addition to fields named in field_names.  Default is
                               None, field_names only.  If field_names is None, and custom_offsets are provided,
                               only the custom offsets are included, over-riding the normal field_names default.
        :param limit: Limit number of dataset (None/default = unlimited)
        :param archived: False (default): Return active datasets only.
                         None: Include archived and active datasets.
                         True: Return archived datasets only.
        :param order_by: a field name, field, function or clause by which to sort output. None is unsorted and may allow
                         faster return of first result depending on the index driver's implementation.
        :param query: search query parameters
        :return: Namedtuple of requested fields, for each matching dataset.
        """

    @abstractmethod
    def count(self, archived: bool | None = False, **query: QueryField) -> int:
        """
        Perform a search, returning count of results.

        :param archived: False (default): Count active datasets only.
                         None: Count archived and active datasets.
                         True: Count archived datasets only.
        :param query: search query parameters
        :return: Count of matching datasets in index
        """

    @abstractmethod
    def count_by_product(
        self, archived: bool | None = False, **query: QueryField
    ) -> Iterable[tuple[Product, int]]:
        """
        Perform a search, returning a count of for each matching product type.

        :param archived: False (default): Count active datasets only.
                         None: Count archived and active datasets.
                         True: Count archived datasets only.
        :param query: search query parameters
        :return: Counts of matching datasets in index, grouped by product.
        """

    @abstractmethod
    def count_by_product_through_time(
        self, period: str, archived: bool | None = False, **query: QueryField
    ) -> Iterable[tuple[Product, Iterable[tuple[Range, int]]]]:
        """
        Perform a search, returning counts for each product grouped in time slices
        of the given period.

        :param period: Time range for each slice: '1 month', '1 day' etc.
        :param archived: False (default): Count active datasets only.
                         None: Count archived and active datasets.
                         True: Count archived datasets only.
        :param query: search query parameters
        :returns: For each matching product type, a list of time ranges and their count.
        """

    @abstractmethod
    def count_product_through_time(
        self, period: str, archived: bool | None = False, **query: QueryField
    ) -> Iterable[tuple[Range, int]]:
        """
        Perform a search, returning counts for a single product grouped in time slices
        of the given period.

        Will raise an error if the search terms match more than one product.

        :param period: Time range for each slice: '1 month', '1 day' etc.
        :param archived: False (default): Count active datasets only.
                         None: Count archived and active datasets.
                         True: Count archived datasets only.
        :param query: search query parameters
        :returns: The product, a list of time ranges and the count of matching datasets.
        """

    @deprecat(
        reason="This method is deprecated and will be removed in 2.0.  "
        "Consider migrating to search_returning()",
        version="1.9.0",
        category=ODC2DeprecationWarning,
    )
    @abstractmethod
    def search_summaries(self, **query: QueryField) -> Iterable[Mapping[str, Any]]:
        """
        Perform a search, returning just the search fields of each dataset.

        :param query: search query parameters
        :return: Mappings of search fields for matching datasets
        """

    @deprecat(
        reason="This method is deprecated and will be removed in 2.0.  "
        "Please use list(dc.index.datasets.search(...)) instead",
        version="1.9.0",
        category=ODC2DeprecationWarning,
    )
    def search_eager(self, **query: QueryField) -> Iterable[Dataset]:
        """
        Perform a search, returning results as Dataset objects.

        :param query: search query parameters
        :return: Fully instantiated list of matching dataset models
        """
        return list(self.search(**query))

    @abstractmethod
    def temporal_extent(
        self, ids: Iterable[DSID]
    ) -> tuple[datetime.datetime, datetime.datetime]:
        """
        Returns the minimum and maximum acquisition time of an iterable of dataset ids.

        Raises KeyError if none of the datasets are in the index

        :param ids: Iterable of dataset ids.
        :return: minimum and maximum acquisition times
        """

    @deprecat(
        reason="This method has been moved to the Product Resource and renamed 'temporal_extent()'",
        version="1.9.0",
        category=ODC2DeprecationWarning,
    )
    def get_product_time_bounds(
        self, product: str | Product
    ) -> tuple[datetime.datetime, datetime.datetime]:
        """
        Returns the minimum and maximum acquisition time of the product.

        :param product: Product of name of product
        :return: minimum and maximum acquisition times
        """
        return self._index.products.temporal_extent(product=product)

    @abstractmethod
    def search_returning_datasets_light(
        self,
        field_names: tuple[str, ...],
        custom_offsets: Mapping[str, Offset] | None = None,
        limit: int | None = None,
        archived: bool | None = False,
        **query: QueryField,
    ) -> Iterable[tuple]:
        """
        This is a dataset search function that returns the results as objects of a dynamically
        generated Dataset class that is a subclass of tuple.

        Only the requested fields will be returned together with related derived attributes as property functions
        similar to the datacube.model.Dataset class. For example, if 'extent'is requested all of
        'crs', 'extent', 'transform', and 'bounds' are available as property functions.

        The field_names can be custom fields in addition to those specified in metadata_type, fixed fields, or
        native fields. The field_names can also be derived fields like 'extent', 'crs', 'transform',
        and 'bounds'. The custom fields require custom offsets of the metadata doc be provided.

        The datasets can be selected based on values of custom fields as long as relevant custom
        offsets are provided. However custom field values are not transformed so must match what is
        stored in the database.

        :param field_names: A tuple of field names that would be returned including derived fields
                            such as extent, crs
        :param custom_offsets: A dictionary of offsets in the metadata doc for custom fields
        :param limit: Number of datasets returned per product.
        :param archived: False (default): Return active datasets only.
                         None: Return archived and active datasets.
                         True: Return archived datasets only.
        :param query: query parameters that will be processed against metadata_types,
                      product definitions and/or dataset table.
        :return: A Dynamically generated DatasetLight (a subclass of namedtuple and possibly with
            property functions).
        """

    @abstractmethod
    def spatial_extent(
        self, ids: Iterable[DSID], crs: CRS = CRS("EPSG:4326")
    ) -> Geometry | None:
        """
        Return the combined spatial extent of the nominated datasets

        Uses spatial index.

        Returns None if no index for the CRS, or if no identified datasets are indexed in the relevant spatial index.
        Result will not include extents of datasets that cannot be validly projected into the CRS.

        :param ids: An iterable of dataset IDs
        :param crs: A CRS (defaults to EPSG:4326)
        :return: The combined spatial extents of the datasets.
        """

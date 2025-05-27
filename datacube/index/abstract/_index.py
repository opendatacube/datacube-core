# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2025 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
import logging
from abc import ABC, abstractmethod
from collections.abc import Iterable, Mapping, Sequence
from urllib.parse import ParseResult, urlparse

from deprecat import deprecat
from odc.geo import CRS

from datacube.cfg import ODCEnvironment, ODCOptionHandler
from datacube.migration import ODC2DeprecationWarning
from datacube.model import Field, MetadataType
from datacube.utils import cached_property, report_to_user
from datacube.utils.generic import thread_local_cache

from ._datasets import AbstractDatasetResource
from ._lineage import AbstractLineageResource
from ._metadata_types import AbstractMetadataTypeResource
from ._products import AbstractProductResource
from ._transactions import AbstractTransaction
from ._types import DSID, BatchStatus
from ._users import AbstractUserResource

_LOG: logging.Logger = logging.getLogger(__name__)


class AbstractIndex(ABC):
    """
    Abstract base class for an Index.  All Index implementations should
    inherit from this base class, and implement all abstract methods (and
    override other methods and contract flags as required).
    """

    # Interface contracts - implementations should set to True where appropriate.

    # Metadata type support flags
    #   supports legacy ODCv1 EO style metadata types.
    supports_legacy = False
    #   supports eo3 compatible metadata types.
    supports_eo3 = False
    #   supports non-geospatial (e.g. telemetry) metadata types
    supports_nongeo = False
    #   supports geospatial vector (i.e. non-raster) metadata types (reserved for future use)
    supports_vector = False

    # Database/storage feature support flags
    #   supports add() update() remove() etc methods.
    supports_write = False
    #   supports persistent storage. Writes from previous instantiations will persist into future ones.
    #   (Requires supports_write)
    supports_persistance = False
    #    Supports ACID transactions (Requires supports_write)
    supports_transactions = False
    #    Supports per-CRS spatial indexes (Requires supports_write)
    supports_spatial_indexes = False

    # User management support flags
    #   support the index.users API
    supports_users = False

    # Lineage support flags
    #   supports lineage (either legacy or new API)
    supports_lineage = False
    #   supports external lineage API (as described in EP-08).  Requires supports_lineage
    #   IF support_lineage is True and supports_external_lineage is False THEN legacy lineage API.
    supports_external_lineage = False
    #   supports an external lineage home field.  Requires supports_external_lineage
    supports_external_home = False

    @property
    @abstractmethod
    def name(self) -> str:
        """The index name"""

    @property
    @abstractmethod
    def environment(self) -> ODCEnvironment:
        """The cfg.ODCEnvironment object this Index was initialised from."""

    @property
    @abstractmethod
    def url(self) -> str:
        """A string representing the index"""

    @cached_property
    def url_parts(self) -> ParseResult:
        return urlparse(self.url)

    @property
    @abstractmethod
    def users(self) -> AbstractUserResource:
        """A User Resource instance for the index"""

    @property
    @abstractmethod
    def metadata_types(self) -> AbstractMetadataTypeResource:
        """A MetadataType Resource instance for the index"""

    @property
    @abstractmethod
    def products(self) -> AbstractProductResource:
        """A Product Resource instance for the index"""

    @property
    @abstractmethod
    def lineage(self) -> AbstractLineageResource:
        """A Lineage Resource instance for the index"""

    @property
    @abstractmethod
    def datasets(self) -> AbstractDatasetResource:
        """A Dataset Resource instance for the index"""

    @classmethod
    @abstractmethod
    def from_config(
        cls,
        cfg_env: ODCEnvironment,
        application_name: str | None = None,
        validate_connection: bool = True,
    ) -> "AbstractIndex":
        """Instantiate a new index from an ODCEnvironment configuration object"""

    @classmethod
    @abstractmethod
    def get_dataset_fields(cls, doc: dict) -> Mapping[str, Field]:
        """Return dataset search fields from a metadata type document"""

    @abstractmethod
    def init_db(
        self, with_default_types: bool = True, with_permissions: bool = True
    ) -> bool:
        """
        Initialise an empty database.

        :param with_default_types: Whether to create default metadata types
        :param with_permissions: Whether to create db permissions
        :return: true if the database was created, false if already exists
        """

    # Spatial Index API

    def create_spatial_index(self, crs: CRS) -> bool:
        """
        Create a spatial index for a CRS.

        Note that a newly created spatial index is empty.  If there are already datasets in the index whose
        extents can be safely projected into the CRS, then it is necessary to also call update_spatial_index
        otherwise they will not be found by queries against that CRS.

        Only implemented by index drivers with supports_spatial_indexes set to True.

        :param crs: The coordinate reference system to create a spatial index for.
        :return: True if the spatial index was successfully created (or already exists)
        """
        if not self.supports_spatial_indexes:
            raise NotImplementedError(
                "This index driver does not support the Spatial Index API"
            )
        else:
            raise NotImplementedError()

    def spatial_indexes(self, refresh: bool = False) -> Iterable[CRS]:
        """
        Return the CRSs for which spatial indexes have been created.

        :param refresh: If true, query the backend for the list of current spatial indexes.  If false (the default)
                        a cached list of spatial index CRSs may be returned.
        :return: An iterable of CRSs for which spatial indexes exist in the index
        """
        if not self.supports_spatial_indexes:
            raise NotImplementedError(
                "This index driver does not support the Spatial Index API"
            )
        else:
            raise NotImplementedError()

    def update_spatial_index(
        self,
        crses: Sequence[CRS] = [],
        product_names: Sequence[str] = [],
        dataset_ids: Sequence[DSID] = [],
    ) -> int:
        """
        Populate a newly created spatial index (or indexes).

        Spatial indexes are automatically populated with new datasets as they are indexed, but if there were
        datasets already in the index when a new spatial index is created, or if geometries have been added or
        modified outside of the ODC in a populated index (e.g. with SQL) then the spatial indices must be
        updated manually with this method.

        This is a very slow operation.  The product_names and dataset_ids lists can be used to break the
        operation up into chunks or allow faster updating when the spatial index is only relevant to a
        small portion of the entire index.

        :param crses: A list of CRSes whose spatial indexes are to be updated.
                      Default is to update all spatial indexes
        :param product_names: A list of product names to update the spatial indexes.
                              Default is to update for all products
        :param dataset_ids: A list of ids of specific datasets to update in the spatial index.
                            Default is to update for all datasets (or all datasets in the products
                            in the product_names list)
        :return: The number of dataset extents processed - i.e. the number of datasets updated multiplied by the
                 number of spatial indexes updated.
        """
        if not self.supports_spatial_indexes:
            raise NotImplementedError(
                "This index driver does not support the Spatial Index API"
            )
        else:
            raise NotImplementedError()

    def drop_spatial_index(self, crs: CRS) -> bool:
        """
        Remove a spatial index from the database.

        Note that creating spatial indexes on an existing index is a slow and expensive operation.  Do not
        delete spatial indexes unless you are absolutely certain it is no longer required by any users of
        this ODC index.

        :param crs: The CRS whose spatial index is to be deleted.
        :return: True if the spatial index was successfully dropped.
                 False if spatial index could not be dropped.
        """
        if not self.supports_spatial_indexes:
            raise NotImplementedError(
                "This index driver does not support the Spatial Index API"
            )
        else:
            raise NotImplementedError()

    def clone(
        self,
        origin_index: "AbstractIndex",
        batch_size: int = 1000,
        skip_lineage: bool = False,
        lineage_only: bool = False,
    ) -> Mapping[str, BatchStatus]:
        """
        Clone an existing index into this one.

        Steps are:

        1) Clone all metadata types compatible with this index driver.

           - Products and Datasets with incompatible metadata types are excluded from subsequent steps.
           - Existing metadata types are skipped, but products and datasets associated with them are only
             excluded if the existing metadata type does not match the one from the origin index.

        2) Clone all products with "safe" metadata types.

           - Products are included or excluded by metadata type as discussed above.
           - Existing products are skipped, but datasets associated with them are only
             excluded if the existing product definition does not match the one from the origin index.

        3) Clone all datasets with "safe" products

           - Datasets are included or excluded by product and metadata type, as discussed above.
           - Archived datasets and locations are not cloned.

        4) Clone all lineage relations that can be cloned.

           - All lineage relations are skipped if either index driver does not support lineage,
             or if skip_lineage is True.
           - If this index does not support external lineage then lineage relations that reference datasets
             that do not exist in this index after step 3 above are skipped.

        API Note: This API method is not finalised and may be subject to change.

        :param origin_index: Index whose contents we wish to clone.
        :param batch_size: Maximum number of objects to write to the database in one go.
        :param skip_lineage: Skip lineage in cloned result.
        :param lineage_only: Only clone lineage.
        :return: Dictionary containing a BatchStatus named tuple for "metadata_types", "products"
                 and "datasets", and optionally "lineage".
        """
        results = {}
        if not lineage_only:
            if self.supports_spatial_indexes and origin_index.supports_spatial_indexes:
                for crs in origin_index.spatial_indexes(refresh=True):
                    report_to_user(f"Creating spatial index for CRS {crs}")
                    self.create_spatial_index(crs)
                self.update_spatial_index(
                    list(origin_index.spatial_indexes(refresh=False))
                )
            # Clone Metadata Types
            report_to_user("Cloning Metadata Types:")
            results["metadata_types"] = self.metadata_types.bulk_add(
                origin_index.metadata_types.get_all_docs(), batch_size=batch_size
            )
            res = results["metadata_types"]
            msg = (
                f"{res.completed} metadata types loaded ({res.skipped} skipped) in "
                f"{res.seconds_elapsed:.2f}seconds "
                f"({res.completed * 60 / res.seconds_elapsed:.2f} metadata_types/min)"
            )
            report_to_user(msg, logger=_LOG)
            if res.safe:
                metadata_cache = {
                    name: self.metadata_types.get_by_name_unsafe(name)
                    for name in res.safe
                }
            else:
                metadata_cache = {}
            # Clone Products
            report_to_user("Cloning Products:")
            results["products"] = self.products.bulk_add(
                origin_index.products.get_all_docs(),
                metadata_types=metadata_cache,
                batch_size=batch_size,
            )
            res = results["products"]
            msg = (
                f"{res.completed} products loaded ({res.skipped} skipped) in {res.seconds_elapsed:.2f}seconds "
                f"({res.completed * 60 / res.seconds_elapsed:.2f} products/min)"
            )
            report_to_user(msg, logger=_LOG)
            # Clone Datasets (group by product for now for convenience)
            report_to_user("Cloning Datasets:")
            if res.safe:
                products = [p for p in self.products.get_all() if p.name in res.safe]
            else:
                products = []
            results["datasets"] = self.datasets.bulk_add(
                origin_index.datasets.get_all_docs(
                    products=products, batch_size=batch_size
                ),
                batch_size=batch_size,
            )
            res = results["datasets"]
            report_to_user("")
            msg = (
                f"{res.completed} datasets loaded ({res.skipped} skipped) in {res.seconds_elapsed:.2f}seconds "
                f"({res.completed * 60 / res.seconds_elapsed:.2f} datasets/min)"
            )
            report_to_user(msg, logger=_LOG)
        if (
            not self.supports_lineage
            or not origin_index.supports_lineage
            or skip_lineage
        ):
            report_to_user("Skipping lineage")
            return results
        report_to_user("Cloning Lineage:")
        results["lineage"] = self.lineage.bulk_add(
            origin_index.lineage.get_all_lineage(batch_size), batch_size
        )
        res = results["lineage"]
        report_to_user("")
        msg = (
            f"{res.completed} lineage relations loaded ({res.skipped} skipped) in {res.seconds_elapsed:.2f}seconds "
            f"({res.completed * 60 / res.seconds_elapsed:.2f} lineage relations/min)"
        )
        report_to_user(msg, logger=_LOG)
        return results

    @abstractmethod
    def close(self) -> None:
        """
        Close and cleanup the Index.
        """

    @property
    @abstractmethod
    def index_id(self) -> str:
        """
        :return: Unique ID for this index
                 (e.g. same database/dataset storage + same index driver implementation = same id)
        """

    @abstractmethod
    def transaction(self) -> AbstractTransaction:
        """
        :return: a Transaction context manager for this index.
        """

    def thread_transaction(self) -> AbstractTransaction | None:
        """
        :return: The existing Transaction object cached in thread-local storage for this index, if there is one.
        """
        return thread_local_cache(f"txn-{self.index_id}", None)

    def __enter__(self) -> "AbstractIndex":
        return self

    def __exit__(self) -> None:
        self.close()


class AbstractIndexDriver(ABC):
    """
    Abstract base class for an IndexDriver.  All IndexDrivers should inherit from this base class
    and implement all abstract methods.
    """

    @classmethod
    @abstractmethod
    def index_class(cls) -> type[AbstractIndex]: ...

    @classmethod
    def connect_to_index(
        cls,
        config_env: ODCEnvironment,
        application_name: str | None = None,
        validate_connection: bool = True,
    ) -> "AbstractIndex":
        return cls.index_class().from_config(
            config_env, application_name, validate_connection
        )

    @staticmethod
    @abstractmethod
    @deprecat(
        reason="The 'metadata_type_from_doc' static method has been deprecated. "
        "Please use the 'index.metadata_type.from_doc()' instead.",
        version="1.9.0",
        category=ODC2DeprecationWarning,
    )
    def metadata_type_from_doc(definition: dict) -> MetadataType: ...

    @staticmethod
    def get_config_option_handlers(env: ODCEnvironment) -> Iterable[ODCOptionHandler]:
        """
        Default Implementation does nothing.
        Override for driver-specific config handling (e.g. for db connection)
        """
        return []

# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2025 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
import logging
from collections.abc import Mapping
from typing import Any

from deprecat import deprecat
from odc.geo import CRS
from typing_extensions import override

from datacube.cfg import ODCEnvironment
from datacube.index.abstract import (
    AbstractIndex,
    AbstractIndexDriver,
    NoLineageResource,
    UnhandledTransaction,
)
from datacube.index.null._datasets import DatasetResource
from datacube.index.null._metadata_types import MetadataTypeResource
from datacube.index.null._products import ProductResource
from datacube.index.null._users import UserResource
from datacube.migration import ODC2DeprecationWarning
from datacube.model import Field, MetadataType
from datacube.model.fields import get_dataset_fields

_LOG: logging.Logger = logging.getLogger(__name__)


class Index(AbstractIndex):
    """
    (Sub-)Minimal (non-)implementation of the Index API.
    """

    #   Metadata type support flags
    supports_legacy = True
    supports_eo3 = True
    supports_nongeo = True

    #   User management support flags
    supports_users = True

    def __init__(self, env: ODCEnvironment) -> None:
        self._env = env
        self._users = UserResource()
        self._metadata_types = MetadataTypeResource()
        self._products = ProductResource(self)
        self._lineage = NoLineageResource(self)
        self._datasets = DatasetResource(self)

    @override
    @property
    def name(self) -> str:
        return "null_index"

    @override
    @property
    def environment(self) -> ODCEnvironment:
        return self._env

    @override
    @property
    def users(self) -> UserResource:
        return self._users

    @override
    @property
    def metadata_types(self) -> MetadataTypeResource:
        return self._metadata_types

    @override
    @property
    def products(self) -> ProductResource:
        return self._products

    @override
    @property
    def lineage(self) -> NoLineageResource:
        return self._lineage

    @override
    @property
    def datasets(self) -> DatasetResource:
        return self._datasets

    @override
    @property
    def url(self) -> str:
        return "null"

    @override
    @property
    def index_id(self) -> str:
        return "null"

    @override
    def transaction(self) -> UnhandledTransaction:
        return UnhandledTransaction(self.index_id)

    @classmethod
    @override
    def from_config(
        cls,
        config_env,
        application_name: str | None = None,
        validate_connection: bool = True,
    ) -> "Index":
        return cls(config_env)

    @classmethod
    @override
    def get_dataset_fields(cls, doc: Mapping[str, Any]) -> dict[str, Field]:
        return get_dataset_fields(doc)

    @override
    def init_db(
        self, with_default_types: bool = True, with_permissions: bool = True
    ) -> bool:
        return True

    @override
    def close(self) -> None:
        pass

    @override
    def create_spatial_index(self, crs: CRS) -> bool:
        _LOG.warning("null driver does not support spatio-temporal indexes")
        return False

    @override
    def __repr__(self) -> str:
        return "Index<null>"


class NullIndexDriver(AbstractIndexDriver):
    @classmethod
    @override
    def index_class(cls) -> type[AbstractIndex]:
        return Index

    @staticmethod
    @override
    @deprecat(
        reason="The 'metadata_type_from_doc' static method has been deprecated. "
        "Please use the 'index.metadata_type.from_doc()' instead.",
        version="1.9.0",
        category=ODC2DeprecationWarning,
    )
    def metadata_type_from_doc(definition: dict) -> MetadataType:
        """
        :param definition:
        """
        MetadataType.validate(definition)  # type: ignore
        return MetadataType(
            definition, dataset_search_fields=Index.get_dataset_fields(definition)
        )


def index_driver_init() -> NullIndexDriver:
    return NullIndexDriver()

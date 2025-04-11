# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2025 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
import logging
from threading import Lock
from typing_extensions import override

from deprecat import deprecat
from datacube.cfg import ODCEnvironment
from datacube.index.memory._datasets import DatasetResource, LineageResource
from datacube.index.memory._fields import get_dataset_fields
from datacube.index.memory._metadata_types import MetadataTypeResource
from datacube.index.memory._products import ProductResource
from datacube.index.memory._users import UserResource
from datacube.index.abstract import AbstractIndex, AbstractIndexDriver, UnhandledTransaction
from datacube.model import MetadataType
from datacube.migration import ODC2DeprecationWarning
from odc.geo import CRS

_LOG = logging.getLogger(__name__)


counter = 0
counter_lock = Lock()


class Index(AbstractIndex):
    """
    Lightweight in-memory index driver
    """
    #   Metadata type support flags
    supports_legacy = True
    supports_eo3 = True
    supports_nongeo = True

    #   Database/storage feature support flags
    supports_write = True

    #   User management support flags
    supports_users = True

    #   Lineage support flags
    supports_lineage = True

    def __init__(self, env: ODCEnvironment) -> None:
        self._env = env
        self._users = UserResource()
        self._metadata_types = MetadataTypeResource()
        self._products = ProductResource(self)
        self._lineage = LineageResource(self)
        self._datasets = DatasetResource(self)
        global counter
        with counter_lock:
            counter += 1
            self._index_id = f"memory={counter}"

    @override
    @property
    def name(self) -> str:
        return "memory_index"

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
    def lineage(self) -> LineageResource:
        return self._lineage

    @override
    @property
    def datasets(self) -> DatasetResource:
        return self._datasets

    @override
    @property
    def url(self) -> str:
        return "memory"

    @override
    @property
    def index_id(self) -> str:
        return self._index_id

    @override
    def transaction(self) -> UnhandledTransaction:
        return UnhandledTransaction(self.index_id)

    @classmethod
    @override
    def from_config(cls,
                    config_env: ODCEnvironment,
                    application_name: str | None = None,
                    validate_connection: bool = True):
        return cls(config_env)

    @classmethod
    def get_dataset_fields(cls, doc):
        return get_dataset_fields(doc)

    @override
    def init_db(self, with_default_types=True, with_permissions=True):
        return True

    @override
    def close(self):
        pass

    @override
    def create_spatial_index(self, crs: CRS) -> bool:
        _LOG.warning("memory index driver does not support spatio-temporal indexes")
        return False

    @override
    def __repr__(self):
        return "Index<memory>"


class MemoryIndexDriver(AbstractIndexDriver):
    @classmethod
    @override
    def index_class(cls) -> type[AbstractIndex]:
        return Index

    @staticmethod
    @override
    @deprecat(
        reason="The 'metadata_type_from_doc' static method has been deprecated. "
               "Please use the 'index.metadata_type.from_doc()' instead.",
        version='1.9.0',
        category=ODC2DeprecationWarning)
    def metadata_type_from_doc(definition: dict) -> MetadataType:
        """
        :param definition:
        """
        MetadataType.validate(definition)  # type: ignore
        return MetadataType(definition, dataset_search_fields=Index.get_dataset_fields(definition))


def index_driver_init():
    return MemoryIndexDriver()

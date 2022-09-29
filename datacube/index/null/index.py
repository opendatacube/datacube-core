# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2022 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
import logging

from datacube.index.null._datasets import DatasetResource  # type: ignore
from datacube.index.null._metadata_types import MetadataTypeResource
from datacube.index.null._products import ProductResource
from datacube.index.null._users import UserResource
from datacube.index.abstract import AbstractIndex, AbstractIndexDriver, UnhandledTransaction
from datacube.model import MetadataType
from datacube.model.fields import get_dataset_fields
from datacube.utils.geometry import CRS

_LOG = logging.getLogger(__name__)


class Index(AbstractIndex):
    """
    (Sub-)Minimal (non-)implementation of the Index API.
    """
    # Supports everything but persistance
    supports_persistance = False

    def __init__(self) -> None:
        self._users = UserResource()
        self._metadata_types = MetadataTypeResource()
        self._products = ProductResource(self.metadata_types)
        self._datasets = DatasetResource(self.products)

    @property
    def users(self) -> UserResource:
        return self._users

    @property
    def metadata_types(self) -> MetadataTypeResource:
        return self._metadata_types

    @property
    def products(self) -> ProductResource:
        return self._products

    @property
    def datasets(self) -> DatasetResource:
        return self._datasets

    @property
    def url(self) -> str:
        return "null"

    @property
    def index_id(self) -> str:
        return "null"

    def transaction(self) -> UnhandledTransaction:
        return UnhandledTransaction(self.index_id)

    @classmethod
    def from_config(cls, config, application_name=None, validate_connection=True):
        return cls()

    @classmethod
    def get_dataset_fields(cls, doc):
        return get_dataset_fields(doc)

    def init_db(self, with_default_types=True, with_permissions=True):
        return True

    def close(self):
        pass

    def create_spatial_index(self, crs: CRS) -> bool:
        _LOG.warning("null driver does not support spatio-temporal indexes")
        return False

    def __repr__(self):
        return "Index<null>"


class NullIndexDriver(AbstractIndexDriver):
    @staticmethod
    def connect_to_index(config, application_name=None, validate_connection=True):
        return Index.from_config(config, application_name, validate_connection)

    @staticmethod
    def metadata_type_from_doc(definition: dict) -> MetadataType:
        """
        :param definition:
        """
        MetadataType.validate(definition)  # type: ignore
        return MetadataType(definition,
                            dataset_search_fields=Index.get_dataset_fields(definition))


def index_driver_init():
    return NullIndexDriver()

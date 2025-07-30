# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2025 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
import datetime
import logging
from collections.abc import Iterable, Sequence

from odc.geo import CRS, Geometry
from typing_extensions import override

from datacube.index.abstract import AbstractProductResource
from datacube.model import Product, QueryDict, QueryField
from datacube.utils.changes import Change
from datacube.utils.json_types import JsonDict

_LOG: logging.Logger = logging.getLogger(__name__)


class ProductResource(AbstractProductResource):
    @override
    def add(self, product: Product, allow_table_lock: bool = False) -> Product | None:
        raise NotImplementedError()

    @override
    def can_update(
        self,
        product: Product,
        allow_unsafe_updates: bool = False,
        allow_table_lock: bool = False,
    ) -> tuple[bool, Iterable[Change], Iterable[Change]]:
        raise NotImplementedError()

    @override
    def update(
        self,
        product: Product,
        allow_unsafe_updates: bool = False,
        allow_table_lock: bool = False,
    ) -> Product | None:
        raise NotImplementedError()

    @override
    def delete(
        self, products: Iterable[Product], allow_delete_active: bool = False
    ) -> Sequence[Product]:
        raise NotImplementedError()

    @override
    def get_unsafe(self, id_: int) -> Product:
        raise KeyError(id_)

    @override
    def get_by_name_unsafe(self, name: str) -> Product:
        raise KeyError(name)

    @override
    def search_robust(self, **query: QueryField) -> Iterable[tuple[Product, QueryDict]]:
        return []

    @override
    def search_by_metadata(self, metadata: JsonDict) -> Iterable[Product]:
        return []

    @override
    def get_all(self) -> Iterable[Product]:
        return []

    @override
    def temporal_extent(
        self, product: str | Product
    ) -> tuple[datetime.datetime, datetime.datetime]:
        raise KeyError(str(product))

    @override
    def spatial_extent(
        self, product: str | Product, crs: CRS = CRS("EPSG:4326")
    ) -> Geometry | None:
        raise KeyError(str(product))

    @override
    def most_recent_change(self, product: str | Product) -> datetime.datetime | None:
        raise KeyError(str(product))

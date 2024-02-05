# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2024 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
import logging
import datetime

from datacube.index.abstract import AbstractProductResource
from datacube.model import Product

from typing import Iterable

_LOG = logging.getLogger(__name__)


class ProductResource(AbstractProductResource):
    def add(self, product, allow_table_lock=False):
        raise NotImplementedError()

    def can_update(self, product, allow_unsafe_updates=False, allow_table_lock=False):
        raise NotImplementedError()

    def update(self, product: Product, allow_unsafe_updates=False, allow_table_lock=False):
        raise NotImplementedError()

    def get_unsafe(self, id_):
        raise KeyError(id_)

    def get_by_name_unsafe(self, name):
        raise KeyError(name)

    def search_robust(self, **query):
        return []

    def search_by_metadata(self, metadata):
        return []

    def get_all(self) -> Iterable[Product]:
        return []

    def temporal_extent(self, product: str | Product) -> tuple[datetime.datetime, datetime.datetime]:
        raise KeyError(str(product))

    def spatial_extent(self, product, crs=None):
        raise KeyError(str(product))

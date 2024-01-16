# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2024 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
import logging

from datacube.index.abstract import AbstractProductResource
from datacube.model import Product

from typing import Iterable

_LOG = logging.getLogger(__name__)


class ProductResource(AbstractProductResource):
    def __init__(self, mdtr):
        self.metadata_type_resource = mdtr

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
